from argparse import ArgumentParser
from asyncio import Queue
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from os import listdir
from os import makedirs
from os import rename
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import NamedTuple
import asyncio
import logging
import re

from aiohttp import ClientError
from aiohttp import ClientSession


PYPI_SIMPLE = "https://pypi.python.org/simple"
PYPI_JSON_TEMPALTE = "https://pypi.python.org/pypi/{package_name}/json"


logger = logging.getLogger("pypi downloader")
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s %(asctime)-10s: %(message)s',
)


class Paths(NamedTuple):
    base: Path
    known_file_names: set[str] = set()

    @property
    def tarball(self) -> Path:
        return self.base / "tarball"

    @property
    def wheel(self) -> Path:
        return self.base / "wheel"

    @property
    def egg(self) -> Path:
        return self.base / "egg"

    @property
    def other(self) -> Path:
        return self.base / "other"

    def iter_paths(self) -> list[Path]:
        return [self.tarball, self.wheel, self.egg, self.other]

    def fill_known_file_names(self) -> None:
        for path in self.iter_paths():
            self.known_file_names.update(set(listdir(path)))

    def add_file_name(self, path: str) -> None:
        self.known_file_names.add(path)

    @classmethod
    def create(cls, working_dir: str) -> "Paths":
        paths = Paths(Path(working_dir))
        for path in paths.iter_paths():
            makedirs(path, exist_ok=True)
        paths.fill_known_file_names()
        return paths


def get_file_name_from_uri(uri: str) -> str:
    return uri.split("/")[-1]


def get_file_path_from_file_name(file_name: str, paths: Paths) -> Path:
    if file_name.endswith(".tar.gz"):
        return paths.tarball / file_name
    if file_name.endswith(".egg"):
        return paths.egg / file_name
    if file_name.endswith(".whl"):
        return paths.wheel / file_name
    return paths.wheel / file_name


async def names_fetcher(
        session: ClientSession,
        names_queue: Queue,
) -> None:
    logger.info("Started")
    async with session.get(PYPI_SIMPLE) as resp:
        if resp.status != 200:
            logger.error("%r status: %d", PYPI_SIMPLE, resp.status)
            return
        content = await resp.text()
    for match in re.finditer(r'\s+<a href="([^"]+)">([^<]+)</a>', content):
        _, name = match.groups()
        await names_queue.put(name)
    logger.info("got all packages names")


async def uris_fetcher(
        session: ClientSession,
        names_queue: Queue,
        uris_to_check_queue: Queue,
        timeout: int = 60,
) -> None:
    while True:
        name = await names_queue.get()
        logger.debug("package: %s taken", name)
        package_uri = PYPI_JSON_TEMPALTE.format(package_name=name)
        async with session.get(package_uri) as resp:
            if resp.status != 200:
                logger.error(
                    "package: %r status: %d",
                    package_uri,
                    resp.status,
                )
                continue
            try:
                package_data = await resp.json()
            except asyncio.TimeoutError as err:
                logger.error(
                    "package: %r %s. Sleeping for %d seconds",
                    package_uri,
                    err,
                    timeout,
                )
                await names_queue.put(name)
                await asyncio.sleep(timeout)
                logger.info("package: woke up")
                continue
        with suppress(KeyError, IndexError):
            await uris_to_check_queue.put(package_data['urls'][0]['url'])
        for releases in package_data['releases'].values():
            for release in releases:
                await uris_to_check_queue.put(release['url'])
        logger.debug("package: %s done", name)


async def uri_checker(
        number: int,
        paths: Paths,
        uris_to_check_queue: Queue,
        uris_to_download_queue: Queue,
) -> None:
    while True:
        uri = await uris_to_check_queue.get()
        logger.debug("checker-%d: %r taken", number, uri)
        file_name = get_file_name_from_uri(uri)
        if file_name in paths.known_file_names:
            logger.debug("checker-%d: %r already done", number, uri)
            continue
        file_path = str(get_file_path_from_file_name(file_name, paths))
        await uris_to_download_queue.put((uri, file_path))
        logger.debug("checker-%d: %r done", number, uri)


async def uri_downloader(
        number: int,
        session: ClientSession,
        uris_to_download_queue: Queue,
        files_to_write_queue: Queue,
        timeout: int = 60,
) -> None:
    while True:
        uri, file_path = await uris_to_download_queue.get()
        logger.debug("downloader-%d: %r taken", number, uri)
        async with session.get(uri) as resp:
            if resp.status != 200:
                logger.error(
                    "downloader-%d: %r status: %d",
                    number,
                    uri,
                    resp.status,
                )
                await uris_to_download_queue.put((uri, file_path))
                await asyncio.sleep(timeout)
                continue
            try:
                content_raw = await resp.read()
            except (asyncio.TimeoutError, ClientError) as err:
                logger.error(
                    "downloader-%d: %r %r. Sleeping for %d seconds.",
                    number,
                    uri,
                    err,
                    timeout,
                )
                await uris_to_download_queue.put((uri, file_path))
                await asyncio.sleep(timeout)
                logger.info("downloader-%d: woke up", number)
                continue
        await files_to_write_queue.put((file_path, content_raw))
        logger.debug("downloader-%d: %r done", number, uri)


async def file_writer(
        number: int,
        files_to_write_queue: Queue,
        paths: Paths,
        timeout: int = 15,
) -> None:
    while True:
        file_path, content_raw = await files_to_write_queue.get()
        logger.debug("writer-%d: %r taken", number, file_path)
        try:
            with NamedTemporaryFile(delete=False) as fd:
                fd.write(content_raw)
                fd.flush()
            rename(fd.name, file_path)
        except OSError as err:
            logger.exception("writer-%d: %r", number, err)
            await files_to_write_queue.put((file_path, content_raw))
            await asyncio.sleep(timeout)
            continue
        paths.add_file_name(file_path)
        logger.debug("writer-%d: %r done", number, file_path)


async def queue_watcher(
        names_queue: Queue,
        uris_to_check_queue: Queue,
        uris_to_download_queue: Queue,
        files_to_write_queue: Queue,
        checkers_count: int,
        downloaders_count: int,
        writers_count: int,
        timeout: int = 5
) -> None:
    workers_message = (
        f"c:{checkers_count} d:{downloaders_count} w:{writers_count}"
    )
    while True:
        logger.info(
            "names %d > check %d > download %d > write %d (%s)",
            names_queue.qsize(),
            uris_to_check_queue.qsize(),
            uris_to_download_queue.qsize(),
            files_to_write_queue.qsize(),
            workers_message,
        )
        await asyncio.sleep(timeout)


async def main(
        paths: Paths,
        download_streams_count: int,
        show_queue: bool,
) -> None:
    checkers_count = 100
    downloaders_count = download_streams_count
    writers_count = 1  # check "ulimit -n" for max file descriptors
    logger.info(
        "Found %d files in %s",
        len(paths.known_file_names),
        str(paths.base),
    )
    names_queue: Queue = Queue(maxsize=checkers_count)
    uris_to_check_queue: Queue = Queue(maxsize=checkers_count)
    uris_to_download_queue: Queue = Queue(maxsize=downloaders_count)
    files_to_write_queue: Queue = Queue(maxsize=writers_count)
    async with ClientSession() as session:
        tasks = []
        tasks.append(names_fetcher(
            session=session,
            names_queue=names_queue,
        ))
        tasks.append(uris_fetcher(
            session=session,
            names_queue=names_queue,
            uris_to_check_queue=uris_to_check_queue,
        ))
        for i in range(checkers_count):
            tasks.append(uri_checker(
                number=i,
                paths=paths,
                uris_to_check_queue=uris_to_check_queue,
                uris_to_download_queue=uris_to_download_queue,
            ))
        for i in range(downloaders_count):
            tasks.append(uri_downloader(
                number=i,
                session=session,
                uris_to_download_queue=uris_to_download_queue,
                files_to_write_queue=files_to_write_queue,
            ))
        for i in range(writers_count):
            tasks.append(file_writer(
                number=i,
                files_to_write_queue=files_to_write_queue,
                paths=paths,
            ))
        if show_queue:
            tasks.append(
                queue_watcher(
                    names_queue=names_queue,
                    uris_to_check_queue=uris_to_check_queue,
                    uris_to_download_queue=uris_to_download_queue,
                    files_to_write_queue=files_to_write_queue,
                    checkers_count=checkers_count,
                    downloaders_count=downloaders_count,
                    writers_count=writers_count,
                ),
            )
        await asyncio.gather(*tasks)


def exception_handler(_loop: asyncio.AbstractEventLoop, context: dict) -> None:
    logger.exception(context['message'])


def prepare() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "path",
        nargs="?",
        default="PYPI",
        metavar="PATH",
        help="Path to cache directory",
    )
    parser.add_argument(
        "--streams",
        type=int,
        default=4,
        metavar="N",
        help="Parallel streams count",
    )
    parser.add_argument(
        "--show-queue",
        action="store_true",
        help="Print queue size info",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set logging level to debug",
    )
    args = parser.parse_args()
    paths = Paths.create(args.path)
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    with ThreadPoolExecutor() as executor:
        loop.set_default_executor(executor)
        try:
            logger.info("Starting...")
            asyncio.run(main(paths, args.streams, args.show_queue))
            logger.info("All done!")
        except KeyboardInterrupt:
            logger.info("Exiting...")


if __name__ == '__main__':
    prepare()
