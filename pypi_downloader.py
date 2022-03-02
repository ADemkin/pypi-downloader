from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os import makedirs
from pathlib import Path
from typing import NamedTuple
import asyncio
import logging
import re

from aiohttp import ClientSession
import aiofiles

PYPI_SIMPLE = "https://pypi.python.org/simple"
PYPI_JSON_TEMPALTE = "https://pypi.python.org/pypi/{package_name}/json"

logger = logging.getLogger("pypi downloader")


class Paths(NamedTuple):
    base: Path
    known_paths: set[str] = set()

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

    def fill_known_paths(self) -> None:
        for path in self.iter_paths():
            self.known_paths.update(set(listdir(path)))

    def add_path(self, path: str) -> None:
        self.known_paths.add(path)

    @classmethod
    def create(cls, working_dir: str) -> "Paths":
        paths = Paths(Path(working_dir))
        for path in paths.iter_paths():
            makedirs(path, exist_ok=True)
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


async def get_package_names(
        session: ClientSession,
        names: asyncio.Queue,
) -> None:
    logger.info("Started")
    async with session.get(PYPI_SIMPLE) as resp:
        if resp.status != 200:
            logger.error("%r status: %d", PYPI_SIMPLE, resp.status)
            return
        content = await resp.text()
    for match in re.finditer(r'\s+<a href="([^"]+)">([^<]+)</a>', content):
        _, name = match.groups()
        await names.put(name)
    logger.info("got all packages names")


async def get_package_uris(
        session: ClientSession,
        names: asyncio.Queue,
        uris: asyncio.Queue,
) -> None:
    while True:
        name = await names.get()
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
            package_data = await resp.json()
        try:
            await uris.put(package_data['urls'][0]['url'])
        except (KeyError, IndexError):
            pass
        for releases in package_data['releases'].values():
            for release in releases:
                await uris.put(release['url'])
        logger.debug("package: %s done", name)


async def download_uri(
        number: int,
        session: ClientSession,
        paths: Paths,
        uris: asyncio.Queue,
        fs_semaphore: asyncio.Semaphore,
) -> None:
    while True:
        uri = await uris.get()
        logger.debug("uri-%d: %r taken", number, uri)
        file_name = get_file_name_from_uri(uri)
        file_path = str(get_file_path_from_file_name(file_name, paths))
        if file_path in paths.known_paths:
            logger.debug("uri-%d: %r already done", number, uri)
            continue
        async with session.get(uri) as resp:
            if resp.status != 200:
                logger.error("uri-%d: %r status: %d", number, uri, resp.status)
                continue
            content_raw = await resp.read()
        async with fs_semaphore:
            async with aiofiles.open(file_path, "wb") as fd:
                await fd.write(content_raw)
        paths.add_path(file_path)
        logger.debug("uri-%d: %r done", number, uri)


async def queue_watcher(
        names: asyncio.Queue,
        uris: asyncio.Queue,
        download_streams_count: int,
) -> None:
    while True:
        logger.info(
            "Queue names: %d uris: %d streams: %d",
            names.qsize(),
            uris.qsize(),
            download_streams_count,
        )
        await asyncio.sleep(5)


async def main(
        paths: Paths,
        download_streams_count: int,
        show_queue: bool,
) -> None:
    names_queue: asyncio.Queue = asyncio.Queue(maxsize=30)
    uris_queue: asyncio.Queue = asyncio.Queue()
    paths.fill_known_paths()
    fs_semaphore = asyncio.Semaphore(value=64)  # ulimit -n // 4
    logger.info(
        "found %d files in %s",
        len(paths.known_paths),
        str(paths.base),
    )
    async with ClientSession() as session:
        tasks = []
        tasks.append(get_package_uris(session, names_queue, uris_queue))
        for i in range(download_streams_count):
            tasks.append(download_uri(
                i,
                session,
                paths,
                uris_queue,
                fs_semaphore,
            ))
        if show_queue:
            tasks.append(
                queue_watcher(names_queue, uris_queue, download_streams_count),
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
        default=20,
        metavar="N",
        help="Parallel streams count",
    )
    parser.add_argument(
        "--show-queue",
        action="store_true",
        help="Print queue size info",
    )
    parser.add_argument(
        "--loglevel",
        type=str,
        default="info",
        metavar="INFO",
        help="Logger level info",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.getLevelName(args.loglevel.upper()),
        format='%(levelname)s %(asctime)-10s: %(message)s',
    )
    paths = Paths.create(args.path)
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
