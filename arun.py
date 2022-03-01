from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from os import makedirs
from pathlib import Path
import asyncio
import logging
import re

from aiohttp import ClientSession
from aiohttp import ClientResponse
import aiofiles

PYPI_SIMPLE = "https://pypi.python.org/simple"
PYPI_JSON_TEMPALTE = "https://pypi.python.org/pypi/{package_name}/json"

logger = logging.getLogger("pypi downloader")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s %(asctime)-10s: %(message)s',
)


@dataclass
class Paths:
    base: Path
    tarball: Path
    wheel: Path
    egg: Path
    other: Path


def prepare_paths(working_dir: Path) -> Paths:
    base_path = Path(working_dir)
    tarball_path = base_path / "tarball"
    wheel_path = base_path / "wheel"
    egg_path = base_path / "egg"
    other_path = base_path / "other"
    for path in (tarball_path, wheel_path, egg_path, other_path):
        makedirs(path, exist_ok=True)
    return Paths(base_path, tarball_path, wheel_path, egg_path, other_path)


def get_file_name_from_uri(uri: str) -> str:
    return uri.split("/")[-1]


def get_file_path_from_file_name(file_name: str, paths: Paths) -> Path:
    if file_name.endswith(".tar.gz"):
        return paths.tarball / file_name
    elif file_name.endswith(".egg"):
        return paths.egg / file_name
    elif file_name.endswith(".whl"):
        return paths.wheel / file_name
    else:
        return paths.wheel / file_name


def resp_not_ok(resp: ClientResponse, uri: str) -> bool:
    if resp.status == 404:
        logger.error(f"{uri!r} not found")
        return True
    if resp.status != 200:
        logger.error(f"{uri!r} not OK: {resp!r}")
        return True
    return False


async def get_package_names(session: ClientSession, paths: Paths) -> None:
    async with session.get(PYPI_SIMPLE) as resp:
        if resp_not_ok(resp, PYPI_SIMPLE):
            return
        content = await resp.text()
    for match in re.finditer(r'\s+<a href="([^"]+)">([^<]+)</a>', content):
        _, name = match.groups()
        await get_package_uris(session, paths, name)


async def get_package_uris(
        session: ClientSession,
        paths: Paths,
        name: str,
) -> None:
    logger.info(f"{name!r} package taken")
    package_uri = PYPI_JSON_TEMPALTE.format(package_name=name)
    async with session.get(package_uri) as resp:
        if resp_not_ok(resp, package_uri):
            return
        package_data = await resp.json()
    try:
        actual_uri = package_data['urls'][0]['url']
        await download_uri(session, paths, actual_uri)
    except (KeyError, IndexError):
        pass
    for version, releases in package_data['releases'].items():
        for release in releases:
            uri = release['url']
            await download_uri(session, paths, uri)
    logger.info(f"{name!r} package done")


async def download_uri(session: ClientSession, paths: Paths, uri: str) -> None:
    file_name = get_file_name_from_uri(uri)
    logger.info(f"{file_name} taken")
    file_path = get_file_path_from_file_name(file_name, paths)
    if file_path.exists():
        logger.debug(f"{file_name!r} already downladed")
        return
    async with session.get(uri) as resp:
        if resp_not_ok(resp, uri):
            return
        content_raw = await resp.read()
    async with aiofiles.open(file_path, "wb") as fd:
        await fd.write(content_raw)
    logger.info(f"{file_name!r} done")


async def main(paths: Paths) -> None:
    async with ClientSession() as session:
        await get_package_names(session, paths)


def exception_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    logger.exception(context['exception'])


def prepare() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "path",
        nargs="?",
        default="PYPI",
        metavar="PATH",
        help="Path to cache directory",
    )
    args = parser.parse_args()
    working_dir = Path(args.path)
    paths = prepare_paths(working_dir)
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    with ThreadPoolExecutor() as executor:
        loop.set_default_executor(executor)
        try:
            logger.info("Starting...")
            asyncio.run(main(paths))
            logger.info("All done!")
        except KeyboardInterrupt:
            logger.info("Exiting...")


if __name__ == '__main__':
    prepare()
