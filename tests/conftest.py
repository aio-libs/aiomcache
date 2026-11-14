from collections.abc import AsyncIterator
from typing import Any, TypedDict
from urllib.parse import urlsplit

import pytest

import aiomcache
from .flag_helper import FlagHelperDemo


class McacheParams(TypedDict):
    host: str
    port: int


class McacheUnixParams(TypedDict):
    path: str


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--memcached", default="localhost:11211", metavar="HOST[:PORT]",
        help="Memcached server to test against (default: %(default)s)")
    # If starting memcached via systemd, ensure PrivateTmp is not on.
    parser.addoption(
        "--memcached-socket", default="/tmp/memcached.sock", metavar="PATH",  # noqa: S108
        help="Unix socket of a memcached server to test against (default: %(default)s)")


@pytest.fixture
def mcache_params(pytestconfig: pytest.Config) -> McacheParams:
    url = urlsplit("//" + pytestconfig.getoption("--memcached"))
    return {"host": url.hostname or "localhost", "port": url.port or 11211}


@pytest.fixture
def mcache_unix_params(pytestconfig: pytest.Config) -> McacheUnixParams:
    return {"path": pytestconfig.getoption("--memcached-socket")}


@pytest.fixture
async def mcache(mcache_params: McacheParams) -> AsyncIterator[aiomcache.Client]:
    client = aiomcache.Client(**mcache_params)
    yield client
    await client.close()


@pytest.fixture
async def mcache_unix(mcache_unix_params: McacheUnixParams) -> AsyncIterator[aiomcache.Client]:
    client = aiomcache.Client(path=mcache_unix_params["path"])
    yield client
    await client.close()


test_only_demo_flag_helper = FlagHelperDemo()


@pytest.fixture
async def demo_flag_helper() -> FlagHelperDemo:
    return test_only_demo_flag_helper


@pytest.fixture
async def mcache_flag_client(
    mcache_params: McacheParams, demo_flag_helper: FlagHelperDemo
) -> AsyncIterator[aiomcache.FlagClient[Any]]:

    client = aiomcache.FlagClient(
        get_flag_handler=demo_flag_helper.demo_get_flag_handler,
        set_flag_handler=demo_flag_helper.demo_set_flag_handler,
        **mcache_params)
    try:
        yield client
    finally:
        await client.close()
