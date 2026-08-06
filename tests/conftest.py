import socket
import sys
import uuid
from typing import Any, AsyncIterator, Callable, TypedDict

import docker as docker_mod
import pytest

import aiomcache
from .flag_helper import FlagHelperDemo

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired


class McacheParams(TypedDict):
    host: str
    port: int


class McacheUnixParams(TypedDict):
    path: str


class ServerParams(TypedDict):
    Id: NotRequired[str]
    host: str
    port: int
    mcache_params: McacheParams


class UnixServerParams(TypedDict):
    Id: NotRequired[str]
    path: str
    mcache_unix_params: McacheUnixParams


mcache_server_option = "localhost"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        '--memcached', help='Memcached server')


@pytest.fixture(scope='session')
def unused_port() -> Callable[[], int]:
    def f() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]  # type: ignore[no-any-return]
    return f


def pytest_runtest_setup(item: pytest.Item) -> None:
    global mcache_server_option
    mcache_server_option = item.config.getoption("--memcached", "localhost")


@pytest.fixture(scope='session')
def session_id() -> str:
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker() -> docker_mod.Client:  # type: ignore[no-any-unimported]
    return docker_mod.from_env()


def mcache_server_actual(host: str, port: int = 11211) -> ServerParams:
    port = int(port)
    return {
        "host": host,
        "port": port,
        "mcache_params": {"host": host, "port": port}
    }


@pytest.fixture(scope='session')
def mcache_server() -> ServerParams:
    return mcache_server_actual("localhost")


def mcache_unix_server_actual(path: str) -> UnixServerParams:
    return {
        "path": path,
        "mcache_unix_params": {"path": path}
    }


@pytest.fixture(scope='session')
def mcache_unix_server(session_id: str) -> UnixServerParams:
    # if starting memcached via systemd, ensure privatetmp is not on
    sock_path = '/tmp/memcached.sock'  # noqa: S108
    return mcache_unix_server_actual(sock_path)


@pytest.fixture
def mcache_params(mcache_server: ServerParams) -> McacheParams:
    return mcache_server["mcache_params"]


@pytest.fixture
def mcache_unix_params(mcache_unix_server: UnixServerParams) -> McacheUnixParams:
    return mcache_unix_server["mcache_unix_params"]


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
