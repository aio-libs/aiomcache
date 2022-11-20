import contextlib
import socket
import sys
import time
import uuid
from typing import Any, AsyncIterator, Callable, Iterator

import docker as docker_mod
import memcache
import pytest

import aiomcache
from .flag_helper import FlagHelperDemo

if sys.version_info < (3, 8):
    from typing_extensions import TypedDict
else:
    from typing import TypedDict

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired


class McacheParams(TypedDict):
    host: str
    port: int


class ServerParams(TypedDict):
    Id: NotRequired[str]
    host: str
    port: int
    mcache_params: McacheParams


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


@contextlib.contextmanager
def mcache_server_docker(  # type: ignore[no-any-unimported]
        unused_port: Callable[[], int], docker: docker_mod.Client, session_id: str
) -> Iterator[ServerParams]:
    docker.images.pull("memcached:alpine")
    container = docker.containers.run(
        image='memcached:alpine',
        name='memcached-test-server-{}'.format(session_id),
        ports={"11211/tcp": None},
        detach=True,
    )
    try:
        container.start()
        container.reload()
        net_settings = container.attrs["NetworkSettings"]
        host = net_settings["IPAddress"]
        port = int(net_settings["Ports"]["11211/tcp"][0]["HostPort"])
        mcache_params: McacheParams = {"host": host, "port": port}
        delay = 0.001
        for _i in range(10):
            try:
                conn = memcache.Client(["{host}:{port}".format_map(mcache_params)])
                conn.get_stats()
                break
            except Exception:
                time.sleep(delay)
                delay *= 2
        else:
            pytest.fail("Cannot start memcached")
        ret: ServerParams = {
            "Id": container.id,
            "host": host,
            "port": port,
            "mcache_params": mcache_params
        }
        time.sleep(0.1)
        yield ret
    finally:
        container.kill()
        container.remove()


@pytest.fixture(scope='session')
def mcache_server() -> ServerParams:
    return mcache_server_actual("localhost")


@pytest.fixture
def mcache_params(mcache_server: ServerParams) -> McacheParams:
    return mcache_server["mcache_params"]


@pytest.fixture
async def mcache(mcache_params: McacheParams) -> AsyncIterator[aiomcache.Client]:
    client = aiomcache.Client(**mcache_params)
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
