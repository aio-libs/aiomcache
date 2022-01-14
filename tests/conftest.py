import contextlib
import socket
import time
import uuid

import docker as docker_mod
import memcache
import pytest

import aiomcache


mcache_server_option = "localhost"


def pytest_addoption(parser):
    parser.addoption(
        '--memcached', help='Memcached server')


@pytest.fixture(scope='session')
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return f


def pytest_runtest_setup(item):
    global mcache_server_option
    mcache_server_option = item.config.getoption("--memcached", "localhost")


@pytest.fixture(scope='session')
def session_id():
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    return docker_mod.from_env()


def mcache_server_actual(host, port=11211):
    port = int(port)
    container = {
        'host': host,
        'port': port,
    }
    container['mcache_params'] = container.copy()
    return container


@contextlib.contextmanager
def mcache_server_docker(unused_port, docker, session_id):
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
        mcache_params = dict(host=host, port=port)
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
        ret = {"Id": container.id}
        ret["host"] = host
        ret["port"] = port
        ret["mcache_params"] = mcache_params
        time.sleep(0.1)
        yield ret
    finally:
        container.kill()
        container.remove()


@pytest.fixture(scope='session')
def mcache_server(unused_port, docker, session_id):
    return mcache_server_actual("localhost")


@pytest.fixture
def mcache_params(mcache_server):
    return dict(**mcache_server['mcache_params'])


@pytest.fixture
async def mcache(mcache_params):
    client = aiomcache.Client(**mcache_params)
    yield client
    await client.close()
