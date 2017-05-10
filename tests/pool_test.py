import asyncio
import pytest
import random
from aiomcache.pool import MemcachePool, _connection
from aiomcache.client import acquire


def test_pool_creation(mcache_params, loop):
    pool = MemcachePool(minsize=1, maxsize=5, loop=loop, **mcache_params)
    assert pool.size() == 0
    assert pool._minsize == 1


@pytest.mark.run_loop
def test_pool_acquire_release(mcache_params, loop):
    pool = MemcachePool(minsize=1, maxsize=5, loop=loop, **mcache_params)
    conn = yield from pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)


@pytest.mark.run_loop
def test_pool_acquire_release2(mcache_params, loop):
    pool = MemcachePool(minsize=1, maxsize=5, loop=loop, **mcache_params)
    reader, writer = yield from asyncio.open_connection(
        mcache_params['host'], mcache_params['port'], loop=loop)
    # put dead connection to the pool
    writer.close()
    reader.feed_eof()
    conn = _connection(reader, writer)
    pool._pool.append(conn)
    conn = yield from pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)


@pytest.mark.run_loop
def test_pool_clear(mcache_params, loop):
    pool = MemcachePool(minsize=1, maxsize=5, loop=loop, **mcache_params)
    conn = yield from pool.acquire()
    pool.release(conn)
    assert pool.size() == 1
    yield from pool.clear()
    assert len(pool._pool) == 0


@pytest.mark.run_loop
def test_acquire_dont_create_new_connection_if_have_conn_in_pool(mcache_params,
                                                                 loop):
    pool = MemcachePool(minsize=1, maxsize=5, loop=loop, **mcache_params)
    assert pool.size() == 0

    # Add a valid connection
    _conn = yield from pool._create_new_conn()
    pool._pool.append(_conn)
    assert pool.size() == 1

    conn = yield from pool.acquire()
    assert conn is _conn
    assert pool.size() == 1


@pytest.mark.run_loop
def test_acquire_limit_maxsize(mcache_params,
                               loop):
    pool = MemcachePool(minsize=1, maxsize=1, loop=loop, **mcache_params)
    assert pool.size() == 0

    # Create up to max connections
    _conn = yield from pool.acquire()
    assert pool.size() == 1
    pool.release(_conn)

    @asyncio.coroutine
    def acquire_wait_release():
        conn = yield from pool.acquire()
        assert conn is _conn
        yield from asyncio.sleep(0.01, loop=loop)
        assert len(pool._in_use) == 1
        assert pool.size() == 1
        assert len(pool._pool) == 0
        pool.release(conn)

    yield from asyncio.gather(*([acquire_wait_release()] * 50), loop=loop)
    assert pool.size() == 1
    assert len(pool._in_use) == 0
    assert len(pool._pool) == 1


@pytest.mark.run_loop
def test_acquire_task_cancellation(
        mcache_params, loop):

    class Client:
        def __init__(self):
            self._pool = MemcachePool(
                minsize=1, maxsize=1, loop=loop, **mcache_params)

        @acquire
        @asyncio.coroutine
        def acquire_wait_release(self, conn):
            assert self._pool.size() == 1
            assert len(self._pool._in_use) == 1
            yield from asyncio.sleep(random.uniform(0.01, 0.02), loop=loop)
            assert self._pool.size() == 1

    client = Client()
    _conn = yield from client._pool.acquire()
    assert client._pool.size() == 1
    client._pool.release(_conn)
    tasks = [
        asyncio.wait_for(
            client.acquire_wait_release(),
            random.uniform(1, 2), loop=loop) for x in range(400)
    ]
    yield from asyncio.gather(*tasks, loop=loop, return_exceptions=True)
    assert len(client._pool._in_use) == 0


@pytest.mark.run_loop
def test_maxsize_greater_than_minsize(mcache_params, loop):
    pool = MemcachePool(minsize=5, maxsize=1, loop=loop, **mcache_params)
    conn = yield from pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)


@pytest.mark.run_loop
def test_0_minsize(mcache_params, loop):
    pool = MemcachePool(minsize=0, maxsize=5, loop=loop, **mcache_params)
    conn = yield from pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)


@pytest.mark.run_loop
def test_bad_connection(mcache_params, loop):
    pool = MemcachePool(minsize=5, maxsize=1, loop=loop, **mcache_params)
    pool._host = "INVALID_HOST"
    assert pool.size() == 0
    with pytest.raises(Exception):
        conn = yield from pool.acquire()
        assert isinstance(conn.reader, asyncio.StreamReader)
        assert isinstance(conn.writer, asyncio.StreamWriter)
        pool.release(conn)
    assert pool.size() == 0
