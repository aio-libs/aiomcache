import asyncio
import pytest
from aiomcache.pool import MemcachePool, _connection


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
    yield from pool._pool.put(conn)
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
    assert pool._pool.qsize() == 0


@pytest.mark.run_loop
def test_pool_is_full(mcache_params, loop):
    pool = MemcachePool(minsize=1, maxsize=2, loop=loop, **mcache_params)
    conn = yield from pool.acquire()

    # put garbage to the pool make it look like full
    mocked_conns = [_connection(0, 0), _connection(1, 1)]
    yield from pool._pool.put(mocked_conns[0])
    yield from pool._pool.put(mocked_conns[1])

    # try to return connection back
    assert pool.size() == 3
    pool.release(conn)
    assert pool.size() == 2


@pytest.mark.run_loop
def test_acquire_dont_create_new_connection_if_have_conn_in_pool(mcache_params,
                                                                 loop):
    pool = MemcachePool(minsize=1, maxsize=5, loop=loop, **mcache_params)
    assert pool.size() == 0

    # Add a valid connection
    _conn = yield from pool._create_new_conn()
    yield from pool._pool.put(_conn)
    assert pool.size() == 1

    conn = yield from pool.acquire()
    assert conn is _conn
    assert pool.size() == 1
