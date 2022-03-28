import asyncio
import random

import pytest

from aiomcache.client import acquire
from aiomcache.pool import Connection, MemcachePool


@pytest.mark.asyncio
async def test_pool_creation(mcache_params):
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    assert pool.size() == 0
    assert pool._minsize == 1


@pytest.mark.asyncio
async def test_pool_acquire_release(mcache_params):
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)


@pytest.mark.asyncio
async def test_pool_acquire_release2(mcache_params):
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    reader, writer = await asyncio.open_connection(
        mcache_params["host"], mcache_params["port"])
    # put dead connection to the pool
    writer.close()
    reader.feed_eof()
    conn = Connection(reader, writer)
    await pool._pool.put(conn)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)


@pytest.mark.asyncio
async def test_pool_clear(mcache_params):
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    conn = await pool.acquire()
    pool.release(conn)
    assert pool.size() == 1
    await pool.clear()
    assert pool._pool.qsize() == 0


@pytest.mark.asyncio
async def test_acquire_dont_create_new_connection_if_have_conn_in_pool(
    mcache_params,
):
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    assert pool.size() == 0

    # Add a valid connection
    _conn = await pool._create_new_conn()
    assert _conn is not None
    await pool._pool.put(_conn)
    assert pool.size() == 1

    conn = await pool.acquire()
    assert conn is _conn
    assert pool.size() == 1


@pytest.mark.asyncio
async def test_acquire_limit_maxsize(mcache_params):
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    assert pool.size() == 0

    # Create up to max connections
    _conn = await pool.acquire()
    assert pool.size() == 1
    pool.release(_conn)

    async def acquire_wait_release():
        conn = await pool.acquire()
        assert conn is _conn
        await asyncio.sleep(0.01)
        assert len(pool._in_use) == 1
        assert pool.size() == 1
        assert pool._pool.qsize() == 0
        pool.release(conn)

    await asyncio.gather(*([acquire_wait_release()] * 50))
    assert pool.size() == 1
    assert len(pool._in_use) == 0
    assert pool._pool.qsize() == 1


@pytest.mark.asyncio
async def test_acquire_task_cancellation(
    mcache_params,
):

    class Client:
        def __init__(self, pool_size=4):
            self._pool = MemcachePool(
                minsize=pool_size, maxsize=pool_size,
                **mcache_params)

        @acquire
        async def acquire_wait_release(self, conn):
            assert self._pool.size() <= pool_size
            await asyncio.sleep(random.uniform(0.01, 0.02))  # noqa: S311
            return "foo"

    pool_size = 4
    client = Client(pool_size=pool_size)
    tasks = [
        asyncio.wait_for(
            client.acquire_wait_release(),
            random.uniform(1, 2)) for x in range(1000)  # noqa: S311
    ]
    results = await asyncio.gather(
        *tasks, return_exceptions=True)
    assert client._pool.size() <= pool_size
    assert len(client._pool._in_use) == 0
    assert "foo" in results


@pytest.mark.asyncio
async def test_maxsize_greater_than_minsize(mcache_params):
    pool = MemcachePool(minsize=5, maxsize=1, **mcache_params)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)


@pytest.mark.asyncio
async def test_0_minsize(mcache_params):
    pool = MemcachePool(minsize=0, maxsize=5, **mcache_params)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)


@pytest.mark.asyncio
async def test_bad_connection(mcache_params):
    pool = MemcachePool(minsize=5, maxsize=1, **mcache_params)
    pool._host = "INVALID_HOST"
    assert pool.size() == 0
    with pytest.raises(Exception):
        conn = await pool.acquire()
        assert isinstance(conn.reader, asyncio.StreamReader)
        assert isinstance(conn.writer, asyncio.StreamWriter)
        pool.release(conn)
    assert pool.size() == 0
