import asyncio
import random
import socket
from typing import Optional

import pytest

from aiomcache.client import Client, acquire
from aiomcache.pool import Connection, MemcachePool
from .conftest import McacheParams, McacheUnixParams


async def test_pool_creation(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    assert pool.size() == 0
    assert pool._minsize == 1


async def test_pool_acquire_release(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)
    await pool.clear()


async def test_pool_acquire_release2(mcache_params: McacheParams) -> None:
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
    pool.release(conn)
    await pool.clear()


async def test_pool_clear(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=1, maxsize=5, **mcache_params)
    conn = await pool.acquire()
    pool.release(conn)
    assert pool.size() == 1
    await pool.clear()
    assert pool._pool.qsize() == 0


async def test_acquire_dont_create_new_connection_if_have_conn_in_pool(
    mcache_params: McacheParams,
) -> None:
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
    pool.release(conn)
    await pool.clear()


async def test_acquire_limit_maxsize(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    assert pool.size() == 0

    # Create up to max connections
    _conn = await pool.acquire()
    assert pool.size() == 1
    pool.release(_conn)

    async def acquire_wait_release() -> None:
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
    await pool.clear()


async def test_acquire_task_cancellation(mcache_params: McacheParams) -> None:

    class TestClient(Client):
        def __init__(self, pool_size: int = 4):
            self._pool = MemcachePool(
                minsize=pool_size, maxsize=pool_size,
                **mcache_params)

        @acquire
        async def acquire_wait_release(self, conn: Connection) -> str:
            assert self._pool.size() <= pool_size
            await asyncio.sleep(random.uniform(0.01, 0.02))  # noqa: S311
            return "foo"

    pool_size = 4
    client = TestClient(pool_size=pool_size)
    tasks = [
        asyncio.wait_for(
            client.acquire_wait_release(),
            random.uniform(1, 2)) for x in range(1000)  # noqa: S311
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert client._pool.size() <= pool_size
    assert len(client._pool._in_use) == 0
    assert "foo" in results
    await client._pool.clear()


async def test_cancelled_command_does_not_poison_pool(
    mcache_params: McacheParams,
) -> None:

    response_line_read = asyncio.Event()

    class CancellingClient(Client):
        @acquire
        async def get_then_cancel(self, conn: Connection, key: bytes) -> None:
            # Send a complete request but only consume part of the response,
            # leaving the rest of the reply in the stream.
            conn.writer.write(b'get ' + key + b'\r\n')
            await conn.writer.drain()
            await conn.reader.readline()
            response_line_read.set()
            await asyncio.Event().wait()

    client = CancellingClient(pool_size=1, **mcache_params)
    try:
        await client.set(b'key', b'value')

        command: asyncio.Future[None] = asyncio.ensure_future(
            client.get_then_cancel(b'key')
        )
        waiter_started = asyncio.Event()

        async def get_waiting_for_connection() -> Optional[bytes]:
            waiter_started.set()
            return await client.get(b'key')

        waiter: asyncio.Future[Optional[bytes]] = asyncio.ensure_future(
            get_waiting_for_connection()
        )

        try:
            await asyncio.wait_for(response_line_read.wait(), timeout=5)
            # Ensure the second command is blocked waiting for the only
            # connection before cancelling the first command.
            await asyncio.wait_for(waiter_started.wait(), timeout=5)
            command.cancel()
            with pytest.raises(asyncio.CancelledError):
                await command

            # The waiter must receive a replacement connection, not hang or
            # read the partial response left on the cancelled connection.
            assert await asyncio.wait_for(waiter, timeout=5) == b'value'
        finally:
            for task in (command, waiter):
                if not task.done():
                    task.cancel()
            await asyncio.gather(command, waiter, return_exceptions=True)
    finally:
        await client.close()


async def test_maxsize_greater_than_minsize(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=5, maxsize=1, **mcache_params)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)
    await pool.clear()


async def test_0_minsize(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=0, maxsize=5, **mcache_params)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)
    await pool.clear()


async def test_bad_connection() -> None:
    pool = MemcachePool("INVALID_HOST", 11211, minsize=5, maxsize=1)
    assert pool.size() == 0
    with pytest.raises(socket.gaierror):
        conn = await pool.acquire()
        assert isinstance(conn.reader, asyncio.StreamReader)
        assert isinstance(conn.writer, asyncio.StreamWriter)
        pool.release(conn)
    assert pool.size() == 0


async def test_pool_unix_socket_acquire_release(
    mcache_unix_params: McacheUnixParams,
) -> None:
    pool = MemcachePool(mcache_unix_params["path"], -1, minsize=1, maxsize=5)
    conn = await pool.acquire()
    assert isinstance(conn.reader, asyncio.StreamReader)
    assert isinstance(conn.writer, asyncio.StreamWriter)
    pool.release(conn)
    await pool.clear()


async def test_pool_unix_socket_bad_path() -> None:
    pool = MemcachePool("/tmp/nonexistent-mc.sock", -1, minsize=1, maxsize=1)  # noqa: S108
    assert pool.size() == 0
    with pytest.raises(FileNotFoundError):
        await pool.acquire()
