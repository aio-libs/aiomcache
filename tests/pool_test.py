import asyncio
import random
import socket
from typing import Any
from unittest import mock

import pytest

from aiomcache.client import Client
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


async def test_acquire_recovers_when_reconnect_fails(
    mcache_params: McacheParams,
) -> None:
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    conn = await pool.acquire()
    pool.release(conn, broken=True)

    with mock.patch.object(
        MemcachePool, "_create_new_conn", side_effect=OSError("refused")
    ):
        with pytest.raises(OSError, match="refused"):
            await pool.acquire()
    # The failed reconnect isn't counted, so the next acquire() opens one.
    assert pool.size() == 0

    conn = await pool.acquire()
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
            self._timeout = None
            self._pool = MemcachePool(
                minsize=pool_size, maxsize=pool_size,
                **mcache_params)

        async def acquire_wait_release(self) -> str:
            async with self._connection():
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
        async def get_then_cancel(self, key: bytes) -> None:
            async with self._connection() as conn:
                # Send a complete request but only consume part of the response,
                # leaving the rest of the reply in the stream.
                conn.writer.write(b"get " + key + b"\r\n")
                await conn.writer.drain()
                await conn.reader.readline()
                response_line_read.set()
                await asyncio.Event().wait()

    client = CancellingClient(pool_size=1, **mcache_params)
    try:
        await client.set(b'key', b'value')

        async def cancel_command() -> None:
            await client.get_then_cancel(b'key')

        async def get_waiting_for_connection() -> bytes | None:
            return await client.get(b'key')

        async with asyncio.TaskGroup() as tasks:
            command = tasks.create_task(cancel_command())
            waiter = tasks.create_task(get_waiting_for_connection())
            await asyncio.wait_for(response_line_read.wait(), timeout=5)
            command.cancel()
            with pytest.raises(asyncio.CancelledError):
                await command

            # The waiter must receive a replacement connection, not hang or
            # read the partial response left on the cancelled connection.
            assert await asyncio.wait_for(waiter, timeout=5) == b'value'
    finally:
        await client.close()


async def test_concurrent_commands_open_at_most_pool_size_connections(
    mcache_params: McacheParams, monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = 0
    open_connection = asyncio.open_connection

    async def counting_open_connection(
        *args: Any, **kwargs: Any
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        nonlocal opened
        opened += 1
        return await open_connection(*args, **kwargs)

    monkeypatch.setattr(asyncio, "open_connection", counting_open_connection)
    client = Client(pool_size=2, **mcache_params)
    try:
        await asyncio.gather(*(client.get(b"key") for _ in range(50)))
        assert opened == 2
        assert client._pool.size() == 2

        # Lose every connection, as after a server restart, a network error
        # or a cancelled or timed-out command.
        conns = [await client._pool.acquire() for _ in range(2)]
        for conn in conns:
            conn.reader.feed_eof()
            client._pool.release(conn)
        await asyncio.gather(*(client.get(b"key") for _ in range(50)))
        assert opened == 4
        assert client._pool.size() == 2
    finally:
        await client.close()


async def test_concurrent_acquire_bad_connection() -> None:
    pool = MemcachePool("/tmp/nonexistent-mc.sock", -1, minsize=1, maxsize=1)  # noqa: S108
    results = await asyncio.gather(
        *(pool.acquire() for _ in range(5)), return_exceptions=True)
    # Every waiter gets the error; none is left waiting for a connection.
    assert all(isinstance(r, FileNotFoundError) for r in results)
    assert pool.size() == 0


async def test_cancelled_connect(
    mcache_params: McacheParams, monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    # Each acquire() puts an item here when it blocks, so the test can order
    # the tasks without relying on the scheduler.
    parked: asyncio.Queue[str] = asyncio.Queue()
    open_connection = asyncio.open_connection

    async def gated_open_connection(
        *args: Any, **kwargs: Any
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        parked.put_nowait("connect")
        await gate.wait()
        return await open_connection(*args, **kwargs)

    monkeypatch.setattr(asyncio, "open_connection", gated_open_connection)
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    queue_get = pool._pool.get

    async def watched_get() -> Connection | None:
        if pool._pool.empty():
            parked.put_nowait("queue")
        return await queue_get()

    monkeypatch.setattr(pool._pool, "get", watched_get)

    # A command waiting for a connection that is cancelled while it opens
    # still gets a connection.
    connecting = asyncio.ensure_future(pool.acquire())
    assert await parked.get() == "connect"
    waiting = asyncio.ensure_future(pool.acquire())
    assert await parked.get() == "queue"
    connecting.cancel()
    gate.set()
    conn = await asyncio.wait_for(waiting, 5)
    assert pool.size() == 1
    pool.release(conn)
    await pool.clear()

    # Both cancelled at once, as when they share a deadline: nothing is left
    # counted in size().
    gate.clear()
    parked = asyncio.Queue()
    connecting = asyncio.ensure_future(pool.acquire())
    assert await parked.get() == "connect"
    waiting = asyncio.ensure_future(pool.acquire())
    assert await parked.get() == "queue"
    connecting.cancel()
    waiting.cancel()
    await asyncio.gather(connecting, waiting, return_exceptions=True)
    # Run the callbacks the cancellation scheduled with call_soon() first.
    ran = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().call_soon(ran.set_result, None)
    await ran
    assert pool.size() == 0
    await pool.clear()


async def test_replacing_a_discarded_connection_opens_one_connection(
    mcache_params: McacheParams, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client(pool_size=1, **mcache_params)
    try:
        await client.set(b"key", b"value")
        # Discard the only connection, as after a cancelled or timed-out command.
        conn = await client._pool.acquire()
        client._pool.release(conn, broken=True)

        opened = 0
        open_connection = asyncio.open_connection

        async def counting_open_connection(
            *args: Any, **kwargs: Any
        ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            nonlocal opened
            opened += 1
            return await open_connection(*args, **kwargs)

        monkeypatch.setattr(asyncio, "open_connection", counting_open_connection)
        results = await asyncio.gather(*(client.get(b"key") for _ in range(20)))
        assert results == [b"value"] * 20
        assert opened == 1
        assert client._pool.size() == 1
    finally:
        await client.close()


async def test_failed_reconnect_does_not_exceed_pool_size(
    mcache_params: McacheParams, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    conn = await pool.acquire()
    conn.reader.feed_eof()
    pool.release(conn)  # Discarded, so the next acquire() reconnects.

    spare = await asyncio.open_connection(mcache_params["host"], mcache_params["port"])
    gate = asyncio.Event()
    calls = 0

    # Each acquire() puts an item here when it blocks or connects, so the
    # test can order the tasks without relying on the scheduler.
    parked: asyncio.Queue[str] = asyncio.Queue()

    async def first_open_fails(
        *args: Any, **kwargs: Any
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        nonlocal calls
        calls += 1
        parked.put_nowait("connect")
        if calls == 1:
            await gate.wait()
            raise ConnectionRefusedError("server down")
        return spare

    queue_get = pool._pool.get

    async def watched_get() -> Connection | None:
        if pool._pool.empty():
            parked.put_nowait("queue")
        return await queue_get()

    monkeypatch.setattr(asyncio, "open_connection", first_open_fails)
    monkeypatch.setattr(pool._pool, "get", watched_get)
    reconnecting = asyncio.ensure_future(pool.acquire())
    assert await parked.get() == "connect"
    other = asyncio.ensure_future(pool.acquire())
    assert await parked.get() == "queue"
    gate.set()
    with pytest.raises(ConnectionRefusedError):
        await reconnecting
    conn = await asyncio.wait_for(other, 5)
    assert calls == 2
    assert pool.size() == 1
    pool.release(conn)
    await pool.clear()


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
        await pool.acquire()
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
