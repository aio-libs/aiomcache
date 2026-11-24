import asyncio
import random
import socket
from collections.abc import Sequence
from typing import Any, NoReturn
from unittest import mock

import pytest

from aiomcache.client import Client
from aiomcache.pool import Connection, MemcachePool
from .conftest import McacheParams, McacheUnixParams


def watch_pool_waits(
    pool: MemcachePool, parked: asyncio.Queue[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Put "queue" in parked each time an acquire() blocks waiting for a
    connection, so a test can order tasks without relying on the scheduler."""
    queue_get = pool._pool.get

    async def watched_get() -> Connection | None:
        if pool._pool.empty():
            parked.put_nowait("queue")
        return await queue_get()

    monkeypatch.setattr(pool._pool, "get", watched_get)


async def cancel_all(tasks: Sequence[asyncio.Future[Any]]) -> None:
    """Cancel tasks a test started and wait for them, so none is left behind."""
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


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


async def test_acquire_grows_pool_up_to_maxsize(mcache_params: McacheParams) -> None:
    pool = MemcachePool(minsize=1, maxsize=3, **mcache_params)

    # Hold each connection, so a new one must be opened for the next acquire.
    conns: list[Connection] = []
    try:
        for _ in range(3):
            conns.append(await asyncio.wait_for(pool.acquire(), 5))
        assert len(set(conns)) == 3
        assert pool.size() == 3
    finally:
        for conn in conns:
            pool.release(conn)
        await pool.clear()


async def test_client_uses_more_than_minsize_connections(mcache_params: McacheParams) -> None:
    client = Client(pool_size=3, pool_minsize=1, **mcache_params)
    try:
        await client.get(b"key")
        assert client._pool.size() == 1

        # Keep the only connection busy, so the next command needs a second one.
        busy = await client._pool.acquire()
        try:
            await asyncio.wait_for(client.get(b"key"), 5)
            assert client._pool.size() == 2
        finally:
            client._pool.release(busy)
    finally:
        await client.close()


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
    gate = asyncio.Event()
    parked = asyncio.Queue[str]()
    open_connection = asyncio.open_connection

    async def gated_open_connection(
        *args: Any, **kwargs: Any
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        parked.put_nowait("connect")
        await gate.wait()
        return await open_connection(*args, **kwargs)

    monkeypatch.setattr(asyncio, "open_connection", gated_open_connection)
    client = Client(pool_size=2, pool_minsize=1, **mcache_params)
    watch_pool_waits(client._pool, parked, monkeypatch)
    commands = [asyncio.create_task(client.get(b"key")) for _ in range(50)]
    try:
        async with asyncio.timeout(5):
            # Both connections open at once, beyond pool_minsize; the other
            # commands wait for them.
            states = [await parked.get() for _ in commands]
            assert states.count("connect") == 2
            assert states.count("queue") == 48
            gate.set()
            await asyncio.gather(*commands)
            assert client._pool.size() == 2

            # Lose every connection, as after a server restart, a network error
            # or a cancelled or timed-out command.
            gate.clear()
            conns = [await client._pool.acquire() for _ in range(2)]
            for conn in conns:
                conn.reader.feed_eof()
                client._pool.release(conn)
            commands += [asyncio.create_task(client.get(b"key")) for _ in range(50)]
            states = [await parked.get() for _ in range(50)]
            assert states.count("connect") == 2
            assert states.count("queue") == 48
            gate.set()
            await asyncio.gather(*commands)
            assert client._pool.size() == 2
    finally:
        await cancel_all(commands)
        await client.close()


async def test_concurrent_acquire_bad_connection(
    mcache_params: McacheParams, monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    parked = asyncio.Queue[str]()
    calls = 0

    async def failing_open_connection(*args: Any, **kwargs: Any) -> NoReturn:
        nonlocal calls
        calls += 1
        parked.put_nowait("connect")
        await gate.wait()
        raise ConnectionRefusedError("server down")

    monkeypatch.setattr(asyncio, "open_connection", failing_open_connection)
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    watch_pool_waits(pool, parked, monkeypatch)
    acquires = [asyncio.create_task(pool.acquire()) for _ in range(5)]
    try:
        async with asyncio.timeout(5):
            # One caller connects; the others wait for its connection.
            states = [await parked.get() for _ in acquires]
            assert states == ["connect"] + ["queue"] * 4
            gate.set()
            results = await asyncio.gather(*acquires, return_exceptions=True)
        # Each failure hands the slot to a waiter, which then tries to connect
        # itself: every caller gets the error and none is left waiting.
        assert all(isinstance(r, ConnectionRefusedError) for r in results)
        assert calls == 5
        # The last failure leaves its slot for the next acquire().
        assert pool.size() == 1
    finally:
        await cancel_all(acquires)
        await pool.clear()


async def test_cancelled_connect(
    mcache_params: McacheParams, monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    parked = asyncio.Queue[str]()
    open_connection = asyncio.open_connection

    async def gated_open_connection(
        *args: Any, **kwargs: Any
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        parked.put_nowait("connect")
        await gate.wait()
        return await open_connection(*args, **kwargs)

    monkeypatch.setattr(asyncio, "open_connection", gated_open_connection)
    pool = MemcachePool(minsize=1, maxsize=1, **mcache_params)
    watch_pool_waits(pool, parked, monkeypatch)
    acquires: list[asyncio.Future[Connection]] = []
    try:
        async with asyncio.timeout(5):
            # A command waiting for a connection that is cancelled while it
            # opens still gets a connection, which it opens itself.
            connecting = asyncio.create_task(pool.acquire())
            acquires.append(connecting)
            assert await parked.get() == "connect"
            waiting = asyncio.create_task(pool.acquire())
            acquires.append(waiting)
            assert await parked.get() == "queue"
            connecting.cancel()
            gate.set()
            conn = await waiting
            assert parked.get_nowait() == "connect"
            assert parked.empty()
            assert pool.size() == 1
            pool.release(conn)
    finally:
        await cancel_all(acquires)
        await pool.clear()


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
    parked = asyncio.Queue[str]()

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

    monkeypatch.setattr(asyncio, "open_connection", first_open_fails)
    watch_pool_waits(pool, parked, monkeypatch)
    acquires: list[asyncio.Future[Connection]] = []
    try:
        async with asyncio.timeout(5):
            reconnecting = asyncio.create_task(pool.acquire())
            acquires.append(reconnecting)
            assert await parked.get() == "connect"
            other = asyncio.create_task(pool.acquire())
            acquires.append(other)
            assert await parked.get() == "queue"
            gate.set()
            with pytest.raises(ConnectionRefusedError):
                await reconnecting
            conn = await other
        assert calls == 2
        assert pool.size() == 1
        pool.release(conn)
    finally:
        await cancel_all(acquires)
        await pool.clear()
        spare[1].close()


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
    # The failed connection's slot is kept for the next acquire() to retry.
    assert pool.size() == 1
    with pytest.raises(socket.gaierror):
        await pool.acquire()
    assert pool.size() == 1


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
