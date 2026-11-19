import asyncio
import time
from collections.abc import AsyncIterator
from unittest import mock

import pytest

from aiomcache import Client


@pytest.fixture
async def server() -> AsyncIterator[tuple[str, int]]:
    """Fake memcached that accepts commands but never replies."""
    handlers: set[asyncio.Task[object]] = set()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            await reader.read()  # Read until the client hangs up.
        finally:
            writer.close()

    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    yield "127.0.0.1", srv.sockets[0].getsockname()[1]
    srv.close()
    for task in handlers:
        task.cancel()
    await asyncio.gather(*handlers, return_exceptions=True)
    await srv.wait_closed()


async def get(client: Client, key: bytes) -> bytes | None:
    """Call client.get(), failing instead of hanging if the timeout never fires."""
    task = asyncio.ensure_future(client.get(key))
    done, _ = await asyncio.wait((task,), timeout=5)
    if not done:
        task.cancel()
        pytest.fail("client timeout did not fire")
    return task.result()


async def test_timeout(server: tuple[str, int]) -> None:
    client = Client(*server, timeout=0.1)
    try:
        with pytest.raises(TimeoutError):
            await get(client, b"key")
    finally:
        await client.close()


async def test_timeout_waiting_for_connection(server: tuple[str, int]) -> None:
    client = Client(*server, pool_size=1, timeout=0.1)
    try:
        conn = await client._pool.acquire()  # Take the only connection.
        with pytest.raises(TimeoutError):
            await get(client, b"key")
        client._pool.release(conn)
    finally:
        await client.close()


async def test_conn_timeout() -> None:
    async def hang(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(10)

    client = Client("127.0.0.1", 11211, conn_timeout=0.1)
    with mock.patch("asyncio.open_connection", hang):
        with pytest.raises(TimeoutError):
            await client.get(b"key")
    await client.close()


async def test_timeout_while_connecting(server: tuple[str, int]) -> None:
    """The command timeout must win even if the connection opens as it fires.

    Regression test: on Python 3.11, a nested asyncio.wait_for() swallows the
    cancellation when its inner future completes at the same moment, so the
    command kept running (and waited forever for a reply) past its timeout.
    """
    conn = await asyncio.open_connection(*server)

    async def open_as_deadline_passes(*args: object, **kwargs: object) -> object:
        # Block the loop past the command deadline, then finish on the next
        # iteration: the same iteration the expired deadline is handled.
        time.sleep(0.2)
        await asyncio.sleep(0)
        return conn

    client = Client(*server, timeout=0.1, conn_timeout=10)
    try:
        with mock.patch("asyncio.open_connection", open_as_deadline_passes):
            with pytest.raises(TimeoutError):
                await get(client, b"key")
    finally:
        conn[1].close()
        await client.close()
