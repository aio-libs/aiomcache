import asyncio
from typing import AsyncIterator, Optional, Set, Tuple
from unittest import mock

import pytest

from aiomcache import Client


@pytest.fixture
async def server() -> AsyncIterator[Tuple[str, int]]:
    """Fake memcached that accepts commands but never replies."""
    handlers: Set["asyncio.Task[object]"] = set()

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


async def get(client: Client, key: bytes) -> Optional[bytes]:
    """Call client.get(), failing instead of hanging if the timeout never fires."""
    task = asyncio.ensure_future(client.get(key))
    done, _ = await asyncio.wait((task,), timeout=5)
    if not done:
        task.cancel()
        pytest.fail("client timeout did not fire")
    return task.result()


async def test_timeout(server: Tuple[str, int]) -> None:
    client = Client(*server, timeout=0.1)
    try:
        with pytest.raises(asyncio.TimeoutError):
            await get(client, b"key")
    finally:
        await client.close()


async def test_timeout_waiting_for_connection(server: Tuple[str, int]) -> None:
    client = Client(*server, pool_size=1, timeout=0.1)
    try:
        conn = await client._pool.acquire()  # Take the only connection.
        with pytest.raises(asyncio.TimeoutError):
            await get(client, b"key")
        client._pool.release(conn)
    finally:
        await client.close()


async def test_conn_timeout() -> None:
    async def hang(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(10)

    client = Client("127.0.0.1", 11211, conn_timeout=0.1)
    with mock.patch("asyncio.open_connection", hang):
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")
    await client.close()
