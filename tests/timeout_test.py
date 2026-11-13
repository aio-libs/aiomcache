import asyncio
from typing import AsyncIterator, Tuple
from unittest import mock

import pytest

from aiomcache import Client


@pytest.fixture
async def server() -> AsyncIterator[Tuple[str, int]]:
    """Fake memcached: "get <key>" returns the key, after 0.2s for "slow"."""
    writers = set()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.add(writer)
        while line := await reader.readline():
            key = line.split()[1]
            if key == b"slow":
                await asyncio.sleep(0.2)
            writer.write(b"VALUE %s 0 %d\r\n%s\r\nEND\r\n" % (key, len(key), key))
            await writer.drain()
        writer.close()

    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    yield "127.0.0.1", srv.sockets[0].getsockname()[1]
    srv.close()
    for w in writers:
        w.close()
    await srv.wait_closed()


async def test_timeout(server: Tuple[str, int]) -> None:
    client = Client(*server, pool_size=1, timeout=0.1)
    with pytest.raises(asyncio.TimeoutError):
        await client.get(b"slow")
    # The timed-out connection must not be reused.
    assert await client.get(b"fast") == b"fast"
    await client.close()


async def test_conn_timeout() -> None:
    async def hang(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(10)

    client = Client("127.0.0.1", 11211, conn_timeout=0.1)
    with mock.patch("asyncio.open_connection", hang):
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")
    await client.close()
