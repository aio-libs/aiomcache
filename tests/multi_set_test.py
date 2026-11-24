import asyncio
import datetime
from collections.abc import AsyncIterator
from typing import Any
from unittest import mock

import pytest

from aiomcache import Client, FlagClient
from aiomcache.client import _MULTI_SET_BATCH_BYTES
from aiomcache.exceptions import ClientException
from aiomcache.pool import MemcachePool
from .conftest import McacheParams, McacheUnixParams
from .flag_helper import FlagHelperDemo


@pytest.fixture(params=("tcp", "unix"))
async def any_client(request: pytest.FixtureRequest, mcache_params: McacheParams,
                     mcache_unix_params: McacheUnixParams) -> AsyncIterator[Client]:
    if request.param == "tcp":
        client = Client(**mcache_params)
    else:
        client = Client(path=mcache_unix_params["path"])
    yield client
    await client.close()


async def test_multi_set(any_client: Client) -> None:
    await any_client.set(b"key:ms:2", b"old")
    items = {b"key:ms:1": b"1", b"key:ms:2": b"", "中文".encode(): b"3" * 1000}
    assert await any_client.multi_set(items) is None  # type: ignore[func-returns-value]
    assert await any_client.multi_get(*items) == tuple(items.values())


async def test_multi_set_empty(mcache: Client) -> None:
    with mock.patch.object(MemcachePool, "acquire",
                           side_effect=AssertionError("connection acquired")):
        await mcache.multi_set({})


async def test_multi_set_flag_handler(
    mcache_flag_client: FlagClient[Any], demo_flag_helper: FlagHelperDemo
) -> None:
    items: dict[bytes, Any] = {
        b"key:ms:date": datetime.date(2015, 12, 28),
        b"key:ms:bytes": b"bytes",
        b"key:ms:list": [1, 2, 3],
    }
    set_count = demo_flag_helper.set_invocation_count
    await mcache_flag_client.multi_set(items)
    # Only non-bytes values go through the handler.
    assert demo_flag_helper.set_invocation_count == set_count + 2
    assert await mcache_flag_client.multi_get(*items) == tuple(items.values())


async def test_multi_set_rejected_items(mcache_params: McacheParams) -> None:
    # memcached rejects an item too large, skips its data and carries on, so
    # every reply is read, the rest are stored and the connection is kept.
    client = Client(pool_size=1, **mcache_params)
    try:
        await client.set(b"key:ms:big", b"old")
        conn = await client._pool.acquire()
        client._pool.release(conn)
        items = {b"key:ms:before": b"1", b"key:ms:big": b"x" * 2 * 1024 * 1024,
                 b"key:ms:after": b"2"}
        with pytest.raises(ClientException) as excinfo:
            await client.multi_set(items)
        assert excinfo.value.args == (
            "Memcached multi_set failed for some keys",
            {b"key:ms:big": b"SERVER_ERROR object too large for cache"})
        # memcached removes the old item rather than leave it stale.
        assert await client.multi_get(*items) == (b"1", None, b"2")
        reacquired = await client._pool.acquire()
        client._pool.release(reacquired)
        assert reacquired is conn
    finally:
        await client.close()


@pytest.mark.parametrize("n", (1000, 2001))
async def test_multi_set_reads_every_reply(mcache_params: McacheParams, n: int) -> None:
    # A reply left unread would be taken as the reply to the next command.
    client = Client(pool_size=1, **mcache_params)
    try:
        items = {b"key:ms:r%d" % i: b"%d" % i for i in range(n)}
        await client.multi_set(items)
        assert await client.delete(b"key:ms:r0") is True
        assert await client.multi_get(*items) == (None, *list(items.values())[1:])
    finally:
        await client.close()


async def test_multi_set_many_keys(any_client: Client) -> None:
    # memcached stops reading while its replies back up, so waiting for the
    # commands to be sent before reading the replies would hang.
    items = {b"key:ms:many%d" % i: b"%d" % i for i in range(100_000)}
    async with asyncio.timeout(30):
        await any_client.multi_set(items)
    sample = list(items)[::997]
    assert await any_client.multi_get(*sample) == tuple(items[k] for k in sample)


async def test_multi_set_bounds_buffered_bytes(any_client: Client) -> None:
    # Large values are sent a batch at a time, so only about two batches
    # are ever buffered, not the whole call.
    value_size = 300_000
    items = {b"key:ms:large%d" % i: bytes([65 + i % 26]) * value_size for i in range(30)}
    buffered = []
    writelines = asyncio.StreamWriter.writelines

    def spy(self: asyncio.StreamWriter, data: Any) -> None:
        writelines(self, data)
        buffered.append(self.transport.get_write_buffer_size())

    with mock.patch.object(asyncio.StreamWriter, "writelines", spy):
        async with asyncio.timeout(30):
            await any_client.multi_set(items)
    assert max(buffered) <= 2 * (_MULTI_SET_BATCH_BYTES + value_size)
    assert await any_client.multi_get(*items) == tuple(items.values())


async def _serve(replies: bytes) -> tuple[asyncio.Server, list[int]]:
    """Send replies to the first three sets, closing if they end mid-line,
    then answer version."""
    connections = [0]

    async def handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        connections[0] += 1
        if connections[0] == 1:
            for _ in range(6):  # Three commands and their data blocks.
                await reader.readline()
            writer.write(replies)
            if not replies.endswith(b"\r\n"):
                await writer.drain()
                writer.close()
                return
        while await reader.readline():
            writer.write(b"VERSION 1.2.3\r\n")
        writer.close()

    return await asyncio.start_server(handler, "127.0.0.1", 0), connections


@pytest.mark.parametrize(("replies", "error"), (
    (b"STORED\r\nERROR\r\nSTORED\r\n", ("Memcached multi_set failed", b"ERROR")),
    (b"STORED\r\nCLIENT_ERROR bad data chunk\r\nSTORED\r\n",
     ("Memcached multi_set failed", b"CLIENT_ERROR bad data chunk")),
    # Earlier rejected items are not reported, as the stream can't be trusted.
    (b"SERVER_ERROR out of memory\r\nCLIENT_ERROR bad data chunk\r\n",
     ("Memcached multi_set failed", b"CLIENT_ERROR bad data chunk")),
    (b"STORED\r\nSTO",
     ("Connection closed by the server before receiving a full response", None)),
), ids=("ERROR", "CLIENT_ERROR", "after-rejected", "closed"))
async def test_multi_set_unexpected_reply_discards_connection(
    replies: bytes, error: tuple[object, ...]
) -> None:
    server, connections = await _serve(replies)
    port = server.sockets[0].getsockname()[1]
    client = Client("127.0.0.1", port, pool_size=1)
    try:
        with pytest.raises(ClientException) as excinfo:
            async with asyncio.timeout(5):
                await client.multi_set({b"key:1": b"1", b"key:2": b"2", b"key:3": b"3"})
        assert excinfo.value.args == error
        # The next command gets its own reply, on a new connection.
        assert await client.version() == b"1.2.3"
        assert connections[0] == 2
    finally:
        await client.close()
        server.close()
        await server.wait_closed()
