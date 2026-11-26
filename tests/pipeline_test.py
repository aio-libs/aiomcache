import asyncio
import contextlib
import datetime
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, assert_type
from unittest import mock

import pytest

from aiomcache import Client, FlagClient
from aiomcache.exceptions import ClientException, ValidationException
from aiomcache.pipeline import Pipeline, _SEND_SIZE
from aiomcache.pool import Connection
from .conftest import McacheParams, McacheUnixParams
from .flag_helper import FlagHelperDemo

_Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@pytest.fixture(params=("tcp", "unix"))
async def any_client(request: pytest.FixtureRequest, mcache_params: McacheParams,
                     mcache_unix_params: McacheUnixParams) -> AsyncIterator[Client]:
    if request.param == "tcp":
        client = Client(pool_size=1, **mcache_params)
    else:
        client = Client(path=mcache_unix_params["path"], pool_size=1)
    yield client
    await client.close()


@contextlib.asynccontextmanager
async def serve(handler: _Handler) -> AsyncIterator[int]:
    """Yield the port of a server that runs handler for each connection."""
    tasks: set[asyncio.Task[object]] = set()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        try:
            with contextlib.suppress(ConnectionError):
                await handler(reader, writer)
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await server.wait_closed()


@contextlib.asynccontextmanager
async def fake_memcached(handler: _Handler, **client_args: Any) -> AsyncIterator[Client]:
    """Yield a client of a server that runs handler for each connection."""
    async with serve(handler) as port:
        client = Client("127.0.0.1", port, pool_size=1, **client_args)
        try:
            yield client
        finally:
            await client.close()


async def answer_version(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Answer every request as if it were version, until the client hangs up."""
    while await reader.readline():
        writer.write(b"VERSION 1.2.3\r\n")


async def idle_connection(client: FlagClient[Any]) -> Connection:
    """Return the connection that the client's next command would use."""
    conn = await client._pool.acquire()
    client._pool.release(conn)
    return conn


async def test_pipeline(any_client: Client) -> None:
    await any_client.set(b"key:pipe:hits", b"1")
    conn = await idle_connection(any_client)

    async with any_client.pipeline() as pipe:
        results = await asyncio.gather(
            pipe.set(b"key:pipe:1", b"one"),
            pipe.get(b"key:pipe:1"),
            pipe.incr(b"key:pipe:hits"),
        )
        assert_type(results, tuple[bool, bytes | None, int | None])
        assert list(results) == [True, b"one", 2]  # gather() returns a list.

        # Every command can be sent this way, and each is carried out before
        # the next one sent.
        _, cas_token = await pipe.gets(b"key:pipe:1")
        assert cas_token is not None
        tomorrow = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)
        more: list[Any] = await asyncio.gather(
            pipe.append(b"key:pipe:1", b"!"),
            pipe.prepend(b"key:pipe:1", b"<"),
            pipe.multi_get(b"key:pipe:1", b"key:pipe:missing"),
            pipe.cas(b"key:pipe:1", b"stale", cas_token),
            pipe.replace(b"key:pipe:1", b"two", tomorrow),
            pipe.gets(b"key:pipe:missing"),
            pipe.delete(b"key:pipe:1"),
            pipe.get(b"key:pipe:1", b"default"),
            pipe.touch(b"key:pipe:1", 10),
            pipe.add(b"key:pipe:1", b"three"),
            pipe.decr(b"key:pipe:hits", 5),
            pipe.stats(),
            pipe.version(),
        )
        assert more[:11] == [True, True, (b"<one!", None), False, True, (None, None), True,
                             b"default", False, True, 0]
        assert b"pid" in more[11]
        assert more[12] == more[11][b"version"]

    # The connection goes back to the pool once the block ends.
    assert await idle_connection(any_client) is conn
    assert await any_client.get(b"key:pipe:1") == b"three"


async def test_pipeline_does_not_wait_for_replies() -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Answer nothing until every request is here.
        for _ in range(3):
            await reader.readline()
        writer.write(b"DELETED\r\nNOT_FOUND\r\n5\r\n")
        await answer_version(reader, writer)

    async with fake_memcached(handler) as client:
        async with client.pipeline() as pipe, asyncio.timeout(5):
            results = await asyncio.gather(
                pipe.delete(b"key:1"), pipe.delete(b"key:2"), pipe.incr(b"key:3"))
        assert list(results) == [True, False, 5]


async def test_pipeline_sends_requests_together(mcache: Client) -> None:
    writelines = asyncio.StreamWriter.writelines
    with mock.patch.object(asyncio.StreamWriter, "writelines", autospec=True,
                           side_effect=writelines) as spy:
        async with mcache.pipeline() as pipe:
            await asyncio.gather(*(pipe.set(b"key:pipe:%d" % i, b"v") for i in range(100)))
            assert spy.call_count == 1
            (_, chunks), _ = spy.call_args
            assert len(chunks) == 300  # The command, the value and the line end of each.

            # Without a pipeline, each command is sent by itself.
            await asyncio.gather(*(mcache.set(b"key:pipe:%d" % i, b"v") for i in range(100)))
            assert spy.call_count == 101


async def test_pipeline_one_command_at_a_time(mcache: Client) -> None:
    async with mcache.pipeline() as pipe:
        assert await pipe.set(b"key:pipe:seq", b"1") is True
        assert await pipe.incr(b"key:pipe:seq") == 2
        assert await pipe.get(b"key:pipe:seq") == b"2"
        await pipe.flush_all()
        assert await pipe.get(b"key:pipe:seq") is None


async def test_pipeline_keeps_order(mcache: Client) -> None:
    async with mcache.pipeline() as pipe:
        await pipe.set(b"key:pipe:order", b"")
        await asyncio.gather(
            *(pipe.append(b"key:pipe:order", b"%d," % i) for i in range(1000)))
        assert await pipe.get(b"key:pipe:order") == b"".join(b"%d," % i for i in range(1000))


async def test_pipeline_holds_one_connection(mcache_params: McacheParams) -> None:
    client = Client(pool_size=1, timeout=0.1, **mcache_params)
    try:
        async with client.pipeline() as pipe:
            assert client._pool.size() == 1
            version = await pipe.version()
            # The pool's only connection is taken, for commands and pipelines alike.
            with pytest.raises(TimeoutError):
                await client.version()
            with pytest.raises(TimeoutError):
                async with client.pipeline():
                    pytest.fail("entered the pipeline")  # pragma: no cover
            # A pipeline of the pipeline's client is the same pipeline.
            async with pipe.pipeline() as inner:
                assert inner is pipe
            assert await pipe.version() == version
            assert client._pool.size() == 1
    finally:
        await client.close()


async def test_pipeline_flag_handlers(
    mcache_flag_client: FlagClient[Any], demo_flag_helper: FlagHelperDemo
) -> None:
    items: dict[bytes, Any] = {
        b"key:pipe:date": datetime.date(2015, 12, 28),
        b"key:pipe:bytes": b"bytes",
        b"key:pipe:list": [1, 2, 3],
    }
    set_count = demo_flag_helper.set_invocation_count
    get_count = demo_flag_helper.get_invocation_count
    async with mcache_flag_client.pipeline() as pipe:
        assert await asyncio.gather(*(pipe.set(k, v) for k, v in items.items())) == [True] * 3
        assert await asyncio.gather(*(pipe.get(k) for k in items)) == list(items.values())
        assert await pipe.multi_get(*items) == tuple(items.values())
    # Only the values which are not bytes go through the handlers.
    assert demo_flag_helper.set_invocation_count == set_count + 2
    assert demo_flag_helper.get_invocation_count == get_count + 4


async def test_pipeline_validation_error(mcache_params: McacheParams) -> None:
    client = Client(pool_size=1, **mcache_params)
    try:
        conn = await idle_connection(client)
        async with client.pipeline() as pipe:
            results = await asyncio.gather(
                pipe.set(b"key:pipe:valid", b"1"),
                pipe.set(b"bad key", b"2"),
                pipe.get(b"key:pipe:valid"),
                return_exceptions=True,
            )
        assert results[0] is True
        assert isinstance(results[1], ValidationException)
        assert results[2] == b"1"
        assert await idle_connection(client) is conn
    finally:
        await client.close()


async def test_pipeline_error_reply_fails_only_its_command(mcache_params: McacheParams) -> None:
    client = Client(pool_size=1, **mcache_params)
    try:
        conn = await idle_connection(client)
        await client.set(b"key:pipe:text", b"text")
        async with client.pipeline() as pipe:
            results = await asyncio.gather(
                pipe.set(b"key:pipe:before", b"1"),
                # memcached rejects an item this large, and skips its data.
                pipe.set(b"key:pipe:large", b"x" * 2 * 1024 * 1024),
                pipe.incr(b"key:pipe:text"),
                pipe.set(b"key:pipe:after", b"2"),
                pipe.multi_get(b"key:pipe:before", b"key:pipe:large", b"key:pipe:after"),
                return_exceptions=True,
            )
        assert results[0] is True
        assert isinstance(results[1], ClientException)
        assert results[1].args == ("stats set failed", b"SERVER_ERROR object too large for cache")
        assert isinstance(results[2], ClientException)
        assert results[2].args == (
            "Memcached b'incr' command failed",
            b"CLIENT_ERROR cannot increment or decrement non-numeric value")
        assert results[3] is True
        assert results[4] == (b"1", None, b"2")
        # Every reply was read, so the connection is kept.
        assert await idle_connection(client) is conn
    finally:
        await client.close()


@pytest.mark.parametrize(("replies", "error"), (
    (b"STORED\r\nGARBAGE\r\nSTORED\r\n", ("stats set failed", b"GARBAGE")),
    # memcached may have taken the data sent with the command for another command.
    (b"STORED\r\nCLIENT_ERROR bad data chunk\r\nERROR\r\nSTORED\r\n",
     ("stats set failed", b"CLIENT_ERROR bad data chunk")),
    # memcached may have lost replies which it had not sent yet.
    (b"SERVER_ERROR out of memory writing get response\r\nSTORED\r\n",
     ("stats set failed", b"SERVER_ERROR out of memory writing get response")),
    (b"STORED\r\nSTO",
     ("Connection closed by the server before receiving a full response", None)),
), ids=("garbage", "CLIENT_ERROR", "SERVER_ERROR", "closed"))
async def test_pipeline_unreadable_reply_fails_later_commands(
    replies: bytes, error: tuple[object, ...]
) -> None:
    connections = 0

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal connections
        connections += 1
        if connections == 1:
            for _ in range(6):  # Three commands and their data.
                await reader.readline()
            writer.write(replies)
            if not replies.endswith(b"\r\n"):
                return
        await answer_version(reader, writer)

    async with fake_memcached(handler) as client:
        async with client.pipeline() as pipe, asyncio.timeout(5):
            results = await asyncio.gather(
                *(pipe.set(b"key:%d" % i, b"v") for i in range(3)), return_exceptions=True)
            failed = [r for r in results if r is not True]
            assert isinstance(failed[0], ClientException)
            assert failed[0].args == error
            # The replies can no longer be matched to their commands.
            assert len(failed) > 1
            for result in failed[1:]:
                assert isinstance(result, ClientException)
                assert result.args == ("Pipeline broken by an earlier command", None)
            with pytest.raises(ClientException, match="Pipeline broken by an earlier command"):
                await pipe.version()
        # The connection was discarded, so the next command gets its own reply.
        assert await client.version() == b"1.2.3"
        assert connections == 2


async def test_pipeline_cancelled_command() -> None:
    requests: list[bytes] = []
    reply = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        for _ in range(3):
            requests.append(await reader.readline())
        await reply.wait()
        writer.write(b"1\r\n2\r\n3\r\n")
        await answer_version(reader, writer)

    async with fake_memcached(handler) as client:
        conn = await idle_connection(client)
        async with client.pipeline() as pipe, asyncio.timeout(5):
            tasks = [asyncio.create_task(pipe.incr(b"key:%d" % i)) for i in range(3)]
            while len(requests) < 3:
                await asyncio.sleep(0)
            tasks[1].cancel()
            reply.set()
            # The request was sent, so its reply is read, for nobody. The
            # other commands get their own replies.
            results = await asyncio.gather(*tasks, return_exceptions=True)
            assert results[0] == 1
            assert isinstance(results[1], asyncio.CancelledError)
            assert results[2] == 3
        assert await idle_connection(client) is conn


async def test_pipeline_cancelled_command_reply_unreadable() -> None:
    requests: list[bytes] = []
    reply = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if not requests:
            for _ in range(2):
                requests.append(await reader.readline())
            await reply.wait()
            return  # Hang up.
        await answer_version(reader, writer)

    async with fake_memcached(handler) as client:
        async with client.pipeline() as pipe, asyncio.timeout(5):
            tasks = [asyncio.create_task(pipe.incr(b"key:%d" % i)) for i in range(2)]
            while len(requests) < 2:
                await asyncio.sleep(0)
            tasks[0].cancel()
            reply.set()
            with pytest.raises(ClientException, match="Pipeline broken by an earlier command"):
                await tasks[1]
        assert await client.version() == b"1.2.3"


async def test_pipeline_timeout() -> None:
    reply = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readline()
        await reply.wait()
        writer.write(b"1\r\n")
        await reader.readline()
        writer.write(b"2\r\n")
        await answer_version(reader, writer)

    async with fake_memcached(handler, timeout=0.1) as client:
        conn = await idle_connection(client)
        async with client.pipeline() as pipe:
            with pytest.raises(TimeoutError):
                await pipe.incr(b"key:1")
            # A late reply is read for nobody, and the next command gets its own.
            reply.set()
            assert await pipe.incr(b"key:2") == 2
        assert await idle_connection(client) is conn


async def test_pipeline_ends_with_commands_in_flight() -> None:
    connections = 0

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal connections
        connections += 1
        if connections == 1:
            await reader.read()  # Never answer.
        await answer_version(reader, writer)

    async with fake_memcached(handler, timeout=0.1) as client:
        async with client.pipeline() as pipe:
            with pytest.raises(TimeoutError):
                await pipe.version()
            in_flight = asyncio.create_task(pipe.version())
            await asyncio.sleep(0)
        with pytest.raises(ClientException, match="Pipeline closed before the reply was read"):
            await in_flight
        with pytest.raises(ClientException, match="Pipeline is closed"):
            await pipe.version()
        # Replies were left unread, so the connection was discarded.
        assert await client.version() == b"1.2.3"
        assert connections == 2


async def test_pipeline_closed(mcache: Client) -> None:
    async with mcache.pipeline() as pipe:
        await pipe.version()
    with pytest.raises(ClientException, match="Pipeline is closed"):
        await pipe.version()
    with pytest.raises(ClientException, match="Pipeline is closed"):
        async with pipe._connection():
            pytest.fail("got a connection")  # pragma: no cover


async def test_pipeline_many_commands(any_client: Client) -> None:
    items = {b"key:pipe:many%d" % i: b"%d" % i for i in range(5000)}
    async with any_client.pipeline() as pipe, asyncio.timeout(30):
        assert all(await asyncio.gather(*(pipe.set(k, v) for k, v in items.items())))
        values = await asyncio.gather(*(pipe.get(k) for k in items))
    assert values == list(items.values())


async def test_pipeline_large_requests_and_replies(any_client: Client) -> None:
    # memcached stops reading requests while its replies back up, so the
    # replies have to be read while the requests are sent. And the commands
    # wait for room in the write buffer, instead of all being buffered at once.
    value_size = 300_000
    items = {b"key:pipe:large%d" % i: bytes([65 + i % 26]) * value_size for i in range(30)}
    buffered = []
    writelines = asyncio.StreamWriter.writelines

    def spy(self: asyncio.StreamWriter, data: Any) -> None:
        writelines(self, data)
        buffered.append(self.transport.get_write_buffer_size())

    with mock.patch.object(asyncio.StreamWriter, "writelines", spy):
        async with any_client.pipeline() as pipe, asyncio.timeout(30):
            commands: list[Awaitable[object]] = []
            for key, value in items.items():
                commands += (pipe.set(key, value), pipe.get(key))
            results = await asyncio.gather(*commands)
    assert results[::2] == [True] * len(items)
    assert results[1::2] == list(items.values())
    assert max(buffered) <= value_size + _SEND_SIZE


async def test_pipeline_value_not_bytes(mcache_params: McacheParams) -> None:
    async def text_handler(value: object) -> tuple[bytes, int]:
        return "not bytes", 1  # type: ignore[return-value]

    client = FlagClient[object](pool_size=1, set_flag_handler=text_handler, **mcache_params)
    try:
        conn = await idle_connection(client)
        async with client.pipeline() as pipe:
            results = await asyncio.gather(
                pipe.set(b"key:pipe:text", b"1"),
                pipe.set(b"key:pipe:text", object()),
                pipe.get(b"key:pipe:text"),
                return_exceptions=True,
            )
        # None of the request was sent, so the other commands are not affected.
        assert results[0] is True
        assert isinstance(results[1], TypeError)
        assert results[2] == b"1"
        assert await idle_connection(client) is conn
    finally:
        await client.close()


@pytest.mark.skipif(sys.version_info < (3, 12), reason="No eager tasks before Python 3.12")
async def test_pipeline_eager_tasks(mcache: Client) -> None:
    # A task may run before create_task() returns it.
    if sys.version_info >= (3, 12):  # pragma: no branch
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
    async with mcache.pipeline() as pipe, asyncio.timeout(5):
        assert await pipe.set(b"key:pipe:eager", b"1") is True
        results = await asyncio.gather(*(pipe.incr(b"key:pipe:eager") for _ in range(100)))
        assert results == list(range(2, 102))
        assert await pipe.get(b"key:pipe:eager") == b"101"


async def read_line(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readline()
    if not line:
        raise ConnectionResetError("Connection lost")
    return line


async def test_pipeline_aborted_as_reply_arrives() -> None:
    # A request can't be written in the loop iteration that an earlier reply
    # arrives in.
    stream = asyncio.StreamReader()
    writer = mock.create_autospec(asyncio.StreamWriter, instance=True)
    writer.writelines.side_effect = (None, TypeError("not bytes"))
    pipeline = Pipeline(Connection(stream, writer))
    errors: list[dict[str, Any]] = []
    asyncio.get_running_loop().set_exception_handler(lambda loop, ctx: errors.append(ctx))

    writer.transport.is_closing.return_value = False
    first = await pipeline.send((b"version\r\n",), read_line)
    reader = pipeline._reader
    assert reader is not None
    await asyncio.sleep(0)  # The request is written, and its reply waited for.
    second = await pipeline.send((b"version\r\n",), read_line)
    stream.feed_data(b"VERSION 1.2.3\r\n")
    await asyncio.wait((reader,))

    for future in (first, second):
        with pytest.raises(ClientException, match="Pipeline broken by an earlier command"):
            await future
    assert reader.exception() is None
    assert [type(ctx["exception"]) for ctx in errors] == [TypeError]
    writer.transport.abort.assert_called_once_with()
    assert pipeline.close() is False


async def test_pipeline_connection_lost_before_request_is_written() -> None:
    stream = asyncio.StreamReader()
    writer = mock.create_autospec(asyncio.StreamWriter, instance=True)
    writer.transport.is_closing.return_value = True
    pipeline = Pipeline(Connection(stream, writer))

    first = await pipeline.send((b"version\r\n",), read_line)
    second = await pipeline.send((b"version\r\n",), read_line)
    await asyncio.sleep(0)
    writer.writelines.assert_not_called()
    stream.feed_eof()
    with pytest.raises(ConnectionResetError, match="Connection lost"):
        await first
    with pytest.raises(ClientException, match="Pipeline broken by an earlier command"):
        await second
    assert pipeline.close() is False
    await pipeline.wait_closed()


class RawClient(Client):
    """A client with a command that uses the connection itself."""

    async def raw_version(self, fail: bool = False) -> bytes:
        async with self._connection() as conn:
            conn.writer.write(b"version\r\n")
            if fail:
                raise RuntimeError("stopped before the reply was read")
            return (await read_line(conn.reader)).split()[1]


async def test_pipeline_command_using_connection(mcache_params: McacheParams) -> None:
    client = RawClient(pool_size=1, **mcache_params)
    try:
        conn = await idle_connection(client)
        await client.set(b"key:pipe:raw", b"1")
        version = await client.raw_version()
        async with client.pipeline() as pipe, asyncio.timeout(5):
            # It waits for the replies owed, and holds up the commands after it.
            results = await asyncio.gather(
                pipe.incr(b"key:pipe:raw"),
                pipe.raw_version(),
                pipe.incr(b"key:pipe:raw"),
                pipe.raw_version(),
            )
            assert list(results) == [2, version, 3, version]
        assert await idle_connection(client) is conn

        async with client.pipeline() as pipe, asyncio.timeout(5):
            failed = await asyncio.gather(
                pipe.incr(b"key:pipe:raw"),
                pipe.raw_version(fail=True),
                pipe.incr(b"key:pipe:raw"),
                return_exceptions=True,
            )
            assert failed[0] == 4
            assert isinstance(failed[1], RuntimeError)
            # Its reply may have been left unread.
            assert isinstance(failed[2], ClientException)
            assert failed[2].args == ("Pipeline broken by an earlier command", None)
        assert await idle_connection(client) is not conn

        # The pipeline ends while the command has the connection.
        async with client.pipeline() as pipe, asyncio.timeout(5):
            in_flight = asyncio.create_task(pipe.raw_version())
            await asyncio.sleep(0)
        with pytest.raises(ConnectionResetError, match="Connection lost"):
            await in_flight
        assert await client.raw_version() == version

        # The pipeline ends while the command waits for the connection, and
        # before the reply that it waits behind has started to be read.
        async with client.pipeline() as pipe:
            waiting = [asyncio.create_task(pipe.version()),
                       asyncio.create_task(pipe.raw_version())]
            await asyncio.sleep(0)
        async with asyncio.timeout(5):
            errors = await asyncio.gather(*waiting, return_exceptions=True)
            assert [e.args[0] for e in errors if isinstance(e, ClientException)] == [
                "Pipeline closed before the reply was read", "Pipeline is closed"]
            with pytest.raises(ClientException, match="Pipeline is closed"):
                await pipe.raw_version()
        assert await client.raw_version() == version
    finally:
        await client.close()
