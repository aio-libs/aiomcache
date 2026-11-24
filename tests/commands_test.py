import asyncio
import datetime
import enum
import time
from collections.abc import Awaitable, Callable, Iterator
from typing import Any
from unittest import mock

import pytest

from aiomcache import Client, FlagClient
from aiomcache.exceptions import ClientException, ValidationException
from aiomcache.pool import MemcachePool
from .conftest import McacheParams, McacheUnixParams
from .flag_helper import FlagHelperDemo


@pytest.mark.parametrize("key", (
    b"key",
    b"123",
    bytes("!@#", "utf-8"),
    bytes("中文", "utf-8"),
    bytes("こんにちは", "utf-8"),
    bytes("안녕하세요", "utf-8"),
))
async def test_valid_key(mcache: Client, key: bytes) -> None:
    assert mcache._validate_key(key) == key


@pytest.mark.parametrize("key", (
    # Whitespace
    b"foo bar",
    b"foo\t",
    b"\nbar",
    b"foo\x20\x0Dbar",
    b"\x18\x0E",
    b"\x20\x60",
    b"\x30\x00",
    b"\x20\x01",
    # Control characters
    b"foo\x00bar",
    b"\x1F",
    b"\x7F",
    "\u0080".encode(),
    "\u009F".encode(),
    # Non-UTF-8 bytes
    b"\x80",
    b"\xff",
    b"\xff\xfe",
))
async def test_invalid_key(mcache: Client, key: bytes) -> None:
    with pytest.raises(ValidationException, match="invalid key"):
        mcache._validate_key(key)


async def test_version(mcache: Client) -> None:
    version = await mcache.version()
    stats = await mcache.stats()
    assert bytes(version) == stats[b'version']


async def test_flush_all(mcache: Client) -> None:
    key, value = b'key:flush_all', b'flush_all_value'
    await mcache.set(key, value)
    # make sure value exists
    test_value = await mcache.get(key)
    assert test_value == value
    # flush data
    await mcache.flush_all()
    # make sure value does not exists
    test_value = await mcache.get(key)
    assert test_value is None


async def test_set_get(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value)
    test_value = await mcache.get(key)
    assert test_value == value
    test_value = await mcache.get(b"not:" + key)
    assert test_value is None
    test_value = await mcache.get(b"not:" + key, default=value)
    assert test_value == value


async def test_gets(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value)

    test_value, cas = await mcache.gets(key)
    assert test_value == value
    assert isinstance(cas, int)

    test_value, cas = await mcache.gets(b"not:" + key)
    assert test_value is None
    assert cas is None

    test_value, cas = await mcache.gets(b"not:" + key, default=value)
    assert test_value == value
    assert cas is None


async def test_multi_get(mcache: Client) -> None:
    key1, value1 = b'key:multi_get:1', b'1'
    key2, value2 = b'key:multi_get:2', b'2'
    await mcache.set(key1, value1)
    await mcache.set(key2, value2)
    test_value = await mcache.multi_get(key1, key2)
    assert test_value == (value1, value2)

    test_value = await mcache.multi_get(b'not' + key1, key2)
    assert test_value == (None, value2)
    test_value = await mcache.multi_get()
    assert test_value == ()


async def test_multi_get_doubling_keys(mcache: Client) -> None:
    key, value = b'key:multi_get:3', b'1'
    await mcache.set(key, value)

    with pytest.raises(ClientException, match="duplicate keys"):
        await mcache.multi_get(key, key)


async def test_set_expire(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value, exptime=1)
    test_value = await mcache.get(key)
    assert test_value == value

    await asyncio.sleep(1)

    test_value = await mcache.get(key)
    assert test_value is None


async def test_set_errors(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value, exptime=1)

    with pytest.raises(ValidationException):
        await mcache.set(key, value, exptime=-1)

    with pytest.raises(ValidationException):
        await mcache.set(key, value, exptime=3.14)  # type: ignore[arg-type]


async def test_gets_cas(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value)

    test_value, cas = await mcache.gets(key)

    assert cas is not None

    stored = await mcache.cas(key, value, cas)
    assert stored is True

    stored = await mcache.cas(key, value, cas)
    assert stored is False


async def test_cas_missing(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    stored = await mcache.cas(key, value, 123)
    assert stored is False


async def test_add(mcache: Client) -> None:
    key, value = b'key:add', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.add(key, b"2")
    assert not test_value1

    test_value2 = await mcache.add(b"not:" + key, b"2")
    assert test_value2

    test_value3 = await mcache.get(b"not:" + key)
    assert test_value3 == b"2"


async def test_replace(mcache: Client) -> None:
    key, value = b'key:replace', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.replace(key, b"2")
    assert test_value1
    # make sure value exists
    test_value2 = await mcache.get(key)
    assert test_value2 == b"2"

    test_value3 = await mcache.replace(b"not:" + key, b"3")
    assert not test_value3
    # make sure value exists
    test_value4 = await mcache.get(b"not:" + key)
    assert test_value4 is None


async def test_append(mcache: Client) -> None:
    key, value = b'key:append', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.append(key, b"2")
    assert test_value1

    # make sure value exists
    test_value2 = await mcache.get(key)
    assert test_value2 == b"12"

    test_value3 = await mcache.append(b"not:" + key, b"3")
    assert not test_value3
    # make sure value exists
    test_value4 = await mcache.get(b"not:" + key)
    assert test_value4 is None


async def test_prepend(mcache: Client) -> None:
    key, value = b'key:prepend', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.prepend(key, b"2")
    assert test_value1

    # make sure value exists
    test_value2 = await mcache.get(key)
    assert test_value2 == b"21"

    test_value3 = await mcache.prepend(b"not:" + key, b"3")
    assert not test_value3
    # make sure value exists
    test_value4 = await mcache.get(b"not:" + key)
    assert test_value4 is None


@pytest.mark.parametrize("exptime", (-1, 3.14, True))
async def test_append_prepend_errors(mcache: Client, exptime: Any) -> None:
    key = b"key:append_prepend"

    with pytest.raises(ValidationException):
        await mcache.append(key, b"2", exptime)

    with pytest.raises(ValidationException):
        await mcache.prepend(key, b"2", exptime)


async def test_delete(mcache: Client) -> None:
    key, value = b'key:delete', b'value'
    await mcache.set(key, value)

    # make sure value exists
    test_value = await mcache.get(key)
    assert test_value == value

    is_deleted = await mcache.delete(key)
    assert is_deleted
    # make sure value does not exists
    test_value = await mcache.get(key)
    assert test_value is None


async def test_delete_key_not_exists(mcache: Client) -> None:
    is_deleted = await mcache.delete(b"not:key")
    assert not is_deleted


async def test_incr(mcache: Client) -> None:
    key, value = b'key:incr:1', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.incr(key, 2)
    assert test_value1 == 3

    # make sure value exists
    test_value2 = await mcache.get(key)
    assert test_value2 == b"3"


async def test_incr_errors(mcache: Client) -> None:
    key, value = b'key:incr:2', b'string'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.incr(key, 2)

    with pytest.raises(ClientException):
        await mcache.incr(key, 3.14)  # type: ignore[arg-type]

    ivalue = await mcache.incr(b"not:" + key, 5)
    assert ivalue is None


async def test_decr(mcache: Client) -> None:
    key, value = b'key:decr:1', b'17'
    await mcache.set(key, value)

    test_value1 = await mcache.decr(key, 2)
    assert test_value1 == 15

    test_value2 = await mcache.get(key)
    assert test_value2 == b"15"

    test_value3 = await mcache.decr(key, 1000)
    assert test_value3 == 0


async def test_decr_errors(mcache: Client) -> None:
    key, value = b'key:decr:2', b'string'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.decr(key, 2)

    with pytest.raises(ClientException):
        await mcache.decr(key, 3.14)  # type: ignore[arg-type]

    ivalue = await mcache.decr(b"not:" + key, 6)
    assert ivalue is None


async def test_stats(mcache: Client) -> None:
    stats = await mcache.stats()
    assert b'pid' in stats


async def test_touch(mcache: Client) -> None:
    key, value = b'key:touch:1', b'17'
    await mcache.set(key, value)

    test_value1 = await mcache.touch(key, 1)
    assert test_value1

    test_value2 = await mcache.get(key)
    assert test_value2 == value

    await asyncio.sleep(1)

    test_value3 = await mcache.get(key)
    assert test_value3 is None

    test_value4 = await mcache.touch(b"not:" + key, 1)
    assert not test_value4


async def test_touch_errors(mcache: Client) -> None:
    key, value = b"key:touch:2", b"17"
    await mcache.set(key, value)

    with pytest.raises(ValidationException):
        await mcache.touch(key, -1)

    with pytest.raises(ValidationException):
        await mcache.touch(key, 3.14)  # type: ignore[arg-type]

    assert await mcache.get(key) == value


async def test_close(mcache: Client) -> None:
    await mcache.close()
    assert mcache._pool.size() == 0


async def test_simple_command_server_dies_mid_response() -> None:
    async def handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        await reader.readline()
        writer.write(b"VERSION 1.6.0")  # incomplete line, no \r\n
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = Client("127.0.0.1", port)
    try:
        with pytest.raises(ClientException, match="closed by the server"):
            await asyncio.wait_for(client.version(), timeout=5)
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


@pytest.mark.parametrize(
    "value",
    [
        "key",
        b"bkey",
        False,
        1,
        None,
        0.5,
        [1, 2, 3],
        tuple([1, 2, 3]),
        [datetime.date(2015, 12, 28)],
        bytes("!@#", "utf-8"),
        bytes("안녕하세요", "utf-8"),
    ]
)
async def test_flag_helper(
        mcache_flag_client: FlagClient[Any], value: object) -> None:
    key = b"key:test_flag_helper"

    await mcache_flag_client.set(key, value)
    v2 = await mcache_flag_client.get(key)
    assert v2 == value


async def test_objects_not_supported_without_flag_handler(mcache: Client) -> None:
    key = b"key:test_objects_not_supported_without_flag_handler"

    date_value = datetime.date(2015, 12, 28)

    with pytest.raises(ValidationException):
        await mcache.set(key, date_value)  # type: ignore[arg-type]

    result = await mcache.get(key)
    assert result is None


async def test_flag_handler_invoked_only_when_expected(
        mcache_flag_client: FlagClient[Any], demo_flag_helper: FlagHelperDemo) -> None:
    key = b"key:test_flag_handler_invoked_only_when_expected"

    orig_get_count = demo_flag_helper.get_invocation_count
    orig_set_count = demo_flag_helper.set_invocation_count

    # should be invoked on non-byte values

    date_value = datetime.date(2015, 12, 28)

    await mcache_flag_client.set(key, date_value)
    v2 = await mcache_flag_client.get(key)
    assert v2 == date_value

    assert orig_get_count + 1 == demo_flag_helper.get_invocation_count
    assert orig_set_count + 1 == demo_flag_helper.set_invocation_count

    # should not be invoked on byte values

    byte_value = bytes("안녕하세요", "utf-8")

    await mcache_flag_client.set(key, byte_value)
    v3 = await mcache_flag_client.get(key)
    assert v3 == byte_value

    assert orig_get_count + 1 == demo_flag_helper.get_invocation_count
    assert orig_set_count + 1 == demo_flag_helper.set_invocation_count


async def test_client_unix_socket_set_get(
    mcache_unix_params: McacheUnixParams,
) -> None:
    client = Client(path=mcache_unix_params["path"])
    try:
        await client.set(b"key", b"value")
        v = await client.get(b"key")
        assert v == b"value"
    finally:
        await client.close()


# memcached limits keys to 250 bytes, not characters.
KEY_250_BYTES = "é".encode() * 125  # 125 characters
KEY_251_BYTES = b"a" * 249 + "é".encode()  # 250 characters
KEY_500_BYTES = "é".encode() * 250  # 250 characters

_KeyCommand = Callable[[Client, bytes], Awaitable[object]]
KEY_COMMANDS: dict[str, _KeyCommand] = {
    "get": lambda mc, k: mc.get(k),
    "gets": lambda mc, k: mc.gets(k),
    "multi_get": lambda mc, k: mc.multi_get(b"key:other", k),
    "set": lambda mc, k: mc.set(k, b"v"),
    "add": lambda mc, k: mc.add(k, b"v"),
    "replace": lambda mc, k: mc.replace(k, b"v"),
    "append": lambda mc, k: mc.append(k, b"v"),
    "prepend": lambda mc, k: mc.prepend(k, b"v"),
    "cas": lambda mc, k: mc.cas(k, b"v", 1),
    "delete": lambda mc, k: mc.delete(k),
    "incr": lambda mc, k: mc.incr(k),
    "decr": lambda mc, k: mc.decr(k),
    "touch": lambda mc, k: mc.touch(k, 10),
}


@pytest.mark.parametrize("key", (KEY_251_BYTES, KEY_500_BYTES), ids=("251B", "500B"))
@pytest.mark.parametrize("command", KEY_COMMANDS)
async def test_key_over_250_bytes(mcache: Client, command: str, key: bytes) -> None:
    with pytest.raises(ValidationException,
                       match=f"key too long: {len(key)} bytes, max 250"):
        await KEY_COMMANDS[command](mcache, key)


@pytest.mark.parametrize("key", (KEY_250_BYTES, b"a" * 248 + "é".encode(), b"a" * 250))
async def test_key_of_250_bytes(mcache: Client, key: bytes) -> None:
    assert mcache._validate_key(key) == key
    assert await mcache.set(key, b"v") is True
    assert await mcache.get(key) == b"v"
    assert await mcache.delete(key) is True


_InvalidCall = Callable[[Client], Awaitable[object]]

# A key with a space is rejected by the client, before it is sent.
INVALID_CALLS: dict[str, _InvalidCall] = {
    "get": lambda mc: mc.get(b"bad key"),
    "gets": lambda mc: mc.gets(b"bad key"),
    "multi_get": lambda mc: mc.multi_get(b"key:other", b"bad key"),
    "set": lambda mc: mc.set(b"bad key", b"v"),
    "add": lambda mc: mc.add(b"bad key", b"v"),
    "replace": lambda mc: mc.replace(b"bad key", b"v"),
    "append": lambda mc: mc.append(b"bad key", b"v"),
    "prepend": lambda mc: mc.prepend(b"bad key", b"v"),
    "cas": lambda mc: mc.cas(b"bad key", b"v", 1),
    "delete": lambda mc: mc.delete(b"bad key"),
    "incr": lambda mc: mc.incr(b"bad key"),
    "decr": lambda mc: mc.decr(b"bad key"),
    "touch": lambda mc: mc.touch(b"bad key", 10),
    "str-key": lambda mc: mc.delete("key"),  # type: ignore[arg-type]
    "set-negative-exptime": lambda mc: mc.set(b"key", b"v", exptime=-1),
    "set-float-exptime": lambda mc: mc.set(b"key", b"v", exptime=1.5),  # type: ignore[arg-type]
    "set-no-flag-handler": lambda mc: mc.set(b"key", object()),  # type: ignore[arg-type]
}


@pytest.mark.parametrize("call", ("get", "set-negative-exptime", "set-no-flag-handler"))
async def test_validation_error_keeps_connection(
    mcache_params: McacheParams, call: str
) -> None:
    # A rejected call must not close the pooled connection.
    client = Client(pool_size=1, **mcache_params)
    try:
        conn = await client._pool.acquire()
        client._pool.release(conn)
        with pytest.raises(ValidationException):
            await INVALID_CALLS[call](client)
        assert client._pool.size() == 1
        reacquired = await client._pool.acquire()
        client._pool.release(reacquired)
        assert reacquired is conn
        assert not conn.writer.is_closing()
    finally:
        await client.close()


@pytest.mark.parametrize("call", INVALID_CALLS)
async def test_validation_before_acquire(mcache: Client, call: str) -> None:
    with mock.patch.object(MemcachePool, "acquire",
                           side_effect=AssertionError("connection acquired")):
        with pytest.raises(ValidationException):
            await INVALID_CALLS[call](mcache)


async def test_set_flag_handler_error_before_acquire(mcache_params: McacheParams) -> None:
    # The set_flag_handler runs before a connection is taken.
    async def failing_handler(value: object) -> tuple[bytes, int]:
        raise ValueError("handler failed")

    client = FlagClient[object](set_flag_handler=failing_handler, **mcache_params)
    try:
        with mock.patch.object(MemcachePool, "acquire",
                               side_effect=AssertionError("connection acquired")):
            with pytest.raises(ValueError, match="handler failed"):
                await client.set(b"key", object())
    finally:
        await client.close()


# An unexpected reply, followed by bytes the client must never read.
UNEXPECTED_REPLY = b"SERVER_ERROR boom\r\nVERSION stale\r\n"
UNEXPECTED_CALLS: dict[str, tuple[_InvalidCall, str]] = {
    "delete": (lambda mc: mc.delete(b"key"), "Memcached delete failed"),
    "set": (lambda mc: mc.set(b"key", b"v"), "stats set failed"),
    "cas": (lambda mc: mc.cas(b"key", b"v", 1), "stats cas failed"),
    "incr": (lambda mc: mc.incr(b"key"), "Memcached b'incr' command failed"),
    "decr": (lambda mc: mc.decr(b"key"), "Memcached b'decr' command failed"),
    "touch": (lambda mc: mc.touch(b"key", 1), "Memcached touch failed"),
    "version": (lambda mc: mc.version(), "Memcached version failed"),
    "flush_all": (lambda mc: mc.flush_all(), "Memcached flush_all failed"),
    "get": (lambda mc: mc.get(b"key"), "get failed"),
}


@pytest.mark.parametrize("call", UNEXPECTED_CALLS)
async def test_unexpected_reply_discards_connection(call: str) -> None:
    # The first command gets an unexpected reply with extra bytes after it.
    # The connection must be discarded, so the next command gets its own
    # reply and not the leftover bytes.
    connections = 0
    first = True

    async def handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        nonlocal connections, first
        connections += 1
        while line := await reader.readline():
            if line.split()[0] in (b"set", b"cas"):
                await reader.readline()  # The data block.
            writer.write(UNEXPECTED_REPLY if first else b"VERSION 1.2.3\r\n")
            first = False
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = Client("127.0.0.1", port, pool_size=1)
    func, message = UNEXPECTED_CALLS[call]
    try:
        with pytest.raises(ClientException) as excinfo:
            await func(client)
        assert excinfo.value.args == (message, b"SERVER_ERROR boom\r\n"
                                      if call == "get" else b"SERVER_ERROR boom")
        assert await client.version() == b"1.2.3"
        assert connections == 2
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


async def test_malformed_version_discards_connection() -> None:
    # A reply that passes the prefix check but fails to parse must also
    # discard the connection, so the leftover bytes are never read.
    replies = [b"VERSION\r\nVERSION stale\r\n", b"VERSION 1.2.3\r\n"]

    async def handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        while await reader.readline():
            writer.write(replies.pop(0))
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = Client("127.0.0.1", port, pool_size=1)
    try:
        with pytest.raises(ValueError):
            await client.version()
        assert await client.version() == b"1.2.3"
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


async def test_storage_command_writes_value_without_copy(mcache: Client) -> None:
    # The value is written as is once a connection is held, not copied into a
    # full command while waiting for one.
    value = b"x" * 100_000
    writelines = asyncio.StreamWriter.writelines
    with mock.patch.object(asyncio.StreamWriter, "writelines", autospec=True,
                           side_effect=writelines) as spy:
        assert await mcache.set(b"key:nocopy", value) is True
    (_, chunks), _ = spy.call_args
    assert any(chunk is value for chunk in chunks)
    assert await mcache.get(b"key:nocopy") == value


DAY = 24 * 60 * 60
# memcached's last Unix timestamp: 2038-01-19 03:14:07 UTC.
MAX_EXPTIME = 2**31 - 1
NOW = 1_800_000_000  # 2027-01-15 08:00:00 UTC
UTC = datetime.UTC


@pytest.fixture
def frozen_clock() -> Iterator[None]:
    """Stop the client's clock at NOW."""
    with mock.patch("aiomcache.client.time") as time_mock:
        time_mock.time.return_value = NOW + 0.5
        yield


@pytest.mark.parametrize(("exptime", "sent"), (
    # Up to 30 days is sent as seconds from now.
    (30 * DAY, 30 * DAY),
    # memcached reads more as a Unix timestamp, so the one it ends at is sent.
    (30 * DAY + 1, NOW + 30 * DAY + 1),
    (MAX_EXPTIME - NOW, MAX_EXPTIME),
    # A datetime is sent as its Unix timestamp.
    (datetime.datetime(2030, 1, 1, tzinfo=UTC), 1893456000),
    (datetime.datetime(2030, 1, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=1))),
     1893456000),
    (datetime.datetime(2038, 1, 19, 3, 14, 7, tzinfo=UTC), MAX_EXPTIME),
    # memcached would read these as seconds from now, or as never for the epoch.
    # They are in the past, so the first timestamp it reads as one is sent.
    (datetime.datetime(1970, 1, 31, tzinfo=UTC), 30 * DAY + 1),
    (datetime.datetime(1970, 1, 1, tzinfo=UTC), 30 * DAY + 1),
    (datetime.datetime(1969, 12, 31, tzinfo=UTC), 30 * DAY + 1),
))
@pytest.mark.usefixtures("frozen_clock")
def test_validate_exptime(exptime: int | datetime.datetime, sent: int) -> None:
    assert Client("127.0.0.1", 11211)._validate_exptime(exptime) == sent


@pytest.mark.parametrize("exptime", (NOW, MAX_EXPTIME))
@pytest.mark.usefixtures("frozen_clock")
def test_validate_exptime_timestamp_deprecated(exptime: int) -> None:
    with pytest.warns(DeprecationWarning):
        assert Client("127.0.0.1", 11211)._validate_exptime(exptime) == exptime


@pytest.mark.parametrize("exptime", (
    -1,
    MAX_EXPTIME + 1,
    # Too long to be seconds from now, and in the past as a timestamp.
    MAX_EXPTIME - NOW + 1,
    NOW - 1,
    True,
    3.14,
    datetime.datetime(2030, 1, 1),  # Not timezone-aware.
    datetime.datetime(2038, 1, 19, 3, 14, 8, tzinfo=UTC),  # MAX_EXPTIME + 1
))
@pytest.mark.usefixtures("frozen_clock")
def test_validate_exptime_invalid(exptime: Any) -> None:
    with pytest.raises(ValidationException):
        Client("127.0.0.1", 11211)._validate_exptime(exptime)


async def _add(mc: Client, key: bytes, exptime: int | datetime.datetime) -> bool:
    await mc.delete(key)
    return await mc.add(key, b"value", exptime)


async def _replace(mc: Client, key: bytes, exptime: int | datetime.datetime) -> bool:
    await mc.set(key, b"old")
    return await mc.replace(key, b"value", exptime)


async def _cas(mc: Client, key: bytes, exptime: int | datetime.datetime) -> bool:
    await mc.set(key, b"old")
    _, cas_token = await mc.gets(key)
    assert cas_token is not None
    return await mc.cas(key, b"value", cas_token, exptime)


async def _touch(mc: Client, key: bytes, exptime: int | datetime.datetime) -> bool:
    await mc.set(key, b"value")
    return await mc.touch(key, exptime)


# Each gives the key an item with a value of b"value" and the exptime.
_ExptimeCommand = Callable[[Client, bytes, int | datetime.datetime], Awaitable[bool]]
EXPTIME_COMMANDS: dict[str, _ExptimeCommand] = {
    "set": lambda mc, k, exptime: mc.set(k, b"value", exptime),
    "add": _add,
    "replace": _replace,
    "cas": _cas,
    "touch": _touch,
}


@pytest.mark.parametrize("exptime", (
    30 * DAY + 1,
    datetime.datetime.now(UTC) + datetime.timedelta(days=31),
), ids=("int", "datetime"))
@pytest.mark.parametrize("command", EXPTIME_COMMANDS)
async def test_exptime_over_30_days(
    mcache: Client, command: str, exptime: int | datetime.datetime
) -> None:
    key = b"key:exptime:" + command.encode()
    assert await EXPTIME_COMMANDS[command](mcache, key, exptime) is True
    assert await mcache.get(key) == b"value"


@pytest.mark.parametrize("exptime", (
    datetime.datetime(2000, 1, 1, tzinfo=UTC),
    datetime.datetime(1970, 1, 1, tzinfo=UTC),
), ids=("2000", "epoch"))
async def test_exptime_past_datetime(mcache: Client, exptime: datetime.datetime) -> None:
    key = b"key:exptime:past"
    assert await mcache.set(key, b"value", exptime) is True
    assert await mcache.get(key) is None


@pytest.mark.parametrize("command", EXPTIME_COMMANDS)
async def test_exptime_timestamp_deprecated(mcache: Client, command: str) -> None:
    key = b"key:exptime:" + command.encode()
    timestamp = int(time.time()) + 31 * DAY
    with pytest.warns(DeprecationWarning) as warned:
        assert await EXPTIME_COMMANDS[command](mcache, key, timestamp) is True
    # The warning must point to the caller of the command.
    assert [w.filename for w in warned] == [__file__]
    assert await mcache.get(key) == b"value"


class _TTL(enum.IntEnum):
    HOUR = 3600


class _Flags(enum.IntFlag):
    PICKLE = 1


async def test_int_enum_arguments(mcache: Client) -> None:
    key = b"key:int_enum"
    assert await mcache.set(key, b"value", exptime=_TTL.HOUR)
    assert await mcache.get(key) == b"value"
    assert await mcache.touch(key, _TTL.HOUR)
    _, cas_token = await mcache.gets(key)
    assert cas_token is not None
    assert await mcache.cas(key, b"other", cas_token, exptime=_TTL.HOUR)
    assert await mcache.get(key) == b"other"

    await mcache.set(key, b"1")
    assert await mcache.incr(key, _TTL.HOUR) == 3601
    assert await mcache.decr(key, _TTL.HOUR) == 1


async def test_int_flag_from_flag_handler(mcache_params: McacheParams) -> None:
    async def get_handler(value: bytes, flags: int) -> str:
        assert flags == _Flags.PICKLE
        return value.decode()

    async def set_handler(value: str) -> tuple[bytes, int]:
        return value.encode(), _Flags.PICKLE

    client = FlagClient(get_flag_handler=get_handler, set_flag_handler=set_handler,
                        **mcache_params)
    try:
        assert await client.set(b"key:int_flag", "value")
        assert await client.get(b"key:int_flag") == "value"
    finally:
        await client.close()
