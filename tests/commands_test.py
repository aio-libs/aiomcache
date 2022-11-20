import asyncio
import datetime
from typing import Any
from unittest import mock
from unittest.mock import MagicMock

import pytest

from aiomcache import Client, FlagClient
from aiomcache.exceptions import ClientException, ValidationException
from .flag_helper import FlagHelperDemo


@pytest.mark.parametrize("key", (
    b"key",
    b"123",
    bytes("!@#", "utf-8"),
    bytes("中文", "utf-8"),
    bytes("こんにちは", "utf-8"),
    bytes("안녕하세요", "utf-8"),
))
@pytest.mark.asyncio
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
))
@pytest.mark.asyncio
async def test_invalid_key(mcache: Client, key: bytes) -> None:
    with pytest.raises(ValidationException, match="invalid key"):
        mcache._validate_key(key)


@pytest.mark.asyncio
async def test_version(mcache: Client) -> None:
    version = await mcache.version()
    stats = await mcache.stats()
    assert version == stats[b'version']

    with mock.patch.object(
            mcache,
            "_execute_simple_command",
            new_callable=MagicMock) as patched:
        fut: asyncio.Future[bytes] = asyncio.Future()
        fut.set_result(b'SERVER_ERROR error\r\n')
        patched.return_value = fut
        with pytest.raises(ClientException):
            await mcache.version()


@pytest.mark.asyncio
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

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        fut: asyncio.Future[bytes] = asyncio.Future()
        fut.set_result(b'SERVER_ERROR error\r\n')
        patched.return_value = fut
        with pytest.raises(ClientException):
            await mcache.flush_all()


@pytest.mark.asyncio
async def test_set_get(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value)
    test_value = await mcache.get(key)
    assert test_value == value
    test_value = await mcache.get(b"not:" + key)
    assert test_value is None
    test_value = await mcache.get(b"not:" + key, default=value)
    assert test_value == value

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        fut: asyncio.Future[bytes] = asyncio.Future()
        fut.set_result(b'SERVER_ERROR error\r\n')
        patched.return_value = fut
        with pytest.raises(ClientException):
            await mcache.set(key, value)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_multi_get_doubling_keys(mcache: Client) -> None:
    key, value = b'key:multi_get:3', b'1'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.multi_get(key, key)


@pytest.mark.asyncio
async def test_set_expire(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value, exptime=1)
    test_value = await mcache.get(key)
    assert test_value == value

    await asyncio.sleep(1)

    test_value = await mcache.get(key)
    assert test_value is None


@pytest.mark.asyncio
async def test_set_errors(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value, exptime=1)

    with pytest.raises(ValidationException):
        await mcache.set(key, value, exptime=-1)

    with pytest.raises(ValidationException):
        await mcache.set(key, value, exptime=3.14)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_gets_cas(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    await mcache.set(key, value)

    test_value, cas = await mcache.gets(key)

    assert cas is not None

    stored = await mcache.cas(key, value, cas)
    assert stored is True

    stored = await mcache.cas(key, value, cas)
    assert stored is False


@pytest.mark.asyncio
async def test_cas_missing(mcache: Client) -> None:
    key, value = b'key:set', b'1'
    stored = await mcache.cas(key, value, 123)
    assert stored is False


@pytest.mark.asyncio
async def test_add(mcache: Client) -> None:
    key, value = b'key:add', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.add(key, b"2")
    assert not test_value1

    test_value2 = await mcache.add(b"not:" + key, b"2")
    assert test_value2

    test_value3 = await mcache.get(b"not:" + key)
    assert test_value3 == b"2"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        fut: asyncio.Future[bytes] = asyncio.Future()
        fut.set_result(b'SERVER_ERROR error\r\n')
        patched.return_value = fut

        with pytest.raises(ClientException):
            await mcache.delete(key)


@pytest.mark.asyncio
async def test_delete_key_not_exists(mcache: Client) -> None:
    is_deleted = await mcache.delete(b"not:key")
    assert not is_deleted


@pytest.mark.asyncio
async def test_incr(mcache: Client) -> None:
    key, value = b'key:incr:1', b'1'
    await mcache.set(key, value)

    test_value1 = await mcache.incr(key, 2)
    assert test_value1 == 3

    # make sure value exists
    test_value2 = await mcache.get(key)
    assert test_value2 == b"3"


@pytest.mark.asyncio
async def test_incr_errors(mcache: Client) -> None:
    key, value = b'key:incr:2', b'string'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.incr(key, 2)

    with pytest.raises(ClientException):
        await mcache.incr(key, 3.14)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_decr(mcache: Client) -> None:
    key, value = b'key:decr:1', b'17'
    await mcache.set(key, value)

    test_value1 = await mcache.decr(key, 2)
    assert test_value1 == 15

    test_value2 = await mcache.get(key)
    assert test_value2 == b"15"

    test_value3 = await mcache.decr(key, 1000)
    assert test_value3 == 0


@pytest.mark.asyncio
async def test_decr_errors(mcache: Client) -> None:
    key, value = b'key:decr:2', b'string'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.decr(key, 2)

    with pytest.raises(ClientException):
        await mcache.decr(key, 3.14)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_stats(mcache: Client) -> None:
    stats = await mcache.stats()
    assert b'pid' in stats


@pytest.mark.asyncio
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

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        fut: asyncio.Future[bytes] = asyncio.Future()
        fut.set_result(b'SERVER_ERROR error\r\n')
        patched.return_value = fut

        with pytest.raises(ClientException):
            await mcache.touch(b"not:" + key, 1)


@pytest.mark.asyncio
async def test_close(mcache: Client) -> None:
    await mcache.close()
    assert mcache._pool.size() == 0


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
@pytest.mark.asyncio
async def test_flag_helper(
        mcache_flag_client: FlagClient[Any], value: object) -> None:
    key = b"key:test_flag_helper"

    await mcache_flag_client.set(key, value)
    v2 = await mcache_flag_client.get(key)
    assert v2 == value


@pytest.mark.asyncio
async def test_objects_not_supported_without_flag_handler(mcache: Client) -> None:
    key = b"key:test_objects_not_supported_without_flag_handler"

    date_value = datetime.date(2015, 12, 28)

    with pytest.raises(ValidationException):
        await mcache.set(key, date_value)  # type: ignore[arg-type]

    result = await mcache.get(key)
    assert result is None


@pytest.mark.asyncio
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
