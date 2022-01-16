import asyncio
from unittest import mock
from unittest.mock import MagicMock

import pytest

from aiomcache.exceptions import ClientException, ValidationException


@pytest.mark.parametrize("key", (
    b"key",
    b"123",
    bytes("!@#", "utf-8"),
    bytes("中文", "utf-8"),
    bytes("こんにちは", "utf-8"),
    bytes("안녕하세요", "utf-8"),
))
@pytest.mark.asyncio
async def test_valid_key(mcache, key):
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
async def test_invalid_key(mcache, key):
    with pytest.raises(ValidationException, match="invalid key"):
        mcache._validate_key(key)


@pytest.mark.asyncio
async def test_version(mcache):
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
async def test_flush_all(mcache):
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
async def test_set_get(mcache):
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
async def test_gets(mcache):
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
async def test_multi_get(mcache):
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
async def test_multi_get_doubling_keys(mcache):
    key, value = b'key:multi_get:3', b'1'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.multi_get(key, key)


@pytest.mark.asyncio
async def test_set_expire(mcache):
    key, value = b'key:set', b'1'
    await mcache.set(key, value, exptime=1)
    test_value = await mcache.get(key)
    assert test_value == value

    await asyncio.sleep(1)

    test_value = await mcache.get(key)
    assert test_value is None


@pytest.mark.asyncio
async def test_set_errors(mcache):
    key, value = b'key:set', b'1'
    await mcache.set(key, value, exptime=1)

    with pytest.raises(ValidationException):
        await mcache.set(key, value, exptime=-1)

    with pytest.raises(ValidationException):
        await mcache.set(key, value, exptime=3.14)


@pytest.mark.asyncio
async def test_gets_cas(mcache):
    key, value = b'key:set', b'1'
    await mcache.set(key, value)

    test_value, cas = await mcache.gets(key)

    stored = await mcache.cas(key, value, cas)
    assert stored is True

    stored = await mcache.cas(key, value, cas)
    assert stored is False


@pytest.mark.asyncio
async def test_cas_missing(mcache):
    key, value = b'key:set', b'1'
    stored = await mcache.cas(key, value, 123)
    assert stored is False


@pytest.mark.asyncio
async def test_add(mcache):
    key, value = b'key:add', b'1'
    await mcache.set(key, value)

    test_value = await mcache.add(key, b"2")
    assert not test_value

    test_value = await mcache.add(b"not:" + key, b"2")
    assert test_value

    test_value = await mcache.get(b"not:" + key)
    assert test_value == b'2'


@pytest.mark.asyncio
async def test_replace(mcache):
    key, value = b'key:replace', b'1'
    await mcache.set(key, value)

    test_value = await mcache.replace(key, b"2")
    assert test_value
    # make sure value exists
    test_value = await mcache.get(key)
    assert test_value == b'2'

    test_value = await mcache.replace(b"not:" + key, b"3")
    assert not test_value
    # make sure value exists
    test_value = await mcache.get(b"not:" + key)
    assert test_value is None


@pytest.mark.asyncio
async def test_append(mcache):
    key, value = b'key:append', b'1'
    await mcache.set(key, value)

    test_value = await mcache.append(key, b"2")
    assert test_value

    # make sure value exists
    test_value = await mcache.get(key)
    assert test_value == b'12'

    test_value = await mcache.append(b"not:" + key, b"3")
    assert not test_value
    # make sure value exists
    test_value = await mcache.get(b"not:" + key)
    assert test_value is None


@pytest.mark.asyncio
async def test_prepend(mcache):
    key, value = b'key:prepend', b'1'
    await mcache.set(key, value)

    test_value = await mcache.prepend(key, b"2")
    assert test_value

    # make sure value exists
    test_value = await mcache.get(key)
    assert test_value == b'21'

    test_value = await mcache.prepend(b"not:" + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = await mcache.get(b"not:" + key)
    assert test_value is None


@pytest.mark.asyncio
async def test_delete(mcache):
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
async def test_delete_key_not_exists(mcache):
    is_deleted = await mcache.delete(b"not:key")
    assert not is_deleted


@pytest.mark.asyncio
async def test_incr(mcache):
    key, value = b'key:incr:1', b'1'
    await mcache.set(key, value)

    test_value = await mcache.incr(key, 2)
    assert test_value == 3

    # make sure value exists
    test_value = await mcache.get(key)
    assert test_value == b'3'


@pytest.mark.asyncio
async def test_incr_errors(mcache):
    key, value = b'key:incr:2', b'string'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.incr(key, 2)

    with pytest.raises(ClientException):
        await mcache.incr(key, 3.14)


@pytest.mark.asyncio
async def test_decr(mcache):
    key, value = b'key:decr:1', b'17'
    await mcache.set(key, value)

    test_value = await mcache.decr(key, 2)
    assert test_value == 15

    test_value = await mcache.get(key)
    assert test_value == b'15'

    test_value = await mcache.decr(key, 1000)
    assert test_value == 0


@pytest.mark.asyncio
async def test_decr_errors(mcache):
    key, value = b'key:decr:2', b'string'
    await mcache.set(key, value)

    with pytest.raises(ClientException):
        await mcache.decr(key, 2)

    with pytest.raises(ClientException):
        await mcache.decr(key, 3.14)


@pytest.mark.asyncio
async def test_stats(mcache):
    stats = await mcache.stats()
    assert b'pid' in stats


@pytest.mark.asyncio
async def test_touch(mcache):
    key, value = b'key:touch:1', b'17'
    await mcache.set(key, value)

    test_value = await mcache.touch(key, 1)
    assert test_value

    test_value = await mcache.get(key)
    assert test_value == value

    await asyncio.sleep(1)

    test_value = await mcache.get(key)
    assert test_value is None

    test_value = await mcache.touch(b"not:" + key, 1)
    assert not test_value

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        fut: asyncio.Future[bytes] = asyncio.Future()
        fut.set_result(b'SERVER_ERROR error\r\n')
        patched.return_value = fut

        with pytest.raises(ClientException):
            await mcache.touch(b"not:" + key, 1)


@pytest.mark.asyncio
async def test_close(mcache):
    await mcache.close()
    assert mcache._pool.size() == 0
