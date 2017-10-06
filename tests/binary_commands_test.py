import asyncio
import pytest
from unittest import mock

from aiomcache.exceptions import ClientException, ValidationException
from aiomcache.constants import StatusCode


class AllCommandsFail(object):
    def __init__(self, mcache_binary, loop):
        self.mcache_binary = mcache_binary
        self.loop = loop
        self.patch = mock.patch.object(mcache_binary, '_receive_response')

    def __enter__(self):
        self.patched = self.patch.__enter__()
        self.future = asyncio.Future(loop=self.loop)
        self.failure_result = (None, None, 'version', None, StatusCode.INVALID_ARGUMENTS, None, None)
        self.future.set_result(self.failure_result)
        self.patched.return_value = self.future

    def __exit__(self, t, v, tb):
        self.patch.__exit__(t, v, tb)


@pytest.mark.run_loop
def test_version(mcache_binary, loop):
    version = yield from mcache_binary.version()
    stats = yield from mcache_binary.stats()
    assert version == stats[b'version']

    with AllCommandsFail(mcache_binary, loop):
        with pytest.raises(ClientException):
            yield from mcache_binary.version()


@pytest.mark.run_loop
def test_flush_all(mcache_binary, loop):
    key, value = b'key:flush_all', b'flush_all_value'
    yield from mcache_binary.set(key, value)
    # make sure value exists
    test_value = yield from mcache_binary.get(key)
    assert test_value == value
    # flush data
    yield from mcache_binary.flush_all()
    # make sure value does not exists
    test_value = yield from mcache_binary.get(key)
    assert test_value is None

    with AllCommandsFail(mcache_binary, loop):
        with pytest.raises(ClientException):
            yield from mcache_binary.flush_all()


@pytest.mark.run_loop
def test_set_get(mcache_binary, loop):
    key, value = b'key:set', b'1'
    yield from mcache_binary.set(key, value)
    test_value = yield from mcache_binary.get(key)
    assert test_value == value
    test_value = yield from mcache_binary.get(b'not:' + key)
    assert test_value is None
    test_value = yield from mcache_binary.get(b'not:' + key, default=value)
    assert test_value == value

    with AllCommandsFail(mcache_binary, loop):
        with pytest.raises(ClientException):
            yield from mcache_binary.set(key, value)


@pytest.mark.run_loop
def test_gets(mcache_binary, loop):
    key, value = b'key:set', b'1'
    yield from mcache_binary.set(key, value)

    test_value, cas = yield from mcache_binary.gets(key)
    assert test_value == value
    assert isinstance(cas, int)

    test_value, cas = yield from mcache_binary.gets(b'not:' + key)
    assert test_value is None
    assert cas is None

    test_value, cas = yield from mcache_binary.gets(b'not:' + key, default=value)
    assert test_value == value
    assert cas is None


@pytest.mark.run_loop
def test_multi_get(mcache_binary):
    key1, value1 = b'key:multi_get:1', b'1'
    key2, value2 = b'key:multi_get:2', b'2'
    yield from mcache_binary.set(key1, value1)
    yield from mcache_binary.set(key2, value2)
    test_value = yield from mcache_binary.multi_get(key1, key2)
    assert test_value == (value1, value2)

    test_value = yield from mcache_binary.multi_get(b'not' + key1, key2)
    assert test_value == (None, value2)
    test_value = yield from mcache_binary.multi_get()
    assert test_value == ()


@pytest.mark.run_loop
def test_multi_get_doubling_keys(mcache_binary):
    key, value = b'key:multi_get:3', b'1'
    yield from mcache_binary.set(key, value)

    with pytest.raises(ClientException):
        yield from mcache_binary.multi_get(key, key)


@pytest.mark.run_loop
def test_set_expire(mcache_binary, loop):
    key, value = b'key:set', b'1'
    yield from mcache_binary.set(key, value, exptime=1)
    test_value = yield from mcache_binary.get(key)
    assert test_value == value

    yield from asyncio.sleep(1, loop=loop)

    test_value = yield from mcache_binary.get(key)
    assert test_value is None


@pytest.mark.run_loop
def test_set_flags(mcache_binary, loop):
    key, value, flags = b'key:set', b'1', 0x1030FF9C
    yield from mcache_binary.set(key, value, flags=flags)

    test_value, test_flags = yield from mcache_binary.get(key, with_flags=True)
    assert test_value == value
    assert test_flags == flags

    test_value, test_cas, test_flags = yield from mcache_binary.gets(
        key, with_flags=True)
    assert test_value == value
    assert test_flags == flags


@pytest.mark.run_loop
def test_set_errors(mcache_binary):
    key, value = b'key:set', b'1'
    yield from mcache_binary.set(key, value, exptime=1)

    with pytest.raises(ValidationException):
        yield from mcache_binary.set(key, value, exptime=-1)

    with pytest.raises(ValidationException):
        yield from mcache_binary.set(key, value, exptime=3.14)


@pytest.mark.run_loop
def test_gets_cas(mcache_binary, loop):
    key, value = b'key:set', b'1'
    yield from mcache_binary.set(key, value)

    test_value, cas = yield from mcache_binary.gets(key)

    stored = yield from mcache_binary.cas(key, value, cas)
    assert stored is True

    stored = yield from mcache_binary.cas(key, value, cas)
    assert stored is False


@pytest.mark.run_loop
def test_cas_missing(mcache_binary, loop):
    key, value = b'key:set', b'1'
    stored = yield from mcache_binary.cas(key, value, 123)
    assert stored is False


@pytest.mark.run_loop
def test_add(mcache_binary):
    key, value = b'key:add', b'1'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.add(key, b'2')
    assert not test_value

    test_value = yield from mcache_binary.add(b'not:' + key, b'2')
    assert test_value

    test_value = yield from mcache_binary.get(b'not:' + key)
    assert test_value == b'2'


@pytest.mark.run_loop
def test_replace(mcache_binary):
    key, value = b'key:replace', b'1'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.replace(key, b'2')
    assert test_value
    # make sure value exists
    test_value = yield from mcache_binary.get(key)
    assert test_value == b'2'

    test_value = yield from mcache_binary.replace(b'not:' + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = yield from mcache_binary.get(b'not:' + key)
    assert test_value is None


@pytest.mark.run_loop
def test_append(mcache_binary):
    key, value = b'key:append', b'1'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.append(key, b'2')
    assert test_value

    # make sure value exists
    test_value = yield from mcache_binary.get(key)
    assert test_value == b'12'

    test_value = yield from mcache_binary.append(b'not:' + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = yield from mcache_binary.get(b'not:' + key)
    assert test_value is None


@pytest.mark.run_loop
def test_prepend(mcache_binary):
    key, value = b'key:prepend', b'1'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.prepend(key, b'2')
    assert test_value

    # make sure value exists
    test_value = yield from mcache_binary.get(key)
    assert test_value == b'21'

    test_value = yield from mcache_binary.prepend(b'not:' + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = yield from mcache_binary.get(b'not:' + key)
    assert test_value is None


@pytest.mark.run_loop
def test_delete(mcache_binary, loop):
    key, value = b'key:delete', b'value'
    yield from mcache_binary.set(key, value)

    # make sure value exists
    test_value = yield from mcache_binary.get(key)
    assert test_value == value

    is_deleted = yield from mcache_binary.delete(key)
    assert is_deleted
    # make sure value does not exists
    test_value = yield from mcache_binary.get(key)
    assert test_value is None

    with AllCommandsFail(mcache_binary, loop):
        with pytest.raises(ClientException):
            yield from mcache_binary.delete(key)


@pytest.mark.run_loop
def test_delete_key_not_exists(mcache_binary):
    is_deleted = yield from mcache_binary.delete(b'not:key')
    assert not is_deleted


@pytest.mark.run_loop
def test_incr(mcache_binary):
    key, value = b'key:incr:1', b'1'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.incr(key, 2)
    assert test_value == 3

    # make sure value exists
    test_value = yield from mcache_binary.get(key)
    assert test_value == b'3'


@pytest.mark.run_loop
def test_incr_errors(mcache_binary):
    key, value = b'key:incr:2', b'string'
    yield from mcache_binary.set(key, value)

    with pytest.raises(ClientException):
        yield from mcache_binary.incr(key, 2)

    with pytest.raises(ValidationException):
        yield from mcache_binary.incr(key, 3.14)


@pytest.mark.run_loop
def test_decr(mcache_binary):
    key, value = b'key:decr:1', b'17'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.decr(key, 2)
    assert test_value == 15

    test_value = yield from mcache_binary.get(key)
    assert test_value == b'15'

    test_value = yield from mcache_binary.decr(key, 1000)
    assert test_value == 0


@pytest.mark.run_loop
def test_decr_errors(mcache_binary):
    key, value = b'key:decr:2', b'string'
    yield from mcache_binary.set(key, value)

    with pytest.raises(ClientException):
        yield from mcache_binary.decr(key, 2)

    with pytest.raises(ClientException):
        yield from mcache_binary.decr(key, 3.14)


@pytest.mark.run_loop
def test_stats(mcache_binary):
    stats = yield from mcache_binary.stats()
    assert b'pid' in stats


@pytest.mark.run_loop
def test_touch(mcache_binary, loop):
    key, value = b'key:touch:1', b'17'
    yield from mcache_binary.set(key, value)

    test_value = yield from mcache_binary.touch(key, 1)
    assert test_value

    test_value = yield from mcache_binary.get(key)
    assert test_value == value

    yield from asyncio.sleep(1, loop=loop)

    test_value = yield from mcache_binary.get(key)
    assert test_value is None

    test_value = yield from mcache_binary.touch(b'not:' + key, 1)
    assert not test_value

    with AllCommandsFail(mcache_binary, loop):
        with pytest.raises(ClientException):
            yield from mcache_binary.touch(b'not:' + key, 1)


@pytest.mark.run_loop
def test_close(mcache_binary):
    yield from mcache_binary.close()
    assert mcache_binary._pool.size() == 0
