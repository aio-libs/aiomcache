import asyncio
import pytest
from unittest import mock

from aiomcache.exceptions import ClientException, ValidationException


@pytest.mark.run_loop
def test_version(mcache, loop):
    version = yield from mcache.version()
    stats = yield from mcache.stats()
    assert version == stats[b'version']

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        with pytest.raises(ClientException):
            fut = asyncio.Future(loop=loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut
            yield from mcache.version()


@pytest.mark.run_loop
def test_flush_all(mcache, loop):
    key, value = b'key:flush_all', b'flush_all_value'
    yield from mcache.set(key, value)
    # make sure value exists
    test_value = yield from mcache.get(key)
    assert test_value == value
    # flush data
    yield from mcache.flush_all()
    # make sure value does not exists
    test_value = yield from mcache.get(key)
    assert test_value is None

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        with pytest.raises(ClientException):
            fut = asyncio.Future(loop=loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut
            yield from mcache.flush_all()


@pytest.mark.run_loop
def test_set_get(mcache, loop):
    key, value = b'key:set', b'1'
    yield from mcache.set(key, value)
    test_value = yield from mcache.get(key)
    assert test_value == value
    test_value = yield from mcache.get(b'not:' + key)
    assert test_value is None

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        with pytest.raises(ClientException):
            fut = asyncio.Future(loop=loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut
            yield from mcache.set(key, value)


@pytest.mark.run_loop
def test_multi_get(mcache):
    key1, value1 = b'key:multi_get:1', b'1'
    key2, value2 = b'key:multi_get:2', b'2'
    yield from mcache.set(key1, value1)
    yield from mcache.set(key2, value2)
    test_value = yield from mcache.multi_get(key1, key2)
    assert test_value == [value1, value2]

    test_value = yield from mcache.multi_get(b'not' + key1, key2)
    assert test_value == [None, value2]
    test_value = yield from mcache.multi_get()
    assert test_value == []


@pytest.mark.run_loop
def test_multi_get_doubling_keys(mcache):
    key, value = b'key:multi_get:3', b'1'
    yield from mcache.set(key, value)

    with pytest.raises(ClientException):
        test_value = yield from mcache.multi_get(key, key)
        assert test_value == []


@pytest.mark.run_loop
def test_set_expire(mcache, loop):
    key, value = b'key:set', b'1'
    yield from mcache.set(key, value, exptime=1)
    test_value = yield from mcache.get(key)
    assert test_value == value

    yield from asyncio.sleep(1, loop=loop)

    test_value = yield from mcache.get(key)
    assert test_value is None


@pytest.mark.run_loop
def test_set_errors(mcache):
    key, value = b'key:set', b'1'
    yield from mcache.set(key, value, exptime=1)

    with pytest.raises(ValidationException):
        yield from mcache.set(key, value, exptime=-1)

    with pytest.raises(ValidationException):
        yield from mcache.set(key, value, exptime=3.14)


@pytest.mark.run_loop
def test_add(mcache):
    key, value = b'key:add', b'1'
    yield from mcache.set(key, value)

    test_value = yield from mcache.add(key, b'2')
    assert not test_value

    test_value = yield from mcache.add(b'not:' + key, b'2')
    assert test_value

    test_value = yield from mcache.get(b'not:' + key)
    assert test_value == b'2'


@pytest.mark.run_loop
def test_replace(mcache):
    key, value = b'key:replace', b'1'
    yield from mcache.set(key, value)

    test_value = yield from mcache.replace(key, b'2')
    assert test_value
    # make sure value exists
    test_value = yield from mcache.get(key)
    assert test_value == b'2'

    test_value = yield from mcache.replace(b'not:' + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = yield from mcache.get(b'not:' + key)
    assert test_value is None


@pytest.mark.run_loop
def test_append(mcache):
    key, value = b'key:append', b'1'
    yield from mcache.set(key, value)

    test_value = yield from mcache.append(key, b'2')
    assert test_value

    # make sure value exists
    test_value = yield from mcache.get(key)
    assert test_value == b'12'

    test_value = yield from mcache.append(b'not:' + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = yield from mcache.get(b'not:' + key)
    assert test_value is None


@pytest.mark.run_loop
def test_prepend(mcache):
    key, value = b'key:prepend', b'1'
    yield from mcache.set(key, value)

    test_value = yield from mcache.prepend(key, b'2')
    assert test_value

    # make sure value exists
    test_value = yield from mcache.get(key)
    assert test_value == b'21'

    test_value = yield from mcache.prepend(b'not:' + key, b'3')
    assert not test_value
    # make sure value exists
    test_value = yield from mcache.get(b'not:' + key)
    assert test_value is None


@pytest.mark.run_loop
def test_delete(mcache, loop):
    key, value = b'key:delete', b'value'
    yield from mcache.set(key, value)

    # make sure value exists
    test_value = yield from mcache.get(key)
    assert test_value == value

    is_deleted = yield from mcache.delete(key)
    assert is_deleted
    # make sure value does not exists
    test_value = yield from mcache.get(key)
    assert test_value is None

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        with pytest.raises(ClientException):
            fut = asyncio.Future(loop=loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut

            yield from mcache.delete(key)


@pytest.mark.run_loop
def test_delete_key_not_exists(mcache):
    is_deleted = yield from mcache.delete(b'not:key')
    assert not is_deleted


@pytest.mark.run_loop
def test_incr(mcache):
    key, value = b'key:incr:1', b'1'
    yield from mcache.set(key, value)

    test_value = yield from mcache.incr(key, 2)
    assert test_value == 3

    # make sure value exists
    test_value = yield from mcache.get(key)
    assert test_value == b'3'


@pytest.mark.run_loop
def test_incr_errors(mcache):
    key, value = b'key:incr:2', b'string'
    yield from mcache.set(key, value)

    with pytest.raises(ClientException):
        yield from mcache.incr(key, 2)

    with pytest.raises(ClientException):
        yield from mcache.incr(key, 3.14)


@pytest.mark.run_loop
def test_decr(mcache):
    key, value = b'key:decr:1', b'17'
    yield from mcache.set(key, value)

    test_value = yield from mcache.decr(key, 2)
    assert test_value == 15

    test_value = yield from mcache.get(key)
    assert test_value == b'15'

    test_value = yield from mcache.decr(key, 1000)
    assert test_value == 0


@pytest.mark.run_loop
def test_decr_errors(mcache):
    key, value = b'key:decr:2', b'string'
    yield from mcache.set(key, value)

    with pytest.raises(ClientException):
        yield from mcache.decr(key, 2)

    with pytest.raises(ClientException):
        yield from mcache.decr(key, 3.14)


@pytest.mark.run_loop
def test_stats(mcache):
    stats = yield from mcache.stats()
    assert b'pid' in stats


@pytest.mark.run_loop
def test_touch(mcache, loop):
    key, value = b'key:touch:1', b'17'
    yield from mcache.set(key, value)

    test_value = yield from mcache.touch(key, 1)
    assert test_value

    test_value = yield from mcache.get(key)
    assert test_value == value

    yield from asyncio.sleep(1, loop=loop)

    test_value = yield from mcache.get(key)
    assert test_value is None

    test_value = yield from mcache.touch(b'not:' + key, 1)
    assert not test_value

    with mock.patch.object(mcache, '_execute_simple_command') as patched:
        with pytest.raises(ClientException):
            fut = asyncio.Future(loop=loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut

            yield from mcache.touch(b'not:' + key, 1)


@pytest.mark.run_loop
def test_close(mcache):
    yield from mcache.close()
    assert mcache._pool.size() == 0
