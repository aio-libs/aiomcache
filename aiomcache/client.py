import asyncio
import functools
import re

from . import constants as const
from .pool import MemcachePool
from .exceptions import ClientException, ValidationException


__all__ = ['Client']


def acquire(func):

    @asyncio.coroutine
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        conn = yield from self._pool.acquire()
        try:
            return (yield from func(self, conn, *args, **kwargs))
        except Exception as exc:
            conn[0].set_exception(exc)
            raise
        finally:
            self._pool.release(conn)

    return wrapper


class Client(object):

    def __init__(self, host, port=11211, *,
                 pool_size=2, pool_minsize=None, loop=None):
        if not pool_minsize:
            pool_minsize = pool_size
        self._pool = MemcachePool(
            host, port, minsize=pool_minsize, maxsize=pool_size, loop=loop)

    # key supports ascii sans space and control chars
    # \x21 is !, right after space, and \x7e is -, right before DEL
    # also 1 <= len <= 250 as per the spec
    _valid_key_re = re.compile(b'^[\x21-\x7e]{1,250}$')

    def _validate_key(self, key):
        if not isinstance(key, bytes):  # avoid bugs subtle and otherwise
            raise ValidationException('key must be bytes', key)

        m = self._valid_key_re.match(key)
        if m:
            # in python re, $ matches either end of line or right before
            # \n at end of line. We can't allow latter case, so
            # making sure length matches is simplest way to detect
            if len(m.group(0)) != len(key):
                raise ValidationException('trailing newline', key)
        else:
            raise ValidationException('invalid key', key)

        return key

    @asyncio.coroutine
    def _execute_simple_command(self, conn, raw_command):
        response, line = bytearray(), b''

        conn.writer.write(raw_command)
        yield from conn.writer.drain()

        while not line.endswith(b'\r\n'):
            line = yield from conn.reader.readline()
            response.extend(line)
        return response[:-2]

    @asyncio.coroutine
    def close(self):
        """Closes the sockets if its open."""
        yield from self._pool.clear()

    @asyncio.coroutine
    def _multi_get(self, conn, *keys, with_cas=True):
        # req  - get <key> [<key> ...]\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        if not keys:
            return {}, {}

        [self._validate_key(key) for key in keys]
        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')

        cmd = b'gets ' if with_cas else b'get '
        conn.writer.write(cmd + b' '.join(keys) + b'\r\n')

        received = {}
        cas_tokens = {}
        line = yield from conn.reader.readline()

        while line != b'END\r\n':
            terms = line.split()

            if terms[0] == b'VALUE':  # exists
                key = terms[1]
                flags = int(terms[2])
                length = int(terms[3])

                if flags != 0:
                    raise ClientException('received non zero flags')

                val = (yield from conn.reader.readexactly(length+2))[:-2]
                if key in received:
                    raise ClientException('duplicate results from server')

                received[key] = val
                cas_tokens[key] = int(terms[4]) if with_cas else None
            else:
                raise ClientException('get failed', line)

            line = yield from conn.reader.readline()

        if len(received) > len(keys):
            raise ClientException('received too many responses')
        return received, cas_tokens

    @acquire
    def delete(self, conn, key):
        """Deletes a key/value pair from the server.

        :param key: is the key to delete.
        :return: True if case values was deleted or False to indicate
        that the item with this key was not found.
        """
        assert self._validate_key(key)

        command = b'delete ' + key + b'\r\n'
        response = yield from self._execute_simple_command(conn, command)

        if response not in (const.DELETED, const.NOT_FOUND):
            raise ClientException('Memcached delete failed', response)
        return response == const.DELETED

    @acquire
    def get(self, conn, key, default=None):
        """Gets a single value from the server.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, is the data for this specified key.
        """
        values, _ = yield from self._multi_get(conn, key)
        return values.get(key, default)

    @acquire
    def gets(self, conn, key, default=None):
        """Gets a single value from the server together with the cas token.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, ``bytes tuple with the value and the cas
        """
        values, cas_tokens = yield from self._multi_get(
            conn, key, with_cas=True)
        return values.get(key, default), cas_tokens.get(key)

    @acquire
    def multi_get(self, conn, *keys):
        """Takes a list of keys and returns a list of values.

        :param keys: ``list`` keys for the item being fetched.
        :return: ``list`` of values for the specified keys.
        :raises:``ValidationException``, ``ClientException``,
        and socket errors
        """
        values, _ = yield from self._multi_get(conn, *keys)
        return tuple(values.get(key) for key in keys)

    @acquire
    def stats(self, conn, args=None):
        """Runs a stats command on the server."""
        # req  - stats [additional args]\r\n
        # resp - STAT <name> <value>\r\n (one per result)
        #        END\r\n
        if args is None:
            args = b''

        conn.writer.write(b''.join((b'stats ', args, b'\r\n')))

        result = {}

        resp = yield from conn.reader.readline()
        while resp != b'END\r\n':
            terms = resp.split()

            if len(terms) == 2 and terms[0] == b'STAT':
                result[terms[1]] = None
            elif len(terms) == 3 and terms[0] == b'STAT':
                result[terms[1]] = terms[2]
            elif len(terms) >= 3 and terms[0] == b'STAT':
                result[terms[1]] = b' '.join(terms[2:])
            else:
                raise ClientException('stats failed', resp)

            resp = yield from conn.reader.readline()

        return result

    @asyncio.coroutine
    def _storage_command(self, conn, command, key, value,
                         flags=0, exptime=0, cas=None):
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)
        # req  - set <key> <flags> <exptime> <bytes> <cas> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)

        # typically, if val is > 1024**2 bytes server returns:
        #   SERVER_ERROR object too large for cache\r\n
        # however custom-compiled memcached can have different limit
        # so, we'll let the server decide what's too much
        assert self._validate_key(key)

        if not isinstance(exptime, int):
            raise ValidationException('exptime not int', exptime)
        elif exptime < 0:
            raise ValidationException('exptime negative', exptime)

        args = [str(a).encode('utf-8') for a in (flags, exptime, len(value))]
        _cmd = b' '.join([command, key] + args)
        if cas:
            _cmd += b' ' + str(cas).encode('utf-8')
        cmd = _cmd + b'\r\n' + value + b'\r\n'
        resp = yield from self._execute_simple_command(conn, cmd)

        if resp not in (
                const.STORED, const.NOT_STORED, const.EXISTS, const.NOT_FOUND):
            raise ClientException('stats {} failed'.format(command), resp)
        return resp == const.STORED

    @acquire
    def set(self, conn, key, value, exptime=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        flags = 0  # TODO: fix when exception removed
        resp = yield from self._storage_command(
            conn, b'set', key, value, flags, exptime)
        return resp

    @acquire
    def cas(self, conn, key, value, cas_token, exptime=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        only if value hasn't change from first retrieval

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is expiration time. If it's 0, the
        item never expires.
        :param cas_token: ``int``, unique cas token retrieve from previous
            ``gets``
        :return: ``bool``, True in case of success.
        """
        flags = 0  # TODO: fix when exception removed
        resp = yield from self._storage_command(
            conn, b'cas', key, value, flags, exptime, cas=cas_token)
        return resp

    @acquire
    def add(self, conn, key, value, exptime=0):
        """Store this data, but only if the server *doesn't* already
        hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        flags = 0  # TODO: fix when exception removed
        return (yield from self._storage_command(
            conn, b'add', key, value, flags, exptime))

    @acquire
    def replace(self, conn, key, value, exptime=0):
        """Store this data, but only if the server *does*
        already hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        flags = 0  # TODO: fix when exception removed
        return (yield from self._storage_command(
            conn, b'replace', key, value, flags, exptime))

    @acquire
    def append(self, conn, key, value, exptime=0):
        """Add data to an existing key after existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        flags = 0  # TODO: fix when exception removed
        return (yield from self._storage_command(
            conn, b'append', key, value, flags, exptime))

    @acquire
    def prepend(self, conn, key, value, exptime=0):
        """Add data to an existing key before existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        flags = 0  # TODO: fix when exception removed
        return (yield from self._storage_command(
            conn, b'prepend', key, value, flags, exptime))

    @asyncio.coroutine
    def _incr_decr(self, conn, command, key, delta):
        delta_byte = str(delta).encode('utf-8')
        cmd = b' '.join([command, key, delta_byte]) + b'\r\n'
        resp = yield from self._execute_simple_command(conn, cmd)
        if not resp.isdigit() or resp == const.NOT_FOUND:
            raise ClientException(
                'Memcached {} command failed'.format(str(command)), resp)
        return int(resp) if resp.isdigit() else None

    @acquire
    def incr(self, conn, key, increment=1):
        """Command is used to change data for some item in-place,
        incrementing it. The data for the item is treated as decimal
        representation of a 64-bit unsigned integer.

        :param key: ``bytes``, is the key of the item the client wishes
        to change
        :param increment: ``int``, is the amount by which the client
        wants to increase the item.
        :return: ``int``, new value of the item's data,
        after the increment or ``None`` to indicate the item with
        this value was not found
        """
        assert self._validate_key(key)
        resp = yield from self._incr_decr(
            conn, b'incr', key, increment)
        return resp

    @acquire
    def decr(self, conn, key, decrement=1):
        """Command is used to change data for some item in-place,
        decrementing it. The data for the item is treated as decimal
        representation of a 64-bit unsigned integer.

        :param key: ``bytes``, is the key of the item the client wishes
        to change
        :param decrement: ``int``, is the amount by which the client
        wants to decrease the item.
        :return: ``int`` new value of the item's data,
        after the increment or ``None`` to indicate the item with
        this value was not found
        """
        assert self._validate_key(key)
        resp = yield from self._incr_decr(
            conn, b'decr', key, decrement)
        return resp

    @acquire
    def touch(self, conn, key, exptime):
        """The command is used to update the expiration time of
        an existing item without fetching it.

        :param key: ``bytes``, is the key to update expiration time
        :param exptime: ``int``, is expiration time. This replaces the existing
        expiration time.
        :return: ``bool``, True in case of success.
        """
        assert self._validate_key(key)

        _cmd = b' '.join([b'touch', key, str(exptime).encode('utf-8')])
        cmd = _cmd + b'\r\n'
        resp = yield from self._execute_simple_command(conn, cmd)
        if resp not in (const.TOUCHED, const.NOT_FOUND):
            raise ClientException('Memcached touch failed', resp)
        return resp == const.TOUCHED

    @acquire
    def version(self, conn):
        """Current version of the server.

        :return: ``bytes``, memcached version for current the server.
        """

        command = b'version\r\n'
        response = yield from self._execute_simple_command(
            conn, command)
        if not response.startswith(const.VERSION):
            raise ClientException('Memcached version failed', response)
        version, number = response.split()
        return number

    @acquire
    def flush_all(self, conn):
        """Its effect is to invalidate all existing items immediately"""
        command = b'flush_all\r\n'
        response = yield from self._execute_simple_command(
            conn, command)

        if const.OK != response:
            raise ClientException('Memcached flush_all failed', response)
