import asyncio
import enum
import functools
import re
import struct

from . import constants as const
from .pool import MemcachePool
from .exceptions import ClientException, ValidationException


__all__ = ['Client', 'BinaryClient']


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


class ClientBase(object):

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
    def close(self):
        """Closes the sockets if its open."""
        yield from self._pool.clear()

    @acquire
    def get(self, conn, key, default=None):
        """Gets a single value from the server.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, is the data for this specified key.
        """
        values, _, _ = yield from self._multi_get(conn, key)
        return values.get(key, default)

    @acquire
    def gets(self, conn, key, default=None):
        """Gets a single value from the server together with the cas token.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, ``bytes tuple with the value and the cas
        """
        values, cas_tokens, _ = yield from self._multi_get(
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
        values, _, _ = yield from self._multi_get(conn, *keys)
        return tuple(values.get(key) for key in keys)


class Client(ClientBase):

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
    def _multi_get(self, conn, *keys, with_cas=True):
        # req  - get <key> [<key> ...]\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        if not keys:
            return {}, {}, {}

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
        return received, cas_tokens, {}

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

        if response == const.DELETED:
            return True
        elif response == const.NOT_FOUND:
            return False

        raise ClientException('Memcached delete failed', response)

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
        response = yield from self._execute_simple_command(conn, command)
        if not response.startswith(const.VERSION):
            raise ClientException('Memcached version failed', response)
        version, number = response.split()
        return number

    @acquire
    def flush_all(self, conn):
        """Its effect is to invalidate all existing items immediately"""
        command = b'flush_all\r\n'
        response = yield from self._execute_simple_command(conn, command)

        if const.OK != response:
            raise ClientException('Memcached flush_all failed', response)


class BinaryClient(ClientBase):

    # binary protocol description:
    # https://github.com/memcached/memcached/blob/master/doc/protocol-binary.xml

    @staticmethod
    def _make_command(opcode, key=b'', data=b'', extra=b'', data_type=0,
                      status=0, opaque=0, cas_token=0):
        return struct.pack('>BBHBBHLLQ', 0x80, int(opcode), len(key),
                len(extra), data_type, status,
                len(extra) + len(key) + len(data), opaque, cas_token) \
                + extra + key + data

    @asyncio.coroutine
    def _send_command(self, conn, raw_command):
        conn.writer.write(raw_command)
        yield from conn.writer.drain()

    @asyncio.coroutine
    def _receive_response(self, conn):
        header = yield from conn.reader.read(24)
        opcode, key_len, extra_len, data_type, status, body_len, opaque, cas = \
            struct.unpack('>xBHBBHLLQ', header)

        if extra_len:
            extra = yield from conn.reader.read(extra_len)
        else:
            extra = b''

        if key_len:
            key = yield from conn.reader.read(key_len)
        else:
            key = b''

        data_len = body_len - key_len - extra_len
        if data_len:
            data = yield from conn.reader.read(data_len)
        else:
            data = b''

        return (opcode, key, data, extra, status, opaque, cas)

    @asyncio.coroutine
    def _execute_simple_command(self, conn, raw_command):
        # send a command and return the response status
        yield from self._send_command(conn, raw_command)
        return (yield from self._receive_response(conn))[4]

    @asyncio.coroutine
    def _multi_get(self, conn, *keys, with_cas=True, with_flags=True):
        if not keys:
            return {}, {}, {}

        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')
        [self._validate_key(key) for key in keys]

        # send GETQ for all keys except the last one
        # this allows the server to skip sending responses for missing keys
        commands = b''.join(self._make_command(const.Opcode.GETKQ, k)
                            for k in keys[:-1])
        yield from self._send_command(conn, commands)
        yield from self._send_command(conn, self._make_command(
            const.Opcode.GETK, keys[-1]))

        key_to_value = {}
        key_to_cas = {}
        key_to_flags = {}

        # we sent getk (not getkq) for the last key, so the server will always
        # send a response for that key, and it will be last - we can check for
        # that key to know when the server is done sending responses
        key = None
        while key != keys[-1]:
            opcode, key, data, extra, status, opaque, cas = \
                yield from self._receive_response(conn)

            if status == const.StatusCode.NO_ERROR:
                key_to_value[key] = data
                if with_flags:
                    key_to_flags[key] = struct.unpack('>L', extra)[0]
                if with_cas:
                    key_to_cas[key] = cas

            elif status == const.StatusCode.KEY_NOT_FOUND:
                continue  # must be the last key

            else:
                raise ClientException('get failed; key=%r, status=%r' % (
                    key, status))

        return (key_to_value, key_to_cas, key_to_flags)

    @acquire
    def delete(self, conn, key):
        """Deletes a key/value pair from the server.

        :param key: is the key to delete.
        :return: True if case values was deleted or False to indicate
        that the item with this key was not found.
        """
        assert self._validate_key(key)

        status = yield from self._execute_simple_command(conn,
            self._make_command(const.Opcode.DELETE, key))

        if status == const.StatusCode.NO_ERROR:
            return True
        if status == const.StatusCode.KEY_NOT_FOUND:
            return False
        raise ClientException('delete failed; key=%r, status=%r' % (
            key, status))

    @acquire
    def stats(self, conn, key=None):
        """Runs a stats command on the server."""
        if key:
            assert self._validate_key(key)
            yield from self._send_command(conn, self._make_command(
                const.Opcode.STAT, key))
        else:
            yield from self._send_command(conn, self._make_command(
                const.Opcode.STAT))

        # the server responds with multiple stat responses, ending with one
        # containing a blank key
        ret = {}
        while True:
            _, key, data, _, status, _, _ = yield from self._receive_response(conn)

            if status != const.StatusCode.NO_ERROR:
                raise ClientException('store (%d) failed; key=%r, status=%r' % (
                    opcode, key, status))

            if not key:
                return ret

            ret[key] = data

    @asyncio.coroutine
    def _execute_storage_command(self, conn, opcode, key, value, flags, exptime,
                                 cas_token=0):
        if key:
            assert self._validate_key(key)

        if exptime is not None:
            if not isinstance(exptime, int):
                raise ValidationException('exptime is not an integer', exptime)
            elif exptime < 0:
                raise ValidationException('exptime is negative', exptime)

        # append/prepend don't take extra data, but the others do
        if (flags is None) and (exptime is None):
            extra = b''
        elif flags is None:
            extra = struct.pack('>l', exptime)
        else:
            extra = struct.pack('>Ll', flags, exptime)
        command = self._make_command(opcode, key, value, extra,
            cas_token=cas_token)
        status = yield from self._execute_simple_command(conn, command)

        if status == const.StatusCode.NO_ERROR:
            return True
        if status in (const.StatusCode.NOT_STORED, const.StatusCode.KEY_EXISTS,
                      const.StatusCode.KEY_NOT_FOUND):
            return False

        raise ClientException('store (%d) failed; key=%r, status=%r' % (
            opcode, key, status))

    @acquire
    def set(self, conn, key, value, exptime=0, flags=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is expiration time. If it's 0, the
            item never expires.
        :param flags: ``int``, flags to store along with value.
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn, const.Opcode.SET,
            key, value, flags, exptime))

    @acquire
    def cas(self, conn, key, value, cas_token, exptime=0, flags=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        only if value hasn't change from first retrieval

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is expiration time. If it's 0, the
            item never expires.
        :param flags: ``int``, flags to store along with value.
        :param cas_token: ``int``, unique cas token retrieve from previous
            ``gets``
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn, const.Opcode.SET,
            key, value, flags, exptime, cas_token=cas_token))

    @acquire
    def add(self, conn, key, value, exptime=0, flags=0):
        """Store this data, but only if the server *doesn't* already
        hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
            item never expires.
        :param flags: ``int``, flags to store along with value.
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn, const.Opcode.ADD,
            key, value, flags, exptime))

    @acquire
    def replace(self, conn, key, value, exptime=0, flags=0):
        """Store this data, but only if the server *does*
        already hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
            item never expires.
        :param flags: ``int``, flags to store along with value.
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn,
            const.Opcode.REPLACE, key, value, flags, exptime))

    @acquire
    def append(self, conn, key, value):
        """Add data to an existing key after existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn,
            const.Opcode.APPEND, key, value, None, None))

    @acquire
    def prepend(self, conn, key, value):
        """Add data to an existing key before existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn,
            const.Opcode.PREPEND, key, value, None, None))

    @asyncio.coroutine
    def _execute_incr_decr(self, conn, opcode, key, default, delta, exptime=0):
        assert self._validate_key(key)

        if not isinstance(delta, int):
            raise ValidationException('delta is not an integer', exptime)

        if default is None:
            extra = struct.pack('>QQl', delta, 0, -1)
        else:
            extra = struct.pack('>QQl', delta, default, exptime)
        command = self._make_command(opcode, key, extra=extra)
        yield from self._send_command(conn, command)
        _, _, data, _, status, _, _ = yield from self._receive_response(conn)

        if status == const.StatusCode.NO_ERROR:
            return struct.unpack('>q', data)[0]
        if status == const.StatusCode.NOT_STORED:
            return None

        raise ClientException('incr/decr (%d) failed; key=%r, status=%r' % (
            opcode, key, status))

    @acquire
    def incr(self, conn, key, increment=1, default=None):
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
        return (yield from self._execute_incr_decr(conn, const.Opcode.INCR, key,
            default, increment))

    @acquire
    def decr(self, conn, key, decrement=1, default=None):
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
        return (yield from self._execute_incr_decr(conn, const.Opcode.DECR, key,
            default, decrement))

    @acquire
    def touch(self, conn, key, exptime):
        """The command is used to update the expiration time of
        an existing item without fetching it.

        :param key: ``bytes``, is the key to update expiration time
        :param exptime: ``int``, is expiration time. This replaces the existing
        expiration time.
        :return: ``bool``, True in case of success.
        """
        return (yield from self._execute_storage_command(conn,
            const.Opcode.TOUCH, key, b'', None, exptime))

    @acquire
    def version(self, conn):
        """Current version of the server.

        :return: ``bytes``, memcached version for current the server.
        """
        yield from self._send_command(conn, self._make_command(
            const.Opcode.VERSION))
        _, _, data, _, status, _, _ = yield from self._receive_response(conn)

        # can this command ever fail? do we need to check the status at all?
        if status == const.StatusCode.NO_ERROR:
            return data

        raise ClientException('version failed; status=%r' % (status,))

    @acquire
    def flush_all(self, conn, exptime=0):
        return (yield from self._execute_storage_command(conn,
            const.Opcode.FLUSH, b'', b'', None, exptime))
