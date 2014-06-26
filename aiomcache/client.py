"""memcached client."""

__all__ = ['Client']

import asyncio
import functools
import re
import aiomcache.constants as const
from .pool import MemcachePool
from .exceptions import ClientException, ValidationException


def acquire(func):

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        conn = yield from self._pool.acquire()
        try:
            return (yield from func(self, conn[0], conn[1], *args, **kwargs))
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
        if not isinstance(key, bytes): # avoid bugs subtle and otherwise
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

    def close(self):
        """Closes the socket if its open."""
        self._pool.clear()

    @acquire
    def delete(self, reader, writer, key):
        """Deletes a key/value pair from the server.
        :param key: is the key of the item the client wishes
        the server to delete.
        :return: True if case values was deleted or False to indicate
        that the item with this key was not found.
        """
        assert self._validate_key(key)

        command = b'delete ' + key + b'\r\n'
        response = yield from self._execute_simple_command(
            reader, writer, command)

        if response not in (const.DELETED, const.NOT_FOUND):
            raise ClientException('Memcached delete failed', response)
        return response == const.DELETED

    @acquire
    def get(self, reader, writer, key, default=None):
        """Gets a single value from the server.
        Returns default value if there is no value.
        """
        result = yield from self._multi_get(reader, writer, key)
        if result:
            return result[0]
        else:
            return default

    @acquire
    def multi_get(self, reader, writer, *keys):
        """Takes a list of keys and returns a list of values.

        Raises ``ValidationException``, ``ClientException``, and socket errors
        """
        return (yield from self._multi_get(reader, writer, *keys))

    @asyncio.coroutine
    def _multi_get(self, reader, writer, *keys):
        # req  - get <key> [<key> ...]\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        if not keys:
            return []

        [self._validate_key(key) for key in keys]
        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')

        writer.write(b'get ' + b' '.join(keys) + b'\r\n')

        received = {}
        line = yield from reader.readline()

        while line != b'END\r\n':
            terms = line.split()

            if len(terms) == 4 and terms[0] == b'VALUE': # exists
                key = terms[1]
                flags = int(terms[2])
                length = int(terms[3])

                if flags != 0:
                    raise ClientException('received non zero flags')

                val = (yield from reader.readexactly(length+2))[:-2]
                if key in received:
                    raise ClientException('duplicate results from server')

                received[key] = val
            else:
                raise ClientException('get failed', line)

            line = yield from reader.readline()

        if len(received) > len(keys):
            raise ClientException('received too many responses')

        # memcache client is used by other servers besides memcached.
        # In the case of kestrel, responses coming back to not necessarily
        # match the requests going out. Thus we just ignore the key name
        # if there is only one key and return what we received.
        if len(keys) == 1 and len(received) == 1:
            response = list(received.values())
        else:
            response = [received.get(k) for k in keys if k in received]

        return response

    @acquire
    def stats(self, reader, writer, args=None):
        """Runs a stats command on the server."""
        # req  - stats [additional args]\r\n
        # resp - STAT <name> <value>\r\n (one per result)
        #        END\r\n
        if args is None:
            args = b''

        writer.write(b''.join((b'stats ', args, b'\r\n')))

        result = {}

        resp = yield from reader.readline()
        while resp != b'END\r\n':
            terms = resp.split()

            if len(terms) == 2 and terms[0] == b'STAT':
                result[terms[1]] = None
            elif len(terms) == 3 and terms[0] == b'STAT':
                result[terms[1]] = terms[2]
            else:
                raise ClientException('stats failed', resp)

            resp = yield from reader.readline()

        return result

    @asyncio.coroutine
    def _storage_command(self, reader, writer, command, key, value,
                         flags=0, exptime=0):
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
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
        _cmd = b' '.join([command, key] + args) + b'\r\n'
        cmd = _cmd + value + b'\r\n'
        resp = yield from self._execute_simple_command(reader, writer, cmd)

        if resp not in (const.STORED, const.NOT_STORED):
            raise ClientException('stats {} failed'.format(command), resp)
        return resp == const.STORED

    @acquire
    def set(self, reader, writer, key, value, exptime=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        """
        flags = 0 # TODO: fix when exception removed
        resp = yield from self._storage_command(
            reader, writer, b'set', key, value, flags, exptime)
        return resp

    @acquire
    def add(self, reader, writer, key, value, exptime=0):
        """Store this data, but only if the server *doesn't* already
        hold data for this key.

        :param key: ``bytes`` is the key of the item.
        :param value: data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        """
        flags = 0 # TODO: fix when exception removed
        return (yield from self._storage_command(
            reader, writer, b'add', key, value, flags, exptime))

    @acquire
    def replace(self, reader, writer, key, value, exptime=0):
        """Store this data, but only if the server *does*
        already hold data for this key.

        :param key: ``bytes`` is the key of the item.
        :param value: data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        """
        flags = 0 # TODO: fix when exception removed
        return (yield from self._storage_command(
            reader, writer, b'replace', key, value, flags, exptime))

    @acquire
    def append(self, reader, writer, key, value, exptime=0):
        """Add data to an existing key after existing data

        :param key: ``bytes`` is the key of the item.
        :param value: data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        """
        flags = 0 # TODO: fix when exception removed
        return (yield from self._storage_command(
            reader, writer, b'append', key, value, flags, exptime))

    @acquire
    def prepend(self, reader, writer, key, value, exptime=0):
        """Add data to an existing key before existing data

        :param key: ``bytes`` is the key of the item.
        :param value: data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        """
        flags = 0 # TODO: fix when exception removed
        return (yield from self._storage_command(
            reader, writer, b'prepend', key, value, flags, exptime))

    @asyncio.coroutine
    def _incr_decr(self, reader, writer, command, key, delta):
        delta_byte = str(delta).encode('utf-8')
        cmd = b' '.join([command, key, delta_byte]) + b'\r\n'
        resp = yield from self._execute_simple_command(reader, writer, cmd)
        if not resp.isdigit() or resp == const.NOT_FOUND:
            raise ClientException('Memcached flush_all failed', resp)
        return int(resp) if resp.isdigit() else None

    @acquire
    def incr(self, reader, writer, key, increment=1):
        """Command is used to change data for some item in-place,
        incrementing it. The data for the item is treated as decimal
        representation of a 64-bit unsigned integer.

        :param key: is the key of the item the client wishes to change
        :param increment: is the amount by which the client
        wants to increase the item. It is a decimal representation
        of a 64-bit unsigned integer.
        :return: ``int`` new value of the item's data,
        after the increment or ``None`` to indicate the item with
        this value was not found
        """
        assert self._validate_key(key)
        resp = yield from self._incr_decr(
            reader, writer, b'incr', key, increment)
        return resp

    @acquire
    def decr(self, reader, writer, key, decrement=1):
        """Command is used to change data for some item in-place,
        decrementing it. The data for the item is treated as decimal
        representation of a 64-bit unsigned integer.

        :param key: is the key of the item the client wishes to change
        :param decrement: is the amount by which the client
        wants to decrease the item. It is a decimal representation
        of a 64-bit unsigned integer.
        :return: ``int`` new value of the item's data,
        after the increment or ``None`` to indicate the item with
        this value was not found
        """
        assert self._validate_key(key)
        resp = yield from self._incr_decr(
            reader, writer, b'decr', key, decrement)
        return resp

    @acquire
    def touch(self, reader, writer, key, exptime):
        """The "touch" command is used to update the expiration time of
        an existing item without fetching it.

        :param key: is the key to update expiration time
        :param exptime: is expiration time. Works the same as with the
            update commands (set/add/etc). This replaces the existing
            expiration time.

        NOTE: ``reader``, ``writer`` added implicitly.
        """
        assert self._validate_key(key)

        _cmd = 'touch {} {}\r\n'.format(key, exptime).encode('utf-8')
        resp = yield from self._execute_simple_command(reader, writer, _cmd)
        if resp not in (const.TOUCHED, const.NOT_FOUND):
            raise ClientException('Memcached touch failed', resp)
        return resp == const.TOUCHED

    @asyncio.coroutine
    def _execute_simple_command(self, reader, writer, raw_command):
        response, line = bytearray(), b''

        writer.write(raw_command)
        yield from writer.drain()

        while not line.endswith(b'\r\n'):
            line = yield from reader.readline()
            response.extend(line)
        return response[:-2]

    @acquire
    def version(self, reader, writer):
        """Current version of the server.

        :return: version string for the server."""

        command = b'version\r\n'
        response = yield from self._execute_simple_command(
            reader, writer, command)
        version, number = response.split()
        if const.VERSION != version:
            raise ClientException('Memcached version failed', response)
        return number

    @acquire
    def quit(self, reader, writer):
        """Upon receiving this command, the server closes the
        connection. However, the client may also simply close the connection
        when it no longer needs it, without issuing this command."""
        raise NotImplementedError
        yield from writer('quit\r\n')
        self.close()

    @acquire
    def flush_all(self, reader, writer):
        """This is a command with an optional numeric argument. It always
        succeeds, and the server sends "OK\r\n" in response (unless "noreply"
        is given as the last parameter). Its effect is to invalidate all
        existing items immediately (by default) or after the expiration
        specified."""

        command = b'flush_all\r\n'
        response = yield from self._execute_simple_command(
            reader, writer, command)

        if const.OK != response:
            raise ClientException('Memcached flush_all failed', response)

    @acquire
    def verbosity(self, reader, writer, level):
        """This is a command with a numeric argument. It always succeeds,
        and the server sends "OK" in response. Its effect is to set the
        verbosity level of the logging output.

        :param level: ``int`` log level
        """
        assert isinstance(level, int), "Log level must be *int* vlaue"

        command = 'verbosity {}\r\n'.format(level).encode('utf-8')
        response = yield from self._execute_simple_command(
            reader, writer, command)

        if const.OK != response:
            raise ClientException('Memcached verbosity failed', response)
