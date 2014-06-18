"""memcached client."""

__all__ = ['Client']

import asyncio
import functools
import re
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
        """Deletes a key/value pair from the server."""
        # req  - delete <key> [noreply]\r\n
        # resp - DELETED\r\n
        #        or
        #        NOT_FOUND\r\n
        assert self._validate_key(key)

        writer.write(b'delete ' + key + b'\r\n')

        line = b''
        resp = bytearray()

        while not line.endswith(b'\r\n'):
            line = yield from reader.readline()
            resp.extend(line)

        if resp not in (b'DELETED\r\n', b'NOT_FOUND\r\n'):
            raise ClientException('Memcached delete failed', resp)

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

        error = None
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
            response = [received.get(key) for key in keys if key in received]

        return response

    @acquire
    def set(self, reader, writer,  key, val, exptime=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        """
        assert self._validate_key(key)
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)

        # typically, if val is > 1024**2 bytes server returns:
        #   SERVER_ERROR object too large for cache\r\n
        # however custom-compiled memcached can have different limit
        # so, we'll let the server decide what's too much

        if not isinstance(exptime, int):
            raise ValidationException('exptime not int', exptime)
        elif exptime < 0:
            raise ValidationException('exptime negative', exptime)

        writer.write(b''.join((b'set ', key, b' 0 ',
                               ('%d %d' % (exptime, len(val))).encode('utf-8'),
                               b'\r\n', val, b'\r\n')))

        resp = yield from reader.readline()
        if resp != b'STORED\r\n':
            raise ClientException('set failed', resp)

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
