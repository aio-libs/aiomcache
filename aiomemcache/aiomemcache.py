"""memcached client, based on mixpanel memcache_clien library

Usage example::

    import memcache
    mc = memcache.Client("127.0.0.1", 11211, timeout=1, connect_timeout=5)
    mc.set("some_key", "Some value")
    value = mc.get("some_key")
    mc.delete("another_key")
"""
__all__ = ['Client', 'ClientException', 'ValidationException']

import asyncio
import re


class ClientException(Exception):
    """Raised when the server does something we don't expect."""

    def __init__(self, msg, item=None):
        if item is not None:
            msg = '%s: %r' % (msg, item)
        super().__init__(msg)


class ValidationException(ClientException):
    """Raised when an invalid parameter is passed to a ``Client`` function."""


class Client(object):

    def __init__(self, host, port=11211, connect_timeout=None, loop=None):
        self._host = host
        self._port = port
        self._timeout = connect_timeout
        self._loop = loop if loop else asyncio.get_event_loop()
        self._protocol = None

    def __del__(self):
        self.close()

    def _get_connection(self):
        if self._protocol and self._protocol.is_connected:
            return self._protocol

        _, self._protocol = (
            yield from self._loop.create_connection(
                MemcacheProtocol, self._host, self._port))

        return self._protocol

    def _send_command(self, command):
        """Send command to server and return initial response line
        Will reopen socket if it got closed (either locally or by server)"""
        protocol = yield from self._get_connection()
        protocol.send(command)
        return (yield from protocol.stream.readline())

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
        if self._protocol and self._protocol.is_connected:
            self._protocol.close()
            self._protocol = None

    @asyncio.coroutine
    def delete(self, key):
        """
        Deletes a key/value pair from the server

        Raises ``ClientException`` and socket errors
        """
        # req  - delete <key> [noreply]\r\n
        # resp - DELETED\r\n
        #        or
        #        NOT_FOUND\r\n
        self._validate_key(key)

        command = b'delete ' + key + b'\r\n'

        resp = yield from self._send_command(command)
        if resp != b'DELETED\r\n' and resp != b'NOT_FOUND\r\n':
            raise ClientException('delete failed', resp)

    @asyncio.coroutine
    def get(self, key):
        """
        Gets a single value from the server; returns None if there is no value

        Raises ``ValidationException``, ``ClientException``, and socket errors
        """
        return (yield from self.multi_get(key))[0]

    @asyncio.coroutine
    def multi_get(self, *keys):
        """
        Takes a list of keys and returns a list of values

        Raises ``ValidationException``, ``ClientException``, and socket errors
        """
        if not keys:
            return []

        # req  - get <key> [<key> ...]\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        [self._validate_key(key) for key in keys]
        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')

        command = b'get ' + b' '.join(keys) + b'\r\n'
        received = {}

        resp = yield from self._send_command(command)
        stream = self._protocol.stream

        error = None
        while resp != b'END\r\n':
            terms = resp.split()

            if len(terms) == 4 and terms[0] == b'VALUE': # exists
                key = terms[1]
                flags = int(terms[2])
                length = int(terms[3])

                if flags != 0:
                    error = ClientException('received non zero flags')

                val = (yield from stream.readexactly(length+2))[:-2]
                if key in received:
                    error = ClientException('duplicate results from server')

                received[key] = val
            else:
                raise ClientException('get failed', resp)

            resp = yield from stream.readline()

        if error is not None:
            # this can happen if a memcached instance contains items
            # set by a previous client leads to subtle bugs, so fail fast
            raise error

        if len(received) > len(keys):
            raise ClientException('received too many responses')

        # memcache client is used by other servers besides memcached.
        # In the case of kestrel, responses coming back to not necessarily
        # match the requests going out. Thus we just ignore the key name
        # if there is only one key and return what we received.
        if len(keys) == 1 and len(received) == 1:
            response = list(received.values())
        else:
            response = [received.get(key) for key in keys]

        return response

    @asyncio.coroutine
    def set(self, key, val, exptime=0):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        """
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)
        self._validate_key(key)

        # typically, if val is > 1024**2 bytes server returns:
        #   SERVER_ERROR object too large for cache\r\n
        # however custom-compiled memcached can have different limit
        # so, we'll let the server decide what's too much

        if not isinstance(exptime, int):
            raise ValidationException('exptime not int', exptime)
        elif exptime < 0:
            raise ValidationException('exptime negative', exptime)

        command = (b'set ' + key + b' 0 ' + (
            '%d %d\r\n' % (exptime, len(val))).encode('utf-8') + val + b'\r\n')

        resp = yield from self._send_command(command)
        if resp != b'STORED\r\n':
            raise ClientException('set failed', resp)

    @asyncio.coroutine
    def stats(self, args=None):
        """Runs a stats command on the server."""
        # req  - stats [additional args]\r\n
        # resp - STAT <name> <value>\r\n (one per result)
        #        END\r\n
        if args is not None:
            command = b'stats ' + args + b'\r\n'
        else:
            command = b'stats\r\n'

        result = {}

        resp = yield from self._send_command(command)
        while resp != b'END\r\n':
            terms = resp.split()

            if len(terms) == 2 and terms[0] == b'STAT':
                result[terms[1]] = None
            elif len(terms) == 3 and terms[0] == b'STAT':
                result[terms[1]] = terms[2]
            else:
                raise ClientException('stats failed', resp)

            resp = yield from self._protocol.stream.readline()

        return result


class MemcacheProtocol(asyncio.Protocol):

    transport = None
    is_connected = False

    def __init__(self):
        self.stream = asyncio.StreamReader()

    def close(self):
        if self.is_connected:
            self.transport.close()

    def connection_made(self, transport):
        self.is_connected = True
        self.transport = transport

    def connection_lost(self, exc):
        self.stream.feed_eof()
        self.is_connected = False
        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.stream.feed_data(data)

    def eof_received(self):
        self.stream.feed_eof()
        self.is_connected = False
        self.transport = None

    def send(self, payload):
        self.transport.write(payload)
