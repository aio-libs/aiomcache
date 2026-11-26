import asyncio
import contextlib
import datetime
import functools
import re
import time
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from typing import Any, Generic, Literal, TypeVar, overload

from . import constants as const
from .exceptions import ClientException, ValidationException
from .pool import Connection, MemcachePool

__all__ = ['Client']

_T = TypeVar("_T")
_U = TypeVar("_U")
_R = TypeVar("_R")
_Result = tuple[dict[bytes, bytes | _T], dict[bytes, _U]]

_GetFlagHandler = Callable[[bytes, int], Awaitable[_T]]
_SetFlagHandler = Callable[[_T], Awaitable[tuple[bytes, int]]]
# Reads the reply to a request. It returns the exception for an error reply
# which it read in full, instead of raising it, as the connection is intact.
_ReplyReader = Callable[[asyncio.StreamReader], Awaitable[_R | ClientException]]


def _is_recoverable_error(line: bytes | bytearray, *, has_data: bool) -> bool:
    """Return True if line is an error reply that the next reply can be read after.

    memcached skips the data of an item which it rejects as too large. After
    another error, it would take the data sent with a command for a command.
    And an error of its own, such as a lack of memory, may come with the loss
    of replies which it had not sent yet, or before it closes the connection.
    """
    line = line.rstrip()
    if has_data:
        return line == b"SERVER_ERROR object too large for cache"
    return line == b"ERROR" or line.startswith(b"CLIENT_ERROR ")


async def _read_line(
    parse: Callable[[bytearray], _R], has_data: bool, reader: asyncio.StreamReader
) -> _R | ClientException:
    """Read a one-line reply, returning what parse makes of it."""
    response, line = bytearray(), b""
    while not line.endswith(b"\r\n"):
        line = await reader.readline()
        if not line:
            # readline() returns an empty string at EOF.
            raise ClientException(
                "Connection closed by the server before receiving a full response",
            )
        response.extend(line)
    del response[-2:]
    try:
        return parse(response)
    except ClientException as exc:
        if _is_recoverable_error(response, has_data=has_data):
            return exc
        raise


async def _read_values(
    n_keys: int, with_cas: bool, reader: asyncio.StreamReader
) -> dict[bytes, tuple[bytes, int, int | None]]:
    """Read the reply to get or gets, returning each key's value, flags and cas."""
    # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
    #        <data block>\r\n (if exists)
    #        [...]
    #        END\r\n
    items: dict[bytes, tuple[bytes, int, int | None]] = {}
    line = await reader.readline()

    while line != b"END\r\n":
        terms = line.split()

        if terms and terms[0] == b"VALUE":  # exists
            key = terms[1]
            flags = int(terms[2])
            length = int(terms[3])

            val_bytes = (await reader.readexactly(length+2))[:-2]
            if key in items:
                raise ClientException("duplicate results from server")

            items[key] = (val_bytes, flags, int(terms[4]) if with_cas else None)
        else:
            # Never a reply to read on from: memcached drops the replies which
            # it has not sent yet when a get fails.
            raise ClientException("get failed", line)

        line = await reader.readline()

    if len(items) > n_keys:
        raise ClientException("received too many responses")

    return items


async def _read_stats(
    reader: asyncio.StreamReader
) -> dict[bytes, bytes | None] | ClientException:
    """Read the reply to stats."""
    # resp - STAT <name> <value>\r\n (one per result)
    #        END\r\n
    result: dict[bytes, bytes | None] = {}

    resp = await reader.readline()
    while resp != b"END\r\n":
        terms = resp.split()

        if len(terms) == 2 and terms[0] == b"STAT":
            result[terms[1]] = None
        elif len(terms) == 3 and terms[0] == b"STAT":
            result[terms[1]] = terms[2]
        elif len(terms) >= 3 and terms[0] == b"STAT":
            result[terms[1]] = b" ".join(terms[2:])
        else:
            error = ClientException("stats failed", resp)
            if _is_recoverable_error(resp, has_data=False):
                return error
            raise error

        resp = await reader.readline()

    return result


class FlagClient(Generic[_T]):
    __slots__ = ("__weakref__", "_pool", "_get_flag_handler", "_set_flag_handler", "_timeout")

    def __init__(self, host: str, port: int, *,
                 pool_size: int = 2, pool_minsize: int | None = None,
                 conn_args: Mapping[str, Any] | None = None,
                 get_flag_handler: _GetFlagHandler[_T] | None = None,
                 set_flag_handler: _SetFlagHandler[_T] | None = None,
                 timeout: float | None = None,
                 conn_timeout: float | None = None):
        """
        Creates new Client instance.

        :param host: memcached host
        :param port: memcached port (-1 implies local unix socket)
        :param pool_size: max connection pool size
        :param pool_minsize: min connection pool size
        :param conn_args: extra arguments passed to
            asyncio.open_connection(). For details, see:
            https://docs.python.org/3/library/asyncio-stream.html#asyncio.open_connection.
        :param get_flag_handler: async method to call to convert flagged
            values. Method takes tuple: (value, flags) and should return
            processed value or raise ClientException if not supported.
        :param set_flag_handler: async method to call to convert non bytes
            value to flagged value. Method takes value and must return tuple:
            (value, flags).
        :param timeout: max seconds for a command, including waiting for a
            pool connection. Raises TimeoutError.
        :param conn_timeout: max seconds to open a new connection.
        """
        if not pool_minsize:
            pool_minsize = pool_size

        self._pool = MemcachePool(
            host, port, minsize=pool_minsize, maxsize=pool_size,
            conn_args=conn_args, conn_timeout=conn_timeout)
        self._timeout = timeout

        self._get_flag_handler = get_flag_handler
        self._set_flag_handler = set_flag_handler

    # key may be anything except whitespace and control chars.
    # Must be str for unicode-aware regex.
    _valid_key_re = re.compile(r"^[^\s\x00-\x1F\x7F-\x9F]+$")

    def _validate_key(self, key: bytes) -> bytes:
        if not isinstance(key, bytes):  # avoid bugs subtle and otherwise
            raise ValidationException('key must be bytes', key)

        # memcached limits the key's length in bytes, not characters.
        if len(key) > const.MAX_KEY_LENGTH:
            raise ValidationException(
                f'key too long: {len(key)} bytes, max {const.MAX_KEY_LENGTH}', key)

        # Must decode to str for unicode-aware comparison.
        try:
            key_str = key.decode()
        except UnicodeDecodeError:
            raise ValidationException('invalid key', key)

        m = self._valid_key_re.match(key_str)
        if m:
            # in python re, $ matches either end of line or right before
            # \n at end of line. We can't allow latter case, so
            # making sure length matches is simplest way to detect
            if len(m.group(0)) != len(key_str):
                raise ValidationException('trailing newline', key)
        else:
            raise ValidationException('invalid key', key)

        return key

    def _validate_exptime(self, exptime: int | datetime.datetime) -> int:
        """Return exptime as memcached expects it."""
        if isinstance(exptime, datetime.datetime):
            if exptime.utcoffset() is None:
                raise ValidationException("exptime datetime must be timezone-aware", exptime)
            # A timestamp less than this distance from the epoch would be treated as
            # relative by memcached.
            timestamp = max(int(exptime.timestamp()), const.MAX_RELATIVE_EXPTIME + 1)
        elif isinstance(exptime, bool) or not isinstance(exptime, int):  # bool is an int
            raise ValidationException("exptime must be int or datetime", exptime)
        elif exptime < 0:
            raise ValidationException("exptime negative", exptime)
        elif exptime <= const.MAX_RELATIVE_EXPTIME:
            return exptime
        else:
            now = int(time.time())
            timestamp = now + exptime
            if exptime <= const.MAX_EXPTIME < timestamp:
                # Too long to be seconds from now, so it must be a timestamp itself.
                if exptime < now:
                    raise ValidationException(
                        "exptime too large to be seconds from now, and in the past as a"
                        " Unix timestamp", exptime)
                warnings.warn(
                    "exptime is too large to be seconds from now, so is sent as a Unix"
                    " timestamp. This is deprecated: pass a timezone-aware datetime instead.",
                    DeprecationWarning,
                    stacklevel=3,  # The caller of the command.
                )
                timestamp = exptime

        if timestamp > const.MAX_EXPTIME:
            raise ValidationException(
                "exptime after 2038-01-19 03:14:07 UTC, the latest memcached supports",
                exptime)
        return timestamp

    @contextlib.asynccontextmanager
    async def _connection(self) -> AsyncIterator[Connection]:
        async with asyncio.timeout(self._timeout):
            conn = await self._pool.acquire()
            # Assume the connection is unusable until the command completes.
            # An interrupted command may have left part of its response unread.
            broken = True
            try:
                yield conn
                broken = False
            finally:
                self._pool.release(conn, broken=broken)

    async def _execute(self, chunks: Sequence[bytes], read_reply: _ReplyReader[_R]) -> _R:
        """Send a request and return what read_reply makes of the reply.

        read_reply must read the whole reply: if it raises, the connection is
        discarded along with anything unread.
        """
        async with self._connection() as conn:
            conn.writer.writelines(chunks)
            await conn.writer.drain()
            reply = await read_reply(conn.reader)
        if isinstance(reply, ClientException):
            raise reply
        return reply

    async def _execute_simple_command(
        self, *chunks: bytes, parse: Callable[[bytearray], _R], has_data: bool = False
    ) -> _R:
        """Send a command and return what parse makes of its one-line reply.

        parse must raise ClientException for a reply it doesn't expect.
        """
        return await self._execute(chunks, functools.partial(_read_line, parse, has_data))

    async def close(self) -> None:
        """Closes the sockets if its open."""
        await self._pool.clear()

    @overload
    async def _multi_get(self, *keys: bytes,
                         with_cas: Literal[True] = ...) -> _Result[_T, int]:
        ...

    @overload
    async def _multi_get(self, *keys: bytes,
                         with_cas: Literal[False]) -> _Result[_T, None]:
        ...

    async def _multi_get(  # type: ignore[misc]
        self, *keys: bytes,
            with_cas: bool = True) -> _Result[_T, int | None]:
        # req  - get <key> [<key> ...]\r\n
        if not keys:
            return {}, {}

        for key in keys:
            self._validate_key(key)

        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')

        cmd = b'gets ' if with_cas else b'get '
        return await self._retrieve(cmd + b" ".join(keys) + b"\r\n", len(keys), with_cas)

    async def _retrieve(self, raw_command: bytes, n_keys: int,
                        with_cas: bool) -> _Result[_T, int | None]:
        items = await self._execute(
            (raw_command,), functools.partial(_read_values, n_keys, with_cas))

        received: dict[bytes, bytes | _T] = {}
        cas_tokens: dict[bytes, int | None] = {}
        # The flag handler runs once the reply is read, so that it doesn't
        # hold the connection.
        for key, (val_bytes, flags, cas_token) in items.items():
            if flags:
                if not self._get_flag_handler:
                    raise ClientException("received flags without handler")

                received[key] = await self._get_flag_handler(val_bytes, flags)
            else:
                received[key] = val_bytes
            cas_tokens[key] = cas_token

        return received, cas_tokens

    async def delete(self, key: bytes) -> bool:
        """Deletes a key/value pair from the server.

        :param key: is the key to delete.
        :return: True if case values was deleted or False to indicate
        that the item with this key was not found.
        """
        self._validate_key(key)

        def parse(response: bytearray) -> bool:
            if response not in (const.DELETED, const.NOT_FOUND):
                raise ClientException("Memcached delete failed", response)
            return response == const.DELETED

        command = b'delete ' + key + b'\r\n'
        return await self._execute_simple_command(command, parse=parse)

    @overload
    async def get(self, key: bytes, default: None = ...) -> bytes | _T | None:
        ...

    @overload
    async def get(self, key: bytes, default: _U) -> bytes | _T | _U:
        ...

    async def get(
        self, key: bytes, default: _U | None = None
    ) -> bytes | _T | _U | None:
        """Gets a single value from the server.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, is the data for this specified key.
        """
        values, _ = await self._multi_get(key, with_cas=False)
        return values.get(key, default)

    async def gets(
        self, key: bytes, default: bytes | None = None
    ) -> tuple[bytes | _T | None, int | None]:
        """Gets a single value from the server together with the cas token.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, ``bytes tuple with the value and the cas
        """
        values, cas_tokens = await self._multi_get(key, with_cas=True)
        return values.get(key, default), cas_tokens.get(key)

    async def multi_get(self, *keys: bytes) -> tuple[bytes | _T | None, ...]:
        """Takes a list of keys and returns a list of values.

        :param keys: ``list`` keys for the item being fetched.
        :return: ``list`` of values for the specified keys.
        :raises:``ValidationException``, ``ClientException``,
        and socket errors
        """
        values, _ = await self._multi_get(*keys)
        return tuple(values.get(key) for key in keys)

    async def stats(self, args: bytes | None = None) -> dict[bytes, bytes | None]:
        """Runs a stats command on the server."""
        # req  - stats [additional args]\r\n
        if args is None:
            args = b''

        return await self._execute((b"stats ", args, b"\r\n"), _read_stats)

    async def _storage_command(self, command: bytes, key: bytes,
                               value: bytes | _T, exptime: int = 0,
                               cas: int | None = None) -> bool:
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
        self._validate_key(key)

        # Only append() and prepend() can fail this. memcached ignores their exptime,
        # so they don't use _validate_exptime().
        if isinstance(exptime, bool) or not isinstance(exptime, int):  # bool is an int
            raise ValidationException('exptime not int', exptime)
        elif exptime < 0:
            raise ValidationException('exptime negative', exptime)

        flags = 0
        if not isinstance(value, bytes):
            # flag handler only invoked on non-byte values,
            # consistent with only being invoked on non-zero flags on retrieval
            if self._set_flag_handler is None:
                raise ValidationException("flag handler must be set for non-byte values")
            value, flags = await self._set_flag_handler(value)
        cas_value = b" %a" % cas if cas else b""
        # Only the header is built here; the value is written once a
        # connection is held, so waiting calls don't keep a copy of it.
        header = b"%b %b %a %a %a%b\r\n" % (
            command, key, flags, exptime, len(value), cas_value
        )

        def parse(resp: bytearray) -> bool:
            if resp not in (const.STORED, const.NOT_STORED, const.EXISTS, const.NOT_FOUND):
                raise ClientException(f"stats {command.decode()} failed", resp)
            return resp == const.STORED

        return await self._execute_simple_command(
            header, value, b"\r\n", parse=parse, has_data=True)

    async def set(self, key: bytes, value: bytes | _T,
                  exptime: int | datetime.datetime = 0) -> bool:
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is the number of seconds until the item
        expires. If it's 0, the item never expires. A timezone-aware
        ``datetime`` is the time at which the item expires.
        :return: ``bool``, True in case of success.
        """
        exptime = self._validate_exptime(exptime)
        return await self._storage_command(b"set", key, value, exptime)

    async def cas(self, key: bytes, value: bytes | _T, cas_token: int,
                  exptime: int | datetime.datetime = 0) -> bool:
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        only if value hasn't changed from first retrieval

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is the number of seconds until the item
        expires. If it's 0, the item never expires. A timezone-aware
        ``datetime`` is the time at which the item expires.
        :param cas_token: ``int``, unique cas token retrieve from previous
            ``gets``
        :return: ``bool``, True in case of success.
        """
        exptime = self._validate_exptime(exptime)
        return await self._storage_command(b"cas", key, value, exptime,
                                           cas=cas_token)

    async def add(self, key: bytes, value: bytes | _T,
                  exptime: int | datetime.datetime = 0) -> bool:
        """Store this data, but only if the server *doesn't* already
        hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int``, is the number of seconds until the item
        expires. If it's 0, the item never expires. A timezone-aware
        ``datetime`` is the time at which the item expires.
        :return: ``bool``, True in case of success.
        """
        exptime = self._validate_exptime(exptime)
        return await self._storage_command(b"add", key, value, exptime)

    async def replace(self, key: bytes, value: bytes | _T,
                      exptime: int | datetime.datetime = 0) -> bool:
        """Store this data, but only if the server *does*
        already hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int``, is the number of seconds until the item
        expires. If it's 0, the item never expires. A timezone-aware
        ``datetime`` is the time at which the item expires.
        :return: ``bool``, True in case of success.
        """
        exptime = self._validate_exptime(exptime)
        return await self._storage_command(b"replace", key, value, exptime)

    async def append(self, key: bytes, value: bytes | _T,
                     exptime: int = 0) -> bool:
        """Add data to an existing key after existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int``, is ignored by memcached. The item keeps
        its expiration time.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(b"append", key, value, exptime)

    async def prepend(self, key: bytes, value: bytes, exptime: int = 0) -> bool:
        """Add data to an existing key before existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is ignored by memcached. The item keeps
        its expiration time.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(b"prepend", key, value, exptime)

    async def _incr_decr(
        self, command: bytes, key: bytes, delta: int
    ) -> int | None:
        def parse(resp: bytearray) -> int | None:
            if resp == const.NOT_FOUND:
                return None
            if not resp.isdigit():
                raise ClientException(f"Memcached {str(command)} command failed", resp)
            return int(resp)

        cmd = b"%b %b %a\r\n" % (command, key, delta)
        return await self._execute_simple_command(cmd, parse=parse)

    async def incr(self, key: bytes, increment: int = 1) -> int | None:
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
        self._validate_key(key)
        return await self._incr_decr(b"incr", key, increment)

    async def decr(self, key: bytes, decrement: int = 1) -> int | None:
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
        self._validate_key(key)
        return await self._incr_decr(b"decr", key, decrement)

    async def touch(self, key: bytes, exptime: int | datetime.datetime) -> bool:
        """The command is used to update the expiration time of
        an existing item without fetching it.

        :param key: ``bytes``, is the key to update expiration time
        :param exptime: ``int``, is the number of seconds until the item
        expires. If it's 0, the item never expires. A timezone-aware
        ``datetime`` is the time at which the item expires. This replaces
        the existing expiration time.
        :return: ``bool``, True in case of success.
        """
        self._validate_key(key)
        exptime = self._validate_exptime(exptime)

        def parse(resp: bytearray) -> bool:
            if resp not in (const.TOUCHED, const.NOT_FOUND):
                raise ClientException("Memcached touch failed", resp)
            return resp == const.TOUCHED

        cmd = b"touch %b %a\r\n" % (key, exptime)
        return await self._execute_simple_command(cmd, parse=parse)

    async def version(self) -> bytearray:
        """Current version of the server.

        :return: memcached version for the current server.
        """

        def parse(response: bytearray) -> bytearray:
            if not response.startswith(const.VERSION):
                raise ClientException("Memcached version failed", response)
            version, number = response.rstrip(b"\r\n").split(maxsplit=1)
            return number

        return await self._execute_simple_command(b'version\r\n', parse=parse)

    async def flush_all(self) -> None:
        """Its effect is to invalidate all existing items immediately"""
        def parse(response: bytearray) -> None:
            if response != const.OK:
                raise ClientException("Memcached flush_all failed", response)

        await self._execute_simple_command(b'flush_all\r\n', parse=parse)


class Client(FlagClient[bytes]):
    @overload
    def __init__(self, host: str, port: int, *,
                 pool_size: int = 2, pool_minsize: int | None = None,
                 conn_args: Mapping[str, Any] | None = None,
                 timeout: float | None = None,
                 conn_timeout: float | None = None):
        ...

    @overload
    def __init__(self, *, path: str,
                 pool_size: int = 2, pool_minsize: int | None = None,
                 conn_args: Mapping[str, Any] | None = None,
                 timeout: float | None = None,
                 conn_timeout: float | None = None):
        ...

    def __init__(self, host: str = '127.0.0.1', port: int = 11211, *,
                 path: str = '',
                 pool_size: int = 2, pool_minsize: int | None = None,
                 conn_args: Mapping[str, Any] | None = None,
                 timeout: float | None = None,
                 conn_timeout: float | None = None):

        # unlikely to provide host/port with the overloads, but still need to deal with it
        if path:
            host = path
            port = -1

        super().__init__(host, port, pool_size=pool_size, pool_minsize=pool_minsize,
                         conn_args=conn_args,
                         get_flag_handler=None, set_flag_handler=None,
                         timeout=timeout, conn_timeout=conn_timeout)
