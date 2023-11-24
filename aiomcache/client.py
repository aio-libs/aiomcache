import functools
import re
import sys
from typing import (Any, Awaitable, Callable, Dict, Generic, Literal, Mapping, Optional,
                    Tuple, TypeVar, Union, overload)

from . import constants as const
from .exceptions import ClientException, ValidationException
from .pool import Connection, MemcachePool

if sys.version_info >= (3, 10):
    from typing import Concatenate, ParamSpec
else:
    from typing_extensions import Concatenate, ParamSpec

__all__ = ['Client']

_P = ParamSpec("_P")
_T = TypeVar("_T")
_U = TypeVar("_U")
_Client = TypeVar("_Client", bound="FlagClient[Any]")
_Result = Tuple[Dict[bytes, Union[bytes, _T]], Dict[bytes, _U]]

_GetFlagHandler = Callable[[bytes, int], Awaitable[_T]]
_SetFlagHandler = Callable[[_T], Awaitable[Tuple[bytes, int]]]


def acquire(
    func: Callable[Concatenate[_Client, Connection, _P], Awaitable[_T]]
) -> Callable[Concatenate[_Client, _P], Awaitable[_T]]:

    @functools.wraps(func)
    async def wrapper(self: _Client, *args: _P.args,  # type: ignore[misc]
                      **kwargs: _P.kwargs) -> _T:
        conn = await self._pool.acquire()
        try:
            return await func(self, conn, *args, **kwargs)
        except Exception as exc:
            conn[0].set_exception(exc)
            raise
        finally:
            self._pool.release(conn)

    return wrapper


class FlagClient(Generic[_T]):
    def __init__(self, host: str, port: int = 11211, *,
                 pool_size: int = 2, pool_minsize: Optional[int] = None,
                 conn_args: Optional[Mapping[str, Any]] = None,
                 get_flag_handler: Optional[_GetFlagHandler[_T]] = None,
                 set_flag_handler: Optional[_SetFlagHandler[_T]] = None):
        """
        Creates new Client instance.

        :param host: memcached host
        :param port: memcached port
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
        """
        if not pool_minsize:
            pool_minsize = pool_size

        self._pool = MemcachePool(
            host, port, minsize=pool_minsize, maxsize=pool_size,
            conn_args=conn_args)

        self._get_flag_handler = get_flag_handler
        self._set_flag_handler = set_flag_handler

    # key may be anything except whitespace and control chars, upto 250 characters.
    # Must be str for unicode-aware regex.
    _valid_key_re = re.compile("^[^\\s\x00-\x1F\x7F-\x9F]{1,250}$")

    def _validate_key(self, key: bytes) -> bytes:
        if not isinstance(key, bytes):  # avoid bugs subtle and otherwise
            raise ValidationException('key must be bytes', key)

        # Must decode to str for unicode-aware comparison.
        key_str = key.decode()
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

    async def _execute_simple_command(self, conn: Connection, raw_command: bytes) -> bytes:
        response, line = bytearray(), b''

        conn.writer.write(raw_command)
        await conn.writer.drain()

        while not line.endswith(b'\r\n'):
            line = await conn.reader.readline()
            response.extend(line)
        return response[:-2]

    async def close(self) -> None:
        """Closes the sockets if its open."""
        await self._pool.clear()

    @overload
    async def _multi_get(self, conn: Connection, *keys: bytes,
                         with_cas: Literal[True] = ...) -> _Result[_T, int]:
        ...

    @overload
    async def _multi_get(self, conn: Connection, *keys: bytes,
                         with_cas: Literal[False]) -> _Result[_T, None]:
        ...

    async def _multi_get(  # type: ignore[misc]
        self, conn: Connection, *keys: bytes,
            with_cas: bool = True) -> _Result[_T, Optional[int]]:
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
        line = await conn.reader.readline()

        while line != b'END\r\n':
            terms = line.split()

            if terms and terms[0] == b"VALUE":  # exists
                key = terms[1]
                flags = int(terms[2])
                length = int(terms[3])

                val_bytes = (await conn.reader.readexactly(length+2))[:-2]
                if key in received:
                    raise ClientException('duplicate results from server')

                if flags:
                    if not self._get_flag_handler:
                        raise ClientException("received flags without handler")

                    val: Union[bytes, _T] = await self._get_flag_handler(val_bytes, flags)
                else:
                    val = val_bytes

                received[key] = val
                cas_tokens[key] = int(terms[4]) if with_cas else None
            else:
                raise ClientException('get failed', line)

            line = await conn.reader.readline()

        if len(received) > len(keys):
            raise ClientException('received too many responses')

        return received, cas_tokens

    @acquire
    async def delete(self, conn: Connection, key: bytes) -> bool:
        """Deletes a key/value pair from the server.

        :param key: is the key to delete.
        :return: True if case values was deleted or False to indicate
        that the item with this key was not found.
        """
        self._validate_key(key)

        command = b'delete ' + key + b'\r\n'
        response = await self._execute_simple_command(conn, command)

        if response not in (const.DELETED, const.NOT_FOUND):
            raise ClientException('Memcached delete failed', response)

        return response == const.DELETED

    @acquire
    @overload
    async def get(self, conn: Connection, /, key: bytes,
                  default: None = ...) -> Union[bytes, _T, None]:
        ...

    @acquire
    @overload
    async def get(self, conn: Connection, /, key: bytes, default: _U) -> Union[bytes, _T, _U]:
        ...

    @acquire
    async def get(
        self, conn: Connection, /, key: bytes, default: Optional[_U] = None
    ) -> Union[bytes, _T, _U, None]:
        """Gets a single value from the server.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, is the data for this specified key.
        """
        values, _ = await self._multi_get(conn, key, with_cas=False)
        return values.get(key, default)

    @acquire
    async def gets(
        self, conn: Connection, key: bytes, default: Optional[bytes] = None
    ) -> Tuple[Union[bytes, _T, None], Optional[int]]:
        """Gets a single value from the server together with the cas token.

        :param key: ``bytes``, is the key for the item being fetched
        :param default: default value if there is no value.
        :return: ``bytes``, ``bytes tuple with the value and the cas
        """
        values, cas_tokens = await self._multi_get(conn, key, with_cas=True)
        return values.get(key, default), cas_tokens.get(key)

    @acquire
    async def multi_get(
        self, conn: Connection, *keys: bytes
    ) -> Tuple[Union[bytes, _T, None], ...]:
        """Takes a list of keys and returns a list of values.

        :param keys: ``list`` keys for the item being fetched.
        :return: ``list`` of values for the specified keys.
        :raises:``ValidationException``, ``ClientException``,
        and socket errors
        """
        values, _ = await self._multi_get(conn, *keys)
        return tuple(values.get(key) for key in keys)

    @acquire
    async def stats(
        self, conn: Connection, args: Optional[bytes] = None
    ) -> Dict[bytes, Optional[bytes]]:
        """Runs a stats command on the server."""
        # req  - stats [additional args]\r\n
        # resp - STAT <name> <value>\r\n (one per result)
        #        END\r\n
        if args is None:
            args = b''

        conn.writer.write(b''.join((b'stats ', args, b'\r\n')))

        result: Dict[bytes, Optional[bytes]] = {}

        resp = await conn.reader.readline()
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

            resp = await conn.reader.readline()

        return result

    async def _storage_command(self, conn: Connection, command: bytes, key: bytes,
                               value: Union[bytes, _T], exptime: int = 0,
                               cas: Optional[int] = None) -> bool:
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

        if not isinstance(exptime, int):
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

        args = [str(a).encode('utf-8') for a in (flags, exptime, len(value))]
        _cmd = b' '.join([command, key] + args)
        if cas:
            _cmd += b' ' + str(cas).encode('utf-8')
        cmd = _cmd + b'\r\n' + value + b'\r\n'
        resp = await self._execute_simple_command(conn, cmd)

        if resp not in (
                const.STORED, const.NOT_STORED, const.EXISTS, const.NOT_FOUND):
            raise ClientException('stats {} failed'.format(command.decode()), resp)
        return resp == const.STORED

    @acquire
    async def set(self, conn: Connection, key: bytes, value: Union[bytes, _T],
                  exptime: int = 0) -> bool:
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(conn, b"set", key, value, exptime)

    @acquire
    async def cas(self, conn: Connection, key: bytes, value: Union[bytes, _T], cas_token: int,
                  exptime: int = 0) -> bool:
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)
        only if value hasn't changed from first retrieval

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int``, is expiration time. If it's 0, the
        item never expires.
        :param cas_token: ``int``, unique cas token retrieve from previous
            ``gets``
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(conn, b"cas", key, value, exptime,
                                           cas=cas_token)

    @acquire
    async def add(self, conn: Connection, key: bytes, value: Union[bytes, _T],
                  exptime: int = 0) -> bool:
        """Store this data, but only if the server *doesn't* already
        hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(conn, b"add", key, value, exptime)

    @acquire
    async def replace(self, conn: Connection, key: bytes, value: Union[bytes, _T],
                      exptime: int = 0) -> bool:
        """Store this data, but only if the server *does*
        already hold data for this key.

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(conn, b"replace", key, value, exptime)

    @acquire
    async def append(self, conn: Connection, key: bytes, value: Union[bytes, _T],
                     exptime: int = 0) -> bool:
        """Add data to an existing key after existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``,  data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(conn, b"append", key, value, exptime)

    @acquire
    async def prepend(self, conn: Connection, key: bytes, value: bytes, exptime: int = 0) -> bool:
        """Add data to an existing key before existing data

        :param key: ``bytes``, is the key of the item.
        :param value: ``bytes``, data to store.
        :param exptime: ``int`` is expiration time. If it's 0, the
        item never expires.
        :return: ``bool``, True in case of success.
        """
        return await self._storage_command(conn, b"prepend", key, value, exptime)

    async def _incr_decr(
        self, conn: Connection, command: bytes, key: bytes, delta: int
    ) -> Optional[int]:
        delta_byte = str(delta).encode('utf-8')
        cmd = b' '.join([command, key, delta_byte]) + b'\r\n'
        resp = await self._execute_simple_command(conn, cmd)
        if not resp.isdigit() or resp == const.NOT_FOUND:
            raise ClientException(
                'Memcached {} command failed'.format(str(command)), resp)
        return int(resp) if resp.isdigit() else None

    @acquire
    async def incr(self, conn: Connection, key: bytes, increment: int = 1) -> Optional[int]:
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
        return await self._incr_decr(conn, b"incr", key, increment)

    @acquire
    async def decr(self, conn: Connection, key: bytes, decrement: int = 1) -> Optional[int]:
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
        return await self._incr_decr(conn, b"decr", key, decrement)

    @acquire
    async def touch(self, conn: Connection, key: bytes, exptime: int) -> bool:
        """The command is used to update the expiration time of
        an existing item without fetching it.

        :param key: ``bytes``, is the key to update expiration time
        :param exptime: ``int``, is expiration time. This replaces the existing
        expiration time.
        :return: ``bool``, True in case of success.
        """
        self._validate_key(key)

        _cmd = b' '.join([b'touch', key, str(exptime).encode('utf-8')])
        cmd = _cmd + b'\r\n'
        resp = await self._execute_simple_command(conn, cmd)
        if resp not in (const.TOUCHED, const.NOT_FOUND):
            raise ClientException('Memcached touch failed', resp)
        return resp == const.TOUCHED

    @acquire
    async def version(self, conn: Connection) -> bytes:
        """Current version of the server.

        :return: ``bytes``, memcached version for current the server.
        """

        command = b'version\r\n'
        response = await self._execute_simple_command(conn, command)
        if not response.startswith(const.VERSION):
            raise ClientException('Memcached version failed', response)
        version, number = response.rstrip(b"\r\n").split(maxsplit=1)
        return number

    @acquire
    async def flush_all(self, conn: Connection) -> None:
        """Its effect is to invalidate all existing items immediately"""
        command = b'flush_all\r\n'
        response = await self._execute_simple_command(conn, command)

        if const.OK != response:
            raise ClientException('Memcached flush_all failed', response)


class Client(FlagClient[bytes]):
    def __init__(self, host: str, port: int = 11211, *,
                 pool_size: int = 2, pool_minsize: Optional[int] = None,
                 conn_args: Optional[Mapping[str, Any]] = None):
        super().__init__(host, port, pool_size=pool_size, pool_minsize=pool_minsize,
                         conn_args=conn_args,
                         get_flag_handler=None, set_flag_handler=None)
