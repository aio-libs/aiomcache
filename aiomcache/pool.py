import asyncio
from typing import Any, Awaitable, Callable, Mapping, NamedTuple, Optional, Set, Tuple

__all__ = ['MemcachePool']


class Connection(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class MemcachePool:
    def __init__(self, host: str, port: int, *, minsize: int, maxsize: int,
                 conn_args: Optional[Mapping[str, Any]] = None):
        self._target = host
        self._unix = not bool(port)
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self.conn_args = conn_args or {}
        self._pool: asyncio.Queue[Connection] = asyncio.Queue()
        self._in_use: Set[Connection] = set()
        self._opener = self._make_opener()

    async def clear(self) -> None:
        """Clear pool connections."""
        while not self._pool.empty():
            conn = await self._pool.get()
            self._do_close(conn)

    def _do_close(self, conn: Connection) -> None:
        conn.reader.feed_eof()
        conn.writer.close()

    async def acquire(self) -> Connection:
        """Acquire connection from the pool, or spawn new one
        if pool maxsize permits.

        :return: ``tuple`` (reader, writer)
        """
        while self.size() == 0 or self.size() < self._minsize:
            _conn = await self._create_new_conn()
            if _conn is None:
                break
            self._pool.put_nowait(_conn)

        conn: Optional[Connection] = None
        while not conn:
            _conn = await self._pool.get()
            if _conn.reader.at_eof() or _conn.reader.exception() is not None:
                self._do_close(_conn)
                conn = await self._create_new_conn()
            else:
                conn = _conn

        self._in_use.add(conn)
        return conn

    def release(self, conn: Connection) -> None:
        """Releases connection back to the pool.

        :param conn: ``namedtuple`` (reader, writer)
        """
        self._in_use.remove(conn)
        if conn.reader.at_eof() or conn.reader.exception() is not None:
            self._do_close(conn)
        else:
            self._pool.put_nowait(conn)

    def _make_opener(self
                     ) -> Callable[...,
                                   Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]:
        """creates open function that has the same signature for unix/tcp sockets"""

        if self._unix:
            def unix_open(str_path: str, _port: int, **kwargs: Any
                          ) -> Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
                return asyncio.open_unix_connection(str_path, **kwargs)
            return unix_open

        def ip_open(host: str, port: int, **kwargs: Any
                    ) -> Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
            return asyncio.open_connection(host, port, **kwargs)
        return ip_open

    async def _create_new_conn(self) -> Optional[Connection]:
        if self.size() < self._maxsize:
            reader, writer = await self._opener(
                self._target, self._port, **self.conn_args)
            if self.size() < self._maxsize:
                return Connection(reader, writer)
            else:
                reader.feed_eof()
                writer.close()
                return None
        else:
            return None

    def size(self) -> int:
        return self._pool.qsize() + len(self._in_use)
