import asyncio
from collections.abc import Mapping
from typing import Any, NamedTuple

__all__ = ["MemcachePool"]


class Connection(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class MemcachePool:
    __slots__ = (
        "__weakref__",
        "_host",
        "_port",
        "_minsize",
        "_maxsize",
        "conn_args",
        "_pool",
        "_in_use",
    )
    def __init__(self, host: str, port: int, *, minsize: int, maxsize: int,
                 conn_args: Mapping[str, Any] | None = None):
        self._target = host
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self.conn_args = conn_args or {}
        self._pool: asyncio.Queue[Connection] = asyncio.Queue()
        self._in_use: set[Connection] = set()

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

        conn: Connection | None = None
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

    async def _create_new_conn(self) -> Connection | None:
        if self.size() < self._maxsize:
            if self._port == -1:  # -1 implies unix socket
                reader, writer = await asyncio.open_unix_connection(self._target, **self.conn_args)
            else:
                reader, writer = await asyncio.open_connection(self._target, self._port,
                                                               **self.conn_args)

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
