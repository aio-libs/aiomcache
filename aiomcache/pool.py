import asyncio
from collections.abc import Mapping
from typing import Any, NamedTuple

__all__ = ['MemcachePool']


class Connection(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class MemcachePool:
    __slots__ = (
        "__weakref__",
        "_target",
        "_port",
        "_minsize",
        "_maxsize",
        "conn_args",
        "_pool",
        "_in_use",
        "_conn_timeout",
    )

    def __init__(self, host: str, port: int, *, minsize: int, maxsize: int,
                 conn_args: Mapping[str, Any] | None = None,
                 conn_timeout: float | None = None):
        self._target = host
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self.conn_args = conn_args or {}
        self._conn_timeout = conn_timeout

        self._pool: asyncio.Queue[Connection | None] = asyncio.Queue()
        self._in_use: set[Connection] = set()

    async def clear(self) -> None:
        """Clear pool connections."""
        while not self._pool.empty():
            conn = await self._pool.get()
            if conn is not None:
                self._do_close(conn)

    def _do_close(self, conn: Connection, *, abort: bool = False) -> None:
        conn.reader.feed_eof()
        if abort:
            # Drop anything still buffered instead of sending it.
            conn.writer.transport.abort()
        else:
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
            if (
                _conn is None
                or _conn.reader.at_eof()
                or _conn.reader.exception() is not None
            ):
                if _conn is not None:
                    self._do_close(_conn)
                try:
                    conn = await self._create_new_conn()
                except BaseException:
                    self._pool.put_nowait(None)
                    raise
            else:
                conn = _conn

        self._in_use.add(conn)
        return conn

    def release(self, conn: Connection, *, broken: bool = False) -> None:
        """Releases connection back to the pool.

        :param conn: ``namedtuple`` (reader, writer)
        :param broken: if True, the connection is closed instead of being
            returned to the pool. This should be used when a command may
            have left unread data in the stream (e.g. it was cancelled).
        """
        self._in_use.remove(conn)
        broken = broken or conn.reader.at_eof() or conn.reader.exception() is not None
        released_conn: Connection | None = conn
        if broken:
            # A command may have been interrupted with part of it still
            # buffered. Sending it now would have memcached carry it out
            # after the caller gave up on it.
            self._do_close(conn, abort=True)
            # None will wake a waiter so it can discard this slot and create a new conn.
            released_conn = None
        self._pool.put_nowait(released_conn)

    async def _create_new_conn(self) -> Connection | None:
        if self.size() < self._maxsize:
            if self._port == -1:  # -1 implies unix socket
                coro = asyncio.open_unix_connection(self._target, **self.conn_args)
            else:
                coro = asyncio.open_connection(self._target, self._port, **self.conn_args)
            async with asyncio.timeout(self._conn_timeout):
                reader, writer = await coro

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
