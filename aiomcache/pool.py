import asyncio
from typing import Any, Mapping, NamedTuple, Optional, Set

__all__ = ['MemcachePool']


class Connection(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class MemcachePool:
    def __init__(self, host: str, port: int, *, minsize: int, maxsize: int,
                 conn_args: Optional[Mapping[str, Any]] = None):
        self._target = host
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self.conn_args = conn_args or {}
        self._pool: asyncio.Queue[Optional[Connection]] = asyncio.Queue()
        self._in_use: Set[Connection] = set()

    async def clear(self) -> None:
        """Clear pool connections."""
        while not self._pool.empty():
            conn = await self._pool.get()
            if conn is not None:
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
        if broken:
            self._do_close(conn)
            # Wake one waiter so it can discard this slot and create a
            # replacement connection. None is never handed to a caller.
            self._pool.put_nowait(None)
        elif conn.reader.at_eof() or conn.reader.exception() is not None:
            self._do_close(conn)
            self._pool.put_nowait(None)
        else:
            self._pool.put_nowait(conn)

    async def _create_new_conn(self) -> Optional[Connection]:
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
