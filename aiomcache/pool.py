import asyncio
from collections import namedtuple

__all__ = ['MemcachePool']


_connection = namedtuple('connection', ['reader', 'writer'])


class MemcachePool:

    def __init__(self, host, port, *, minsize, maxsize):
        self._host = host
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self._pool = asyncio.Queue()
        self._in_use = set()

    async def clear(self):
        """Clear pool connections."""
        while not self._pool.empty():
            conn = await self._pool.get()
            self._do_close(conn)

    def _do_close(self, conn):
        conn.reader.feed_eof()
        conn.writer.close()

    async def acquire(self):
        """Acquire connection from the pool, or spawn new one
        if pool maxsize permits.

        :return: ``tuple`` (reader, writer)
        """
        while self.size() == 0 or self.size() < self._minsize:
            _conn = await self._create_new_conn()
            if _conn is None:
                break
            self._pool.put_nowait(_conn)

        conn = None
        while not conn:
            _conn = await self._pool.get()
            if _conn.reader.at_eof() or _conn.reader.exception():
                self._do_close(_conn)
                conn = await self._create_new_conn()
            else:
                conn = _conn

        self._in_use.add(conn)
        return conn

    def release(self, conn):
        """Releases connection back to the pool.

        :param conn: ``namedtuple`` (reader, writer)
        """
        self._in_use.remove(conn)
        if conn.reader.at_eof() or conn.reader.exception():
            self._do_close(conn)
        else:
            self._pool.put_nowait(conn)

    async def _create_new_conn(self):
        if self.size() < self._maxsize:
            reader, writer = await asyncio.open_connection(
                self._host, self._port)
            if self.size() < self._maxsize:
                return _connection(reader, writer)
            else:
                reader.feed_eof()
                writer.close()
                return None
        else:
            return None

    def size(self):
        return self._pool.qsize() + len(self._in_use)
