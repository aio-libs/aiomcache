import asyncio
from collections import namedtuple

__all__ = ['MemcachePool']


_connection = namedtuple('connection', ['reader', 'writer'])


class MemcachePool:

    def __init__(self, host, port, *, minsize, maxsize, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        self._host = host
        self._port = port
        self._minsize = minsize
        self._maxsize = maxsize
        self._loop = loop
        self._pool = asyncio.Queue(maxsize, loop=loop)
        self._in_use = set()
        self._size = 0

    @asyncio.coroutine
    def clear(self):
        """Clear pool connections."""
        while not self._pool.empty():
            conn = yield from self._pool.get()
            self._do_close(conn)

    def _do_close(self, conn):
        self._size -= 1
        conn.reader.feed_eof()
        conn.writer.close()

    @asyncio.coroutine
    def acquire(self):
        """Acquire connection from the pool, or spawn new one
        if pool maxsize permits.

        :return: ``tuple`` (reader, writer)
        """
        while self._size < self._minsize:
            _conn = yield from self._create_new_conn()

            # Could not create new connection
            if _conn is None:
                break
            yield from self._pool.put(_conn)

        conn = None
        while not conn:
            if not self._pool.empty():
                _conn = yield from self._pool.get()
                if _conn.reader.at_eof() or _conn.reader.exception():
                    self._do_close(_conn)
                    conn = None
                else:
                    conn = _conn

            if conn is None:
                conn = yield from self._create_new_conn()

            # Give up control
            yield from asyncio.sleep(0, loop=self._loop)

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
            # This should never fail because poolsize=maxsize
            self._pool.put_nowait(conn)

    @asyncio.coroutine
    def _create_new_conn(self):
        if self._size < self._maxsize:
            self._size += 1
            try:
                reader, writer = yield from asyncio.open_connection(
                    self._host, self._port, loop=self._loop)
            except:
                self._size -= 1
                raise
            return _connection(reader, writer)
        else:
            return None

    def size(self):
        return self._size
