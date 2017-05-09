import asyncio
from collections import namedtuple, deque

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
        self._in_use = set()
        self._pool = deque()

    @asyncio.coroutine
    def clear(self):
        """Clear pool connections."""
        while self._pool:
            conn = self._pool.popleft()
            self._do_close(conn)

    def _do_close(self, conn):
        conn.reader.feed_eof()
        conn.writer.close()

    @asyncio.coroutine
    def acquire(self):
        """Acquire connection from the pool, or spawn new one
        if pool maxsize permits.

        :return: ``tuple`` (reader, writer)
        """
        while self.size() < self._minsize:
            _conn = yield from self._create_new_conn()

            # Could not create new connection
            if _conn is None:
                break
            if self.size() < self._minsize:
                self._pool.append(_conn)

        conn = None
        while not conn:
            if self._pool:
                _conn = self._pool.popleft()
                if _conn.reader.at_eof() or _conn.reader.exception():
                    self._do_close(_conn)
                    conn = None
                else:
                    conn = _conn

            if conn is None:
                conn = yield from self._create_new_conn()
                if conn is None:
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
            self._pool.append(conn)

    @asyncio.coroutine
    def _create_new_conn(self):
        if self.size() < self._maxsize:
            try:
                reader, writer = yield from asyncio.open_connection(
                    self._host, self._port, loop=self._loop)
            except:
                raise
            return _connection(reader, writer)
        else:
            return None

    def size(self):
        return len(self._pool) + len(self._in_use)
