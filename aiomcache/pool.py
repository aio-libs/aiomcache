import asyncio

__all__ = ['MemcachePool']


class MemcachePool:

    def __init__(self, host, port, *, minsize, maxsize, loop=None):
        loop = loop if loop is not None else asyncio.get_event_loop()
        self._host = host
        self._port = port
        self._minsize = minsize
        self._loop = loop
        self._pool = asyncio.Queue(maxsize, loop=loop)
        self._in_use = set()

    @asyncio.coroutine
    def clear(self):
        """Clear pool connections."""
        while not self._pool.empty():
            reader, writer = yield from self._pool.get()
            self._do_close(reader, writer)

    def _do_close(self, reader, writer):
        reader.feed_eof()
        writer.close()

    @asyncio.coroutine
    def acquire(self):
        """Acquire connection from the pool, or spawn new one
        if pool maxsize permits.

        :return: ``tuple`` (reader, writer)
        """
        while self.size() < self._minsize:
            _conn = yield from self._create_new_conn()
            yield from self._pool.put(_conn)

        conn = None
        while not conn:
            if not self._pool.empty():
                reader, writer = yield from self._pool.get()
                if reader.at_eof() or reader.exception():
                    writer.close()
                    conn = None

            if conn is None:
                conn = yield from self._create_new_conn()

        self._in_use.add(conn)
        return conn

    def release(self, conn):
        """Releases connection back to the pool.

        :param conn: ``tuple`` (reader, writer)
        """
        reader, writer = conn
        self._in_use.remove((reader, writer))

        if reader.at_eof() or reader.exception():
            self._do_close(reader, writer)
        else:
            try:
                self._pool.put_nowait((reader, writer))
            except asyncio.QueueFull:
                self._do_close(reader, writer)

    @asyncio.coroutine
    def _create_new_conn(self):
        reader, writer = yield from asyncio.open_connection(
            self._host, self._port, loop=self._loop)
        return reader, writer

    def size(self):
        return len(self._in_use) + self._pool.qsize()
