import asyncio


class MemcachePool:

    def __init__(self, host, port, *, minsize, maxsize, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
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
            conn = yield from self._pool.get()
            conn.close()

    @asyncio.coroutine
    def acquire(self):
        while (len(self._in_use) + self._pool.qsize()) < self._minsize:
            reader, writer = yield from asyncio.open_connection(
                self._host, self._port, loop=self._loop)
            yield from self._pool.put((reader, writer))

        conn = None
        while not conn:
            if not self._pool.empty():
                reader, writer = yield from self._pool.get()
                if reader.at_eof() or reader.exception():
                    writer.close()
                    conn = None

            if conn is None:
                conn = yield from asyncio.open_connection(
                    self._host, self._port, loop=self._loop)

        self._in_use.add(conn)
        return conn

    def release(self, conn):
        reader, writer = conn
        self._in_use.remove((reader, writer))

        if reader.at_eof() or reader.exception():
            writer.close()
        else:
            try:
                self._pool.put_nowait((reader, writer))
            except asyncio.QueueFull:
                writer.close()
