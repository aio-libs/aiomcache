import asyncio
import unittest
from aiomcache.pool import MemcachePool
from ._testutil import run_until_complete


class PoolTest(unittest.TestCase):

    def setUp(self):
        asyncio.set_event_loop(None)

        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()
        del self.loop

    @run_until_complete
    def test_pool_creation(self):
        pool = MemcachePool('localhost', 11211,
                            minsize=1, maxsize=5, loop=self.loop)
        self.assertEqual(pool.size(), 0)
        self.assertEqual(pool._minsize, 1)

    @run_until_complete
    def test_pool_acquire_release(self):
        pool = MemcachePool('localhost', 11211,
                            minsize=1, maxsize=5, loop=self.loop)
        reader, writer = yield from pool.acquire()
        self.assertIsInstance(reader, asyncio.StreamReader)
        self.assertIsInstance(writer, asyncio.StreamWriter)
        pool.release((reader, writer))

    @run_until_complete
    def test_pool_acquire_release2(self):
        pool = MemcachePool('localhost', 11211,
                            minsize=1, maxsize=5, loop=self.loop)
        reader, writer = yield from asyncio.open_connection(
            'localhost', 11211, loop=self.loop)
        # put dead connection to the pool
        writer.close()
        reader.feed_eof()

        yield from pool._pool.put((reader, writer))
        reader, writer = yield from pool.acquire()
        self.assertIsInstance(reader, asyncio.StreamReader)
        self.assertIsInstance(writer, asyncio.StreamWriter)

    @run_until_complete
    def test_pool_clear(self):
        pool = MemcachePool('localhost', 11211,
                            minsize=1, maxsize=5, loop=self.loop)
        conn = yield from pool.acquire()
        pool.release(conn)
        self.assertEqual(pool.size(), 1)
        yield from pool.clear()
        self.assertEqual(pool._pool.qsize(), 0)

    @run_until_complete
    def test_pool_is_full(self):
        pool = MemcachePool('localhost', 11211,
                            minsize=1, maxsize=2, loop=self.loop)
        conn = yield from pool.acquire()

        # put garbage to the pool make it look like full
        mocked_conns = [(0, 0), (1, 1)]
        yield from pool._pool.put(mocked_conns[0])
        yield from pool._pool.put(mocked_conns[1])

        # try to return connection back
        self.assertEqual(pool.size(), 3)
        pool.release(conn)
        self.assertEqual(pool.size(), 2)
