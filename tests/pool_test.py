import asyncio
import unittest
import aiomcache
from aiomcache.pool import MemcachePool
from ._testutil import run_until_complete


class PoolTest(unittest.TestCase):

    def setUp(self):
        asyncio.set_event_loop(None)

        self.loop = asyncio.new_event_loop()
        self.mcache = aiomcache.Client(
            'localhost', loop=self.loop)

    def tearDown(self):
        self.loop.close()
        del self.loop

    @run_until_complete
    def test_pool_creation(self):
        pool = MemcachePool('localhost', 11211,
                            minsize=1, maxsize=5, loop=self.loop)
        self.assertEqual(pool._pool.qsize(), 0)
        self.assertEqual(pool._minsize, 1)
