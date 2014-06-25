from functools import wraps
import asyncio
import unittest
import aiomcache


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(fun(test, *args, **kw))
        return ret
    return wrapper


class ConnectionCommandsTest(unittest.TestCase):
    """Base test case for unittests.
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()
        del self.loop


    @run_until_complete
    def test_version(self):
        mclient = aiomcache.Client('localhost', loop=self.loop)
        version = yield from mclient.version()
        self.assertTrue(version)

    @run_until_complete
    def test_version(self):
        import ipdb; ipdb.set_trace()
        mclient = aiomcache.Client('localhost', loop=self.loop)
        yield from mclient.flush_all()
        self.assertTrue(True)

    @run_until_complete
    def test_version(self):
        import ipdb; ipdb.set_trace()
        mclient = aiomcache.Client('localhost', loop=self.loop)
        yield from mclient.verbosity(1)
        self.assertTrue(True)