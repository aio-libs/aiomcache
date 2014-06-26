from functools import wraps
import asyncio
import unittest
import aiomcache

from aiomcache.exceptions import ClientException


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
        asyncio.set_event_loop(None)

        self.loop = asyncio.new_event_loop()
        self.mcache = aiomcache.Client('localhost', loop=self.loop)

    def tearDown(self):
        self.loop.close()
        del self.loop

    @run_until_complete
    def test_version(self):
        version = yield from self.mcache.version()
        self.assertTrue(version)

    @run_until_complete
    def test_flush_all(self):
        key, value = b'key:flush_all', b'flush_all_value'
        yield from self.mcache.set(key, value)
        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

        yield from self.mcache.flush_all()
        # make sure value does not exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, None)

    @unittest.skip("Compare with stats")
    @run_until_complete
    def test_verbosity(self):

        yield from self.mcache.verbosity(1)
        self.assertTrue(True)

    @run_until_complete
    def test_set(self):
        key, value = b'key:set', b'1'
        yield from self.mcache.set(key, value)
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

    @run_until_complete
    def test_set_expire(self):
        key, value = b'key:set', b'1'
        yield from self.mcache.set(key, value, exptime=1)

        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

        yield from asyncio.sleep(1, loop=self.loop)

        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_add(self):
        key, value = b'key:add', b'1'
        yield from self.mcache.set(key, value)

        test_value = yield from self.mcache.add(key, b'2')
        self.assertEqual(test_value, False)

        test_value = yield from self.mcache.add(b'not:' + key, b'2')
        self.assertEqual(test_value, True)
        # make sure value exists
        test_value = yield from self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, b'2')

    @run_until_complete
    def test_replace(self):
        key, value = b'key:replace', b'1'
        yield from self.mcache.set(key, value)

        test_value = yield from self.mcache.replace(key, b'2')
        self.assertEqual(test_value, True)
        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, b'2')

        test_value = yield from self.mcache.replace(b'not:' + key, b'3')
        self.assertEqual(test_value, False)
        # make sure value exists
        test_value = yield from self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_append(self):
        key, value = b'key:append', b'1'
        yield from self.mcache.set(key, value)

        test_value = yield from self.mcache.append(key, b'2')
        self.assertEqual(test_value, True)

        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, b'12')

        test_value = yield from self.mcache.append(b'not:' + key, b'3')
        self.assertEqual(test_value, False)
        # make sure value exists
        test_value = yield from self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_prepend(self):
        key, value = b'key:prepend', b'1'
        yield from self.mcache.set(key, value)

        test_value = yield from self.mcache.prepend(key, b'2')
        self.assertEqual(test_value, True)

        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, b'21')

        test_value = yield from self.mcache.prepend(b'not:' + key, b'3')
        self.assertEqual(test_value, False)
        # make sure value exists
        test_value = yield from self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_delete(self):
        key, value = b'key:delete', b'value'
        yield from self.mcache.set(key, value)

        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

        is_deleted = yield from self.mcache.delete(key)
        self.assertTrue(is_deleted)
        # make sure value does not exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_delete_key_not_exists(self):
        is_deleted = yield from self.mcache.delete(b'not:key')
        self.assertFalse(is_deleted)

    @run_until_complete
    def test_incr(self):
        key, value = b'key:incr:1', b'1'
        yield from self.mcache.set(key, value)
        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

        test_value = yield from self.mcache.incr(key, 2)
        self.assertEqual(test_value, 3)

        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, b'3')

    @run_until_complete
    def test_incr_errors(self):
        key, value = b'key:incr:2', b'string'
        yield from self.mcache.set(key, value)

        with self.assertRaises(ClientException):
            yield from self.mcache.incr(key, 2)

        with self.assertRaises(ClientException):
            yield from self.mcache.incr(key, 3.14)

    @run_until_complete
    def test_decr(self):
        key, value = b'key:decr:1', b'17'
        yield from self.mcache.set(key, value)
        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

        test_value = yield from self.mcache.decr(key, 2)
        self.assertEqual(test_value, 15)

        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, b'15')

        test_value = yield from self.mcache.decr(key, 1000)
        self.assertEqual(test_value, 0)

    @run_until_complete
    def test_decr_errors(self):
        key, value = b'key:decr:2', b'string'
        yield from self.mcache.set(key, value)

        with self.assertRaises(ClientException):
            yield from self.mcache.decr(key, 2)

        with self.assertRaises(ClientException):
            yield from self.mcache.decr(key, 3.14)
