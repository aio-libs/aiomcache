import asyncio
import aiomcache

from unittest.mock import patch

from aiomcache.exceptions import ClientException, ValidationException
from ._testutil import BaseTest, run_until_complete


class ConnectionCommandsTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.mcache = aiomcache.Client('localhost', loop=self.loop)

    def tearDown(self):
        yield from self.mcache.close()
        super().tearDown()

    @run_until_complete
    def test_version(self):
        version = yield from self.mcache.version()
        stats = yield from self.mcache.stats()
        self.assertEqual(version, stats[b'version'])

        with patch.object(self.mcache, '_execute_simple_command') as patched, \
                self.assertRaises(ClientException):
            fut = asyncio.Future(loop=self.loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut
            yield from self.mcache.version()

    @run_until_complete
    def test_flush_all(self):
        key, value = b'key:flush_all', b'flush_all_value'
        yield from self.mcache.set(key, value)
        # make sure value exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)
        # flush data
        yield from self.mcache.flush_all()
        # make sure value does not exists
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, None)

        with patch.object(self.mcache, '_execute_simple_command') as patched, \
                self.assertRaises(ClientException):
            fut = asyncio.Future(loop=self.loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut
            yield from self.mcache.flush_all()

    @run_until_complete
    def test_set_get(self):
        key, value = b'key:set', b'1'
        yield from self.mcache.set(key, value)
        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)
        test_value = yield from self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

        with patch.object(self.mcache, '_execute_simple_command') as patched, \
                self.assertRaises(ClientException):

            fut = asyncio.Future(loop=self.loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut

            yield from self.mcache.set(key, value)

    @run_until_complete
    def test_multi_get(self):
        key1, value1 = b'key:multi_get:1', b'1'
        key2, value2 = b'key:multi_get:2', b'2'
        yield from self.mcache.set(key1, value1)
        yield from self.mcache.set(key2, value2)
        test_value = yield from self.mcache.multi_get(key1, key2)
        self.assertEqual(test_value, [value1, value2])

        test_value = yield from self.mcache.multi_get(b'not' + key1, key2)
        self.assertEqual(test_value, [None, value2])
        test_value = yield from self.mcache.multi_get()
        self.assertEqual(test_value, [])

    @run_until_complete
    def test_multi_get_doubling_keys(self):
        key, value = b'key:multi_get:3', b'1'
        yield from self.mcache.set(key, value)

        with self.assertRaises(ClientException):
            test_value = yield from self.mcache.multi_get(key, key)
            self.assertEqual(test_value, [])

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
    def test_set_errors(self):
        key, value = b'key:set', b'1'
        yield from self.mcache.set(key, value, exptime=1)

        with self.assertRaises(ValidationException):
            yield from self.mcache.set(key, value, exptime=-1)

        with self.assertRaises(ValidationException):
            yield from self.mcache.set(key, value, exptime=3.14)

    @run_until_complete
    def test_add(self):
        key, value = b'key:add', b'1'
        yield from self.mcache.set(key, value)

        test_value = yield from self.mcache.add(key, b'2')
        self.assertEqual(test_value, False)

        test_value = yield from self.mcache.add(b'not:' + key, b'2')
        self.assertEqual(test_value, True)

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

        with patch.object(self.mcache, '_execute_simple_command') as patched, \
                self.assertRaises(ClientException):
            fut = asyncio.Future(loop=self.loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut

            yield from self.mcache.delete(key)

    @run_until_complete
    def test_delete_key_not_exists(self):
        is_deleted = yield from self.mcache.delete(b'not:key')
        self.assertFalse(is_deleted)

    @run_until_complete
    def test_incr(self):
        key, value = b'key:incr:1', b'1'
        yield from self.mcache.set(key, value)

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

        test_value = yield from self.mcache.decr(key, 2)
        self.assertEqual(test_value, 15)

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

    @run_until_complete
    def test_stats(self):
        stats = yield from self.mcache.stats()
        self.assertTrue(b'pid' in stats)

    @run_until_complete
    def test_touch(self):
        key, value = b'key:touch:1', b'17'
        yield from self.mcache.set(key, value)

        test_value = yield from self.mcache.touch(key, 1)
        self.assertEqual(test_value, True)

        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, value)

        yield from asyncio.sleep(1, loop=self.loop)

        test_value = yield from self.mcache.get(key)
        self.assertEqual(test_value, None)

        test_value = yield from self.mcache.touch(b'not:' + key, 1)
        self.assertEqual(test_value, False)

        with patch.object(self.mcache, '_execute_simple_command') as patched, \
                self.assertRaises(ClientException):
            fut = asyncio.Future(loop=self.loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut

            yield from self.mcache.touch(b'not:' + key, 1)

    @run_until_complete
    def test_close(self):
        yield from self.mcache.close()
        self.assertEqual(self.mcache._pool.size(), 0)
