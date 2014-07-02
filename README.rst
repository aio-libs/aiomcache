memcached client for asyncio
============================

asyncio (PEP 3156) library to work with memcached.

.. image:: https://travis-ci.org/aio-libs/aiomcache.svg?branch=master
   :target: https://travis-ci.org/aio-libs/aiomcache


Getting started
---------------

The API looks very similar to the other memcache clients:

.. code:: python

    import asyncio
    import aiomcache

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def hello_aiomcache():
        mc = aiomcache.Client("127.0.0.1", 11211, loop=loop)
        yield from mc.set(b"some_key", b"Some value")
        value = yield from mc.get(b"some_key")
        print(value)
        values = yield from mc.multi_get(b"some_key", b"other_key")
        print(values)
        yield from mc.delete(b"another_key")

    loop.run_until_complete(hello_aiomcache())


Requirements
------------

- Python >= 3.3
- asyncio https://pypi.python.org/pypi/asyncio/