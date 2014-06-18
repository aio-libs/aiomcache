memcached client for asyncio
============================

asyncio (PEP 3156) library to work with memcached.

.. image:: https://travis-ci.org/aio-libs/aiomcache.svg?branch=master
   :target: https://travis-ci.org/aio-libs/aiomcache


Requirements
------------

- Python >= 3.3
- asyncio https://pypi.python.org/pypi/asyncio/


Getting started
---------------

The API looks very similar to the other memcache clients::

    import aiomcache
    mc = aiomcache.Client("127.0.0.1", 11211, pool_size)
    yield from mc.set(b"some_key", b"Some value")
    value = yield from mc.get(b"some_key")
    values = yield from mc.multi_get(b"some_key")
    yield from mc.delete(b"another_key")
