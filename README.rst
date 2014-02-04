memcached client for asyncio
============================

aiomemcache is a minimal, pure python client for memcached, kestrel, etc.


Requirements
------------

- Python >= 3.3
- asyncio https://pypi.python.org/pypi/asyncio/


Getting started
---------------

The API looks very similar to the other memcache clients::

    import aiomemcache
    mc = aiomemcache.Client("127.0.0.1", 11211, connect_timeout=5)
    yield from mc.set(b"some_key", b"Some value")
    value = yield from mc.get(b"some_key")
    yield from mc.delete(b"another_key")
