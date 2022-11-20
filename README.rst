memcached client for asyncio
============================

asyncio (PEP 3156) library to work with memcached.


Getting started
---------------

The API looks very similar to the other memcache clients:

.. code:: python

    import asyncio
    import aiomcache

    async def hello_aiomcache():
        mc = aiomcache.Client("127.0.0.1", 11211)
        await mc.set(b"some_key", b"Some value")
        value = await mc.get(b"some_key")
        print(value)
        values = await mc.multi_get(b"some_key", b"other_key")
        print(values)
        await mc.delete(b"another_key")

    asyncio.run(hello_aiomcache())


Version 0.8 introduces `FlagClient` which allows registering callbacks to
set or process flags.  See `examples/simple_with_flag_handler.py`
