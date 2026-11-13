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


Running the tests
-----------------

The tests need two running memcached servers, one listening on
``localhost:11211`` and one listening on the unix socket ``/tmp/memcached.sock``:

.. code:: shell

    memcached -d -l 127.0.0.1 -p 11211
    memcached -d -s /tmp/memcached.sock
    pip install -r requirements.txt
    pytest

To test against servers listening elsewhere, pass their addresses to pytest
(using ``=``, otherwise pytest mistakes the socket path for a test path):

.. code:: shell

    pytest --memcached=127.0.0.1:11311 --memcached-socket=/run/memcached/memcached.sock

Note that the tests write to both servers and flush all data from the TCP one.
