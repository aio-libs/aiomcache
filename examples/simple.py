import asyncio
import aiomcache


loop = asyncio.get_event_loop()


async def hello_aiomcache():
    mc = aiomcache.Client("127.0.0.1", 11211, loop=loop)
    await mc.set(b"some_key", b"Some value")
    value = await mc.get(b"some_key")
    print(value)
    values = await mc.multi_get(b"some_key", b"other_key")
    print(values)
    await mc.delete(b"another_key")


loop.run_until_complete(hello_aiomcache())
