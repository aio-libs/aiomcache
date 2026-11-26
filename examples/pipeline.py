import asyncio

import aiomcache
from aiomcache.exceptions import ClientException


async def hello_pipeline() -> None:
    mc = aiomcache.Client("127.0.0.1", 11211)
    await mc.set(b"hits", b"0")

    # Commands called on pipe are sent without waiting for earlier replies, so
    # these three share one round trip. Each returns what it usually does.
    async with mc.pipeline() as pipe:
        stored, value, hits = await asyncio.gather(
            pipe.set(b"some_key", b"Some value"),
            pipe.get(b"some_key"),
            pipe.incr(b"hits"),
        )
    print(stored, value, hits)

    # Any command can be sent this way, as many at once as needed. A command
    # which fails doesn't stop the others.
    items = {b"key:%d" % i: b"value %d" % i for i in range(1000)}
    items[b"key:large"] = b"x" * 2 * 1024 * 1024
    async with mc.pipeline() as pipe:
        results = await asyncio.gather(
            *(pipe.set(key, value) for key, value in items.items()),
            return_exceptions=True,
        )
    failed = {key: result for key, result in zip(items, results)
              if isinstance(result, ClientException)}
    print(f"stored {len(items) - len(failed)} items, failed: {failed}")

    await mc.close()


asyncio.run(hello_pipeline())
