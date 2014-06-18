import asyncio
import aiomcache


def main(loop):
    mc = aiomcache.Client('127.0.0.1', 11211, loop=loop)
    yield from mc.set(b'some_key', b'Some value')
    value = yield from mc.get(b'some_key')
    print (value)
    values = yield from mc.multi_get(b"some_key")
    print (values)
    yield from mc.delete(b'another_key')
    yield from mc.delete(b'some_key')
    value = yield from mc.get(b'some_key')
    print (value)
    values = yield from mc.multi_get(b"some_key")
    print (values)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task = asyncio.async(main(loop), loop=loop)
    try:
        loop.run_until_complete(task)
    except:
        import traceback
        traceback.print_exc()

    loop.stop()
    loop.close()
