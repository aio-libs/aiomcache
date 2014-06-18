import asyncio
import coverage
from pprint import pprint


def main():
    import aiomcache
    client = aiomcache.Client('localhost')

    # set
    yield from client.set(b'test', b'1')
    res = yield from client.get(b'test')
    assert res == b'1', 'Should be equal'

    yield from client.set(b'test1', b'2')
    res = yield from client.multi_get(b'test', b'test1', b'test2')
    assert res == [b'1', b'2', None]

    # delete
    yield from client.delete(b'test')
    res = yield from client.get(b'test')
    assert res is None, 'Should be None'

    # stats
    pprint((yield from client.stats()))

    client.close()


if __name__ == '__main__':
    cov = coverage.coverage(branch=True, source=['aiomcache'])
    cov.start()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    cov.stop()
    cov.save()
    cov.html_report(directory='coverage')
    cov.report(show_missing=False)
