"""Time ways of sending many set commands, for docs/pipelining.md.

    python docs/pipelining_bench.py                 # memcached on localhost:11211
    python docs/pipelining_bench.py --delay-ms 0.25 # the same, through a slow link

--delay-ms sends the connections through a proxy which holds everything back
for that long in each direction, to stand in for a network.

A way is left out if this version of aiomcache doesn't have it, so the script
can be run on other branches to compare them.
"""
import argparse
import asyncio
import collections
import socket
import subprocess  # noqa: S404
import sys
import threading
import time
from collections.abc import Awaitable, Callable

import aiomcache

_Items = dict[bytes, bytes]
_Way = Callable[[aiomcache.Client, _Items], Awaitable[object]]


async def loop(mc: aiomcache.Client, items: _Items) -> None:
    for key, value in items.items():
        await mc.set(key, value)


async def gather(mc: aiomcache.Client, items: _Items) -> None:
    await asyncio.gather(*(mc.set(key, value) for key, value in items.items()))


async def pipeline(mc: aiomcache.Client, items: _Items) -> None:
    async with mc.pipeline() as pipe:
        await asyncio.gather(*(pipe.set(key, value) for key, value in items.items()))


async def multi_set(mc: aiomcache.Client, items: _Items) -> None:
    await mc.multi_set(items)  # type: ignore[attr-defined]


# Each way, and the method of the client that it needs.
WAYS: dict[str, tuple[_Way, str]] = {
    "loop of set()": (loop, "set"),
    "gather() of set()": (gather, "set"),
    "pipeline(), gather() of set()": (pipeline, "pipeline"),
    "multi_set()": (multi_set, "multi_set"),
}
CASES = ((10, 100), (100, 100), (1000, 100), (10_000, 100), (1000, 10_000))


async def bench(host: str, port: int, pool_size: int, repeat: int) -> None:
    mc = aiomcache.Client(host, port, pool_size=pool_size)
    start = time.perf_counter()
    for _ in range(20):
        await mc.version()
    print(f"A round trip takes about {(time.perf_counter() - start) / 20 * 1e6:.0f} us.\n")

    ways = {name: way for name, (way, method) in WAYS.items() if hasattr(mc, method)}
    print("| Items | " + " | ".join(ways) + " |")
    print("|---|" + "---|" * len(ways))
    for count, size in CASES:
        items = {b"bench:%d" % i: b"x" * size for i in range(count)}
        row = []
        for way in ways.values():
            best = float("inf")
            for _ in range(repeat + 1):  # The first run warms up.
                start = time.perf_counter()
                await way(mc, items)
                best = min(best, time.perf_counter() - start)
                if best > 5:
                    break
            row.append(f"{best * 1e3:.2f} ms")
        print(f"| {count:,} x {size:,} B | " + " | ".join(row) + " |")
    await mc.close()


def delay(source: socket.socket, target: socket.socket, seconds: float) -> None:
    """Pass on what source sends to target, after the delay."""
    queue: collections.deque[tuple[float, bytes]] = collections.deque()
    ready = threading.Condition()

    def send() -> None:
        while True:
            with ready:
                while not queue:
                    ready.wait()
                due, data = queue.popleft()
            time.sleep(max(due - time.perf_counter(), 0))
            try:
                if not data:
                    target.shutdown(socket.SHUT_WR)
                    return
                target.sendall(data)
            except OSError:
                return

    threading.Thread(target=send, daemon=True).start()
    data = b"-"
    while data:
        try:
            data = source.recv(65536)
        except OSError:
            data = b""
        with ready:
            queue.append((time.perf_counter() + seconds, data))
            ready.notify()


def proxy(listen_port: int, host: str, port: int, seconds: float) -> None:
    server = socket.create_server(("127.0.0.1", listen_port))
    while True:
        client, _ = server.accept()
        upstream = socket.create_connection((host, port))
        for sock in (client, upstream):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for pair in ((client, upstream), (upstream, client)):
            threading.Thread(target=delay, args=(*pair, seconds), daemon=True).start()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11211)
    parser.add_argument("--pool-size", type=int, default=2)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--delay-ms", type=float, default=0)
    parser.add_argument("--proxy", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.proxy:
        proxy(args.proxy, args.host, args.port, args.delay_ms / 1000)
    elif args.delay_ms:
        with socket.create_server(("127.0.0.1", 0)) as unused:
            listen_port = unused.getsockname()[1]
        command = [sys.executable, __file__, "--proxy", str(listen_port), "--host", args.host,
                   "--port", str(args.port), "--delay-ms", str(args.delay_ms)]
        with subprocess.Popen(command) as process:  # noqa: S603
            time.sleep(0.5)
            try:
                asyncio.run(bench("127.0.0.1", listen_port, args.pool_size, args.repeat))
            finally:
                process.terminate()
    else:
        asyncio.run(bench(args.host, args.port, args.pool_size, args.repeat))


if __name__ == "__main__":
    main()
