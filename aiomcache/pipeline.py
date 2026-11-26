import asyncio
import collections
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, TypeVar

from .exceptions import ClientException
from .pool import Connection

__all__ = ("Pipeline",)

_R = TypeVar("_R")

# Requests are sent together at the end of the event loop iteration they are
# made in, or sooner once this many bytes are waiting.
_SEND_SIZE = 64 * 1024

_BROKEN = "Pipeline broken by an earlier command"


class Pipeline:
    """Send requests on a connection without waiting for the earlier replies.

    memcached replies to requests in the order it receives them. So each
    request is queued along with how to read its reply, and one task reads the
    replies in that order for the commands waiting for them.
    """

    __slots__ = ("_conn", "_loop", "_send_lock", "_unsent", "_unsent_size", "_replies",
                 "_reader", "_error")

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._loop = asyncio.get_running_loop()
        # Held by the command whose turn it is to send.
        self._send_lock = asyncio.Lock()
        self._unsent: list[bytes] = []
        self._unsent_size = 0
        # The replies memcached owes, oldest first: how to read each one, and
        # the future of the command waiting for it.
        self._replies: collections.deque[
            tuple[Callable[[asyncio.StreamReader], Awaitable[Any]], asyncio.Future[Any]]
        ] = collections.deque()
        # The task reading the replies. It is done whenever none is owed.
        self._reader: asyncio.Task[None] | None = None
        # Why no more requests can be sent, once that is so.
        self._error: str | None = None

    async def send(
        self, chunks: Sequence[bytes],
        read_reply: Callable[[asyncio.StreamReader], Awaitable[_R]],
    ) -> asyncio.Future[_R]:
        """Send a request, returning a future for what read_reply returns.

        read_reply is called once the reply is next on the connection, and
        must read all of it. If it raises, the replies after it can't be read.
        """
        async with self._send_lock:
            if self._error is None:
                # Wait for room in the write buffer, each command in its turn.
                await self._conn.writer.drain()
            if self._error is not None:
                raise ClientException(self._error)

            # Before any of it is queued, as this fails for what isn't bytes.
            size = sum(memoryview(chunk).nbytes for chunk in chunks)
            if not self._unsent:
                self._loop.call_soon(self._send_unsent)
            self._unsent += chunks
            self._unsent_size += size
            if self._unsent_size >= _SEND_SIZE:
                self._send_unsent()

            future: asyncio.Future[_R] = self._loop.create_future()
            self._replies.append((read_reply, future))
            if self._reader is None or self._reader.done():
                self._reader = self._loop.create_task(self._read_replies())
            return future

    def _send_unsent(self) -> None:
        if self._unsent:
            chunks, self._unsent, self._unsent_size = self._unsent, [], 0
            if self._conn.writer.transport.is_closing():
                return  # The connection is lost, as reading the replies will find.
            try:
                self._conn.writer.writelines(chunks)
            except BaseException:
                # No telling what was sent, so no reply can be waited for.
                self._abort(_BROKEN)
                raise

    async def _read_replies(self) -> None:
        while self._replies:
            read_reply, future = self._replies[0]
            try:
                reply = await read_reply(self._conn.reader)
            except BaseException as exc:
                # Unless close() got here first.
                if self._error is None:
                    if isinstance(exc, Exception) and not future.done():
                        future.set_exception(exc)
                    self._abort(_BROKEN)
                if isinstance(exc, Exception):
                    return
                raise
            if self._error is not None:
                return  # Aborted as the reply arrived.
            self._replies.popleft()
            if not future.done():  # Unless its command was cancelled.
                future.set_result(reply)

    def _abort(self, error: str) -> None:
        """Fail the commands waiting for a reply, and those sent from now on.

        The rest of the stream can't be read, so the connection is dropped,
        along with the requests it has not sent yet.
        """
        self._error = error
        self._unsent.clear()
        self._conn.writer.transport.abort()
        for _, future in self._replies:
            if not future.done():
                future.set_exception(ClientException(error))
        self._replies.clear()

    @contextlib.asynccontextmanager
    async def exclusive(self) -> AsyncIterator[Connection]:
        """Yield the connection once no reply is owed.

        No request is sent until the block ends. If it raises, the connection
        is assumed to have a reply left unread.
        """
        async with self._send_lock:
            while self._reader is not None and not self._reader.done():
                await asyncio.wait((self._reader,))
            if self._error is not None:
                raise ClientException(self._error)
            try:
                yield self._conn
            except BaseException:
                if self._error is None:
                    self._abort(_BROKEN)
                raise

    def close(self) -> bool:
        """Stop sending requests, failing the commands still waiting for a reply.

        Return True if the connection can be used again.
        """
        intact = (self._error is None and not self._replies and not self._unsent
                  and not self._send_lock.locked())
        if not intact and self._error is None:
            self._abort("Pipeline closed before the reply was read")
        self._error = "Pipeline is closed"
        if self._reader is not None:
            self._reader.cancel()
        return intact

    async def wait_closed(self) -> None:
        """Wait for the task reading the replies to end, after close()."""
        if self._reader is not None and not self._reader.done():
            await asyncio.wait((self._reader,))
