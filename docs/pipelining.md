# Proposal: pipelining for any command

*A design proposal, with a working prototype on this branch. Nothing here is
released API. It follows from the review of `multi_set()` in [#531], which
asked whether pipelining could be a general solution that works with any
method.*

## Summary

`client.pipeline()` yields a client of the same type, whose commands are sent
without waiting for the replies to earlier commands:

```python
async with client.pipeline() as pipe:
    stored, value, hits = await asyncio.gather(
        pipe.set(b"greeting", b"hello"),
        pipe.get(b"other"),
        pipe.incr(b"hits"),
    )
```

- **Any command.** `pipe` is a `Client` (or `FlagClient`, or a subclass), so
  every method works, with the result type and the exceptions it has today.
  mypy sees `tuple[bool, bytes | None, int | None]` above. There is no second
  set of methods, and nothing to add to it when a command is added.
- **No new concepts for concurrency.** Commands are pipelined when they run at
  the same time, with `asyncio.gather()`, a `TaskGroup` or anything else. The
  one new name is `pipeline()`.
- **Opt-in.** Outside the block, commands take and return a pooled connection
  as they do now.
- **Speed.** 1,000 `set()` commands take about 14 ms instead of 70 ms on
  localhost, and 12 ms instead of 530 ms over a link with a 1.1 ms round trip.
  A loop written for one command, as `multi_set()` in [#531] is, is faster
  still. [Details below](#performance).

What it costs: each command had to be split into the bytes to send and a
function that reads its reply, as predicted in [#531]. That is +163/−90 lines
in `client.py`, mostly code that moved. The pipeline itself is another 63
lines there and the 162 of the new `pipeline.py`. Two behaviours change
outside pipelines, both of which
[need a decision](#changes-outside-pipelines).

## Why bother

`get` takes many keys, but no other command of the text protocol does. Sending
several of any other command means several requests, and today each request
waits for its own reply while holding a pooled connection. So with the default
`pool_size=2`, `asyncio.gather()` over 1,000 `set()` calls is 500 round trips
on each of two connections. On localhost that is about 70 µs per command,
nearly all of it the cost of a round trip (a write, a wake-up, a read) rather
than memcached's work. Over a network it is the latency, 500 times.

memcached answers requests in the order it receives them, so a client may send
many before reading any reply. [#531] does that for `set`. This proposal does it
for the connection, so that it works for every command.

## Using it

Commands are pipelined when they are in flight together. Called one at a time,
they still work, but each waits for its reply as usual:

```python
async with client.pipeline() as pipe:
    await pipe.set(b"a", b"1")   # One round trip.
    await pipe.get(b"a")         # Another.
    await asyncio.gather(pipe.get(b"a"), pipe.get(b"b"))  # One for both.
```

The request in [#436], with the outcome of every item:

```python
async with client.pipeline() as pipe:
    results = await asyncio.gather(
        *(pipe.set(key, value) for key, value in items.items()),
        return_exceptions=True,
    )
failed = {key: r for key, r in zip(items, results) if isinstance(r, Exception)}
```

An item that memcached rejects, as too large for instance, doesn't stop the
others: `failed` holds it, with `SERVER_ERROR object too large for cache` as
the reason. If the connection is lost part way, `failed` also holds every item
after that, as nobody knows whether they were stored. `examples/pipeline.py`
runs this.

aiocache's `multi_set()` for memcached is already a `gather()` of
`client.set()` calls, so it would need only the `async with` around it.

## Behaviour

Each point has a test in `tests/pipeline_test.py`.

| | |
|---|---|
| **Connection** | Entering the block takes one connection from the pool, waiting up to `timeout` for it. The block holds it until it ends, so it counts towards `pool_size` like a command in progress does. |
| **Order** | Requests are sent in the order the commands start, which for `gather()` and `TaskGroup` is the order they are given in, and memcached carries them out in that order. (A `set_flag_handler` that waits for something delays its own command.) |
| **Results** | A command returns what it returns outside a pipeline, once its own reply is read. |
| **Invalid arguments** | `ValidationException` is raised before anything is sent, as now. Nothing else is affected. |
| **Error replies** | A command that memcached answers with an error raises `ClientException`, as now. If the next reply can be read after that error, the other commands are not affected and the connection is kept. [A rule](#changes-outside-pipelines) decides that: it is so for an item that is too large, and for a command without data that memcached rejects, such as an `incr` of text. Any other error is treated as an unreadable reply. |
| **Unreadable reply, lost connection** | The command whose reply it was raises what it raises outside a pipeline. The replies after it can't be told apart any more, so every command still waiting raises `ClientException("Pipeline broken by an earlier command")`, as does any command called later in the block. The connection is dropped at once, with the requests it had not sent yet. |
| **Timeout** | `timeout` applies to each command, from when it is called until its reply is read. That includes the time taken by the replies before it. A command that times out raises `TimeoutError`. Its reply is read when it arrives and thrown away. The other commands are not affected. |
| **Cancellation** | The same: the command stops waiting, and its reply is thrown away when it arrives. Once a command is called, its request is sent even if it is cancelled or times out, so it may be carried out. |
| **Leaving the block** | If no reply is owed, the connection goes back to the pool. Otherwise the commands still waiting raise `ClientException("Pipeline closed before the reply was read")` and the connection is closed, dropping the requests not sent yet. Afterwards, `pipe` raises `ClientException("Pipeline is closed")`. |
| **Memory** | Commands take turns to wait for room in the write buffer, so it holds about one request more than its usual 64 KiB, however many commands are called at once. Replies are read as they arrive. |
| **Flag handlers** | Work as usual. Both run in the task of the command, not while the connection is being read. |
| **Nesting** | `pipe.pipeline()` yields `pipe`, so code that is handed either kind of client can ask for a pipeline. |
| **Custom commands** | A subclass command built on `_execute()` is pipelined. One that uses `_connection()` gets the connection to itself once no reply is owed, and holds up the commands after it until it is done. It can't call another command of `pipe` inside that block, which would wait for itself. |

It is not a transaction. Other clients' commands can run between those of a
pipeline, and a failure part way leaves the earlier commands done.

Three things are worth saying in the documentation:

- The block holds a pool connection, so it should not be kept open across
  unrelated work.
- Every command in flight is a task. For a very large batch, sending it in
  chunks of a few thousand keeps memory and the garbage collector's work down.
- `timeout` starts when a command is called, so the last commands of a batch
  that takes longer than `timeout` to send and answer will time out.

## How it works

### Commands are a request and a reply reader

Today a command writes its request and reads the reply inside one block that
holds the connection. That can't be pipelined, because something else has to
read the reply when its turn comes. So each command now hands one helper the
bytes to send and a function that reads the reply:

```python
async def _execute(self, chunks: Sequence[bytes], read_reply: _ReplyReader[_R]) -> _R:
```

`read_reply` takes the `StreamReader`, reads exactly one reply and returns the
command's result. There are three: `_read_line` for the one-line replies (it is
given the command's `parse` function), `_read_values` for `get`/`gets` and
`_read_stats`. They are the code that was in the commands, moved to functions.
For example:

```python
async def delete(self, key: bytes) -> bool:
    self._validate_key(key)

    def parse(response: bytearray) -> bool:
        if response not in (const.DELETED, const.NOT_FOUND):
            raise ClientException("Memcached delete failed", response)
        return response == const.DELETED

    command = b'delete ' + key + b'\r\n'
    return await self._execute_simple_command(command, parse=parse)
```

Outside a pipeline, `_execute()` does what the commands did: take a connection,
write, drain, call `read_reply` on it, release.

A reader says whether the connection can still be used by how it ends. If it
returns, the reply was read in full. If it raises, the connection is discarded,
as it is today when a command raises. For an error reply that it read in full,
it *returns* the `ClientException` and `_execute()` raises it once the
connection is safe.

### A pipelined connection

`Pipeline` (in `pipeline.py`) wraps the one connection:

```
 command tasks                 Pipeline                                   memcached

 set()  ── send(request, read_reply) ──┐
 get()  ── send(request, read_reply) ──┼─► unsent ── end of loop iteration, ──► socket
 incr() ── send(request, read_reply) ──┘      │      or 64 KiB waiting
   ▲                                          ▼
   │                         replies owed: (read_reply, future), ...
   └── future ◄── reader task: await read_reply(stream), oldest first ◄──────── socket
```

- `send()` appends the request to a list of unsent chunks, and a
  `(read_reply, future)` pair to a queue of the replies owed. The command
  awaits the future. A request that isn't bytes is refused before any of it is
  queued, so that it fails alone.
- The unsent chunks are written with one `writelines()` at the end of the
  event loop iteration, so the requests of commands started together leave
  together. Without this, each small request is its own `send()` call and
  packet, which cost more than everything else here put together.
- One task reads the replies: it calls the oldest `read_reply`, sets the
  result on its future, and goes on to the next. It is started when a reply
  becomes owed and ends when none is, so an idle pipeline has no task. If a
  command was cancelled, its reply is still read, and then dropped.
- If a `read_reply` raises, that command gets the exception, and the pipeline
  is aborted: the transport is aborted, as [#530] does for a broken connection,
  and everything still queued fails.

Two details matter for correctness:

- **Waiting for room happens before a request is queued, never after.**
  `send()` holds a lock while it awaits `drain()`, so commands queue up in
  order for room in the write buffer. Nothing that reads replies ever waits for
  a write. memcached stops reading requests while its replies back up, which
  is why [#531] writes and reads in alternating batches. Here the reader task
  never stops reading, so the same deadlock can't happen, and it needs no
  batching. `test_pipeline_large_requests_and_replies` sends 9 MB each way and
  checks the buffer.
- **Releasing the connection is not left to a task.** When the block ends,
  `close()` decides synchronously whether the connection is reusable (no
  reply owed, nobody sending), and the pool gets it back or discards it before
  anything is awaited.

The client that the block yields is `copy.copy(client)` with `_pipeline` set.
It shares the pool, the flag handlers and the timeout, and `_execute()` sends
through the pipeline when that attribute is set.

### Tests

`tests/pipeline_test.py` covers the table above against a real memcached, on
TCP and a unix socket, and against scripted servers for the failures: a server
that answers nothing until every request has arrived (so the test hangs without
pipelining), garbage and hang-ups part way, late replies, a command cancelled
in the middle of others. The suite passes on Python 3.11 to 3.15, with mypy and
flake8 clean, and `pipeline.py` fully covered.

Not on the branch: I also ran about 300,000 pipelined commands of a dozen
kinds against a model of what memcached should answer, with commands cancelled
at random, on each of those Python versions and with eager tasks. And a
separate review pass went looking for races. What it found is fixed and has a
test here.

## Changes outside pipelines

The split changes two things for ordinary commands. Both are improvements, I
think, but they are changes to behaviour that was decided recently, so they
need a yes.

**1. An error reply no longer discards the connection, when the next reply can
still be read.** Today any reply that a command doesn't expect discards its
connection ([#510]). In a pipeline that would mean one item that is too large
fails every command after it, although memcached carried them out. So the
readers pick out the errors after which the stream is known to be in step:

| Reply | `set`, `add`, `cas`, ... | `get`, `gets` | other commands |
|---|---|---|---|
| `SERVER_ERROR object too large for cache` | keep | discard | discard |
| `CLIENT_ERROR ...`, `ERROR` | discard | discard | keep |
| any other `SERVER_ERROR`, anything else unexpected, EOF | discard | discard | discard |

The list is short on purpose. Discarding is what happens now and is always
safe, so only what I could show to be safe is kept:

- **Too large.** memcached reads past the data of the item before it answers,
  and nothing else is affected.
- **`CLIENT_ERROR` to a command with data.** memcached does not read past the
  data, and takes it for the next command.
- **A command without data** is one line, and so is its error.
- **`get` and `gets`** stay as they are, because a `get` that fails makes
  memcached drop every reply which is queued on the connection but not sent
  yet (`process_get_command()`: "Kill any stacked responses we had"). In a
  pipeline those include the replies to earlier commands. One way into that
  code is a key that is too long, which this client refuses to send. `set`,
  `set`, `get <251-byte key>` and `version` sent together are answered with
  `CLIENT_ERROR bad command line format` and `VERSION ...` alone: neither
  `STORED` ever arrives.
- **Any other `SERVER_ERROR`** is about memcached's own state. The other way
  into the code above is a `get` that runs out of memory, which ends with
  `SERVER_ERROR out of memory writing get response`, and that line is then read
  as the reply to the oldest command still waiting. I could not provoke this
  one; it is from reading `proto_text.c` in 1.6.24. Another,
  `out of memory reading request`, is followed by closing the connection.

This is what memcached 1.6.24 answers, with a `version` sent after each
request to show where the stream is:

| Request | Replies |
|---|---|
| `set` of 2 MB | `SERVER_ERROR object too large for cache`, `VERSION ...` |
| `set k 0 abc 5` and its data (the error in [#532]) | `CLIENT_ERROR bad command line format`, `ERROR`, `VERSION ...` |
| `set k 0 0 3` with 7 bytes of data | `CLIENT_ERROR bad data chunk`, `ERROR`, `VERSION ...` |
| `incr` of text | `CLIENT_ERROR cannot increment or decrement non-numeric value`, `VERSION ...` |
| `touch k abc` | `CLIENT_ERROR invalid exptime argument`, `VERSION ...` |
| `bogus`, `stats bogus` | `ERROR`, `VERSION ...` |

`tests/commands_test.py::test_error_reply` checks each cell of the first table
for each command.

The risk that remains is a server that speaks the protocol but answers "too
large" without reading the data: the connection would be kept a line out of
step. A proxy has to read a whole request to forward it, so I don't expect
one to, but I have only checked memcached. If that is too much to assume, that
cell can be "discard" too, which costs pipelines the item that is too large.
Or the rule can apply inside pipelines only, at the price of an `if`.

**2. `get_flag_handler` runs after the reply is read and the connection is
back in the pool.** It can't run in the reader task without holding up every
reply behind it. So `timeout` no longer covers it, and an exception from it
no longer discards the connection. `set_flag_handler` already works this way
since [#525].

Also: `_execute_simple_command()` changes again. It was made a context manager
in [#525], which isn't released. It now takes `parse=` and returns the result.
`get` and `stats` now `drain()` after writing like the other commands.

## Performance

`set()` of 100-byte values unless noted, best of 5 or more runs, Python 3.11,
memcached 1.6.24, both on one 4-core VM. Runs on it differ by up to a fifth,
so the figures are rounded. `python docs/pipelining_bench.py` reproduces them,
and `--delay-ms` adds the proxy. The `multi_set()` column is [#531], run with
the same script.

Localhost:

| Items | loop of `set()` | `gather()` of `set()` | **`pipeline()`** | `multi_set()` ([#531]) |
|---|---|---|---|---|
| 10 | 0.8 ms | 0.7 ms | **0.3 ms** | 0.1 ms |
| 100 | 8 ms | 7 ms | **1.6 ms** | 0.35 ms |
| 1,000 | 85 ms | 70 ms | **14 ms** | 2 ms |
| 10,000 | 700 ms | 700 ms | **210 ms** | 22 ms |
| 1,000 × 10 KB | 80 ms | 62 ms | **22 ms** | 7 ms |

Through a proxy that delays each direction, for a round trip of about 1.1 ms:

| Items | loop of `set()` | `gather()` of `set()` | **`pipeline()`** | `multi_set()` ([#531]) |
|---|---|---|---|---|
| 10 | 12 ms | 6 ms | **1.5 ms** | 1.2 ms |
| 100 | 114 ms | 52 ms | **2.3 ms** | 1.7 ms |
| 1,000 | 1,120 ms | 530 ms | **12 ms** | 4.3 ms |
| 10,000 | 11,300 ms | 5,500 ms | **240 ms** | 46 ms |
| 1,000 × 10 KB | 1,150 ms | 570 ms | **50 ms** | 34 ms |

So on localhost it is 3 to 5 times faster than `gather()` today. Over a
network the round trips go, and it is 20 to 40 times faster from 100 commands
up.

Ordinary commands cost the same as on master, within the noise of this
machine (about 85 µs for a `get` on its own).

### Where the rest goes

A pipelined command costs about 14 µs here, against 2 µs per item for the loop
in [#531]. Roughly:

- 4–6 µs is the task that `gather()` makes for each command. That is the price
  of using the ordinary methods, and no design that does so avoids it. At
  10,000 tasks the garbage collector adds to it, which is why that row costs
  more per command.
- About 5 µs is the pipeline: queueing the request, and reading and matching
  the reply.
- The rest is the command's own code: validation, formatting,
  `asyncio.timeout()`. With a `timeout` set, the timer adds about 3 µs, as it
  does to a command outside a pipeline.

The pipeline itself doesn't need a task per command. A `multi_set()` written on
`Pipeline.send()`, which is about 15 lines, stores 1,000 items in about 5 ms
and 10,000 in about 50 ms. So if a bulk helper is still wanted after this, it
can sit on these internals and need none of the batching and back-pressure
code of [#531].

## Alternatives considered

**`multi_set()` alone ([#531]).** The fastest way to send sets, as above. But it
is one command: the next request is `multi_delete()` or `multi_incr()`, each
with its own copy of the batching. It can come back later as a small helper.

**A pipeline object that queues commands, with `execute()`** (as in redis-py):
`pipe.set(...); pipe.get(...); results = await pipe.execute()`. No task per
command, so it would be about as fast as the helper above. But it is a second
copy of every method's signature to keep in step; the results are a
`list[Any]`, or each queueing method needs its own handle type; `set()` can't
be queued by a plain method, as the flag handler is awaited; and a subclass's
commands are left out.

**Each command reads its own reply, in turn.** This was my first prototype,
because it leaves the commands alone: they keep their write-then-read code,
and the connection they are given makes them wait for their turn to read. It
works, and the existing tests passed. But a command that is cancelled or times
out leaves its reply unread, so every command after it must fail. It costs
half as much again per command (20–26 µs with the same batching of writes), as
each reply is a hand-off from one task to the next. And a helper without tasks
can't use it, since nothing reads replies while it writes. The reader task is
what fixes all three, and it is what needs the commands split.

**Pipelining every command, with no block.** The pool would hand each command
a shared pipelined connection instead of an exclusive one, and
`gather(client.set(...), ...)` would be pipelined as it stands, in aiocache
too. The internals here are what it would be built on, so it stays possible.
But it changes behaviour for everyone: an unreadable reply then fails the
commands of unrelated callers on that connection, a slow reply holds up
callers behind it, a timeout no longer frees a stuck connection by itself, and
`pool_size` means something else. I would rather see the opt-in block used for
a while first.

**Doing it with a context variable**, so that `client` itself pipelines inside
the block and no second object is needed. Tasks started in the block inherit
it and tasks started before don't, which is a surprising thing to have to
know.

**`noreply`.** Fast, but it hides every failure, and an error reply that does
come back is read as the answer to something else.

**The meta protocol** has quiet mode and opaque tokens for exactly this. It
would be a new protocol implementation, and isn't needed while replies come in
order.

## Open questions

1. **The shape.** Is `async with client.pipeline() as pipe`, yielding a client,
   what you had in mind?
2. **The two changes for ordinary commands**, above. Apply them everywhere, as
   in the prototype, or the first one inside pipelines only?
3. **Timeouts.** In the prototype a timeout fails only its command. [#530] made
   sure that a `set()` which timed out before it was fully sent is never
   carried out. In a pipeline that holds only when the block ends soon after,
   as it does when the `TimeoutError` leaves `gather()`. To hold always, a
   timeout would have to abort the pipeline, failing the commands in flight
   with it. A middle way is to abort only if the request that timed out had
   not been fully written, which is about 10 lines. I would start simple.
4. **Leaving the block with commands in flight** fails them and drops the
   connection. It could wait for them instead. I think that would hide a bug
   in the caller more often than help, and with `timeout=None` and a dead
   server it would wait for ever.
5. **Exceptions.** "Pipeline broken by an earlier command" and "Pipeline is
   closed" are `ClientException`. A subclass would let a caller tell "memcached
   refused this" from "this may or may not have been done".
6. **`pipe.close()`** does what `client.close()` does, as they share the pool.
   It could do nothing, or raise.
7. **[#531].** Close it in favour of this, or keep `multi_set()` as a helper on
   top? With `pipeline()` it is three lines for the caller, at about 7 times
   the CPU per item of [#531], or under 3 times that of a helper on these
   internals.

## On this branch

Four commits. The tests, mypy and flake8 pass at each.

1. **This document**, and `docs/pipelining_bench.py`.
2. **Split commands into a request and a reply reader.** `_execute()`, the
   three readers and the rule for error replies in `client.py`, and their
   tests in `tests/commands_test.py`: `test_error_reply` and
   `test_get_flag_handler_runs_after_reply_is_read`, while
   `test_unexpected_reply_discards_connection` now uses a reply that isn't an
   error. There is no pipeline yet, and nothing here depends on one.
3. **Add `pipeline()`.** `pipeline.py` and the 63 lines in `client.py`.
   `tests/pool_test.py` gains a line, for a test client that skips `__init__`.
4. **Test and document `pipeline()`.** `tests/pipeline_test.py`, and
   `README.rst`, `CHANGES.rst` and `examples/pipeline.py` as they would be in a
   PR.

If the direction is right, 2 can be a PR, and 3 with 4 another. The rule for
error replies can be taken out of 2 into its own: it is
`_is_recoverable_error()` and the two readers that call it.

[#436]: https://github.com/aio-libs/aiomcache/issues/436
[#510]: https://github.com/aio-libs/aiomcache/pull/510
[#525]: https://github.com/aio-libs/aiomcache/pull/525
[#530]: https://github.com/aio-libs/aiomcache/pull/530
[#531]: https://github.com/aio-libs/aiomcache/pull/531
[#532]: https://github.com/aio-libs/aiomcache/pull/532
