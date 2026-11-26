=======
CHANGES
=======

.. towncrier release notes start

Unreleased
==========
- Add ``pipeline()``, which yields a client that sends its commands on one
  connection without waiting for the replies to earlier ones. Commands called on
  it at the same time, such as with ``asyncio.gather()``, share a round trip.
- Keep the connection after an error reply which the next reply can be read
  after, instead of discarding it: ``SERVER_ERROR object too large for cache``,
  and ``CLIENT_ERROR`` or ``ERROR`` in reply to a command which sends no data.
- Call ``get_flag_handler`` once the whole reply is read and the connection is
  back in the pool. The ``timeout`` option no longer covers it, and it no longer
  discards the connection by raising.
- Drop what an interrupted command still had buffered when its connection is
  discarded, instead of sending it. memcached could carry out a ``set()`` after
  the caller got ``TimeoutError``.
- Limit keys to 250 bytes, as memcached does, instead of 250 characters. Longer
  non-ASCII keys now raise ``ValidationException`` instead of a misleading
  ``ClientException`` from the server.
- Fix ``set()``, ``add()``, ``replace()``, ``cas()`` and ``touch()`` expiring the
  item at once when ``exptime`` is over 30 days. memcached reads that as a Unix
  timestamp, so the time at which it ends is now sent instead.
- Accept a timezone-aware ``datetime`` as ``exptime``, to expire the item at that
  time.
- Deprecate passing a Unix timestamp as ``exptime``, in favour of a ``datetime``.
  An ``int`` which would end after 2038-01-19 03:14:07 UTC, the latest that
  memcached supports, as a number of seconds from now is still sent as a Unix
  timestamp, unless that timestamp is in the past.
- Raise ``ValidationException`` for an ``exptime`` which is a ``bool``, a Unix
  timestamp in the past, or a ``datetime`` or Unix timestamp after 2038-01-19
  03:14:07 UTC. ``touch()`` now also raises it for a negative or non-int
  ``exptime``, as the storage commands do.
- Validate arguments before taking a connection from the pool, so invalid input
  no longer closes a pooled connection.
- Private helpers (``_execute_simple_command()``, ``_multi_get()``,
  ``_storage_command()`` and ``_incr_decr()``) no longer take a ``conn``
  argument, and ``_execute_simple_command()`` now takes a ``parse`` function
  for the reply and returns its result; subclasses that call them need updating.
  The ``@acquire`` decorator is removed; use ``self._execute()``, which takes a
  request and a function to read its reply, or
  ``async with self._connection() as conn:``.
- The ``timeout`` option no longer covers ``set_flag_handler``, which now runs
  before a connection is taken.
- Count connections that are still opening towards ``pool_size``. Concurrent
  commands on an empty pool, or after a connection is discarded, no longer
  open one connection each.
- Open new connections on demand up to ``pool_size`` when ``pool_minsize`` is
  smaller. Before, once ``pool_minsize`` connections were open, a command that
  found them all busy waited for one instead of opening another.

0.8.2 (2024-05-07)
==================
- Fix a static typing error with ``Client.get()``.

0.8.1 (2023-02-10)
==================
- Add ``conn_args`` to ``Client`` to allow TLS and other options when connecting to memcache.

0.8.0 (2022-12-11)
==================
- Add ``FlagClient`` to support memcached flags.
- Fix type annotations for ``@acquire``.
- Fix rare exception caused by memcached server dying in middle of operation.
- Fix get method to not use CAS.

0.7.0 (2022-01-20)
=====================

- Added support for Python 3.10
- Added support for non-ascii keys
- Added type annotations

0.6.0 (2017-12-03)
==================

- Drop python 3.3 support

0.5.2 (2017-05-27)
==================

- Fix issue with pool concurrency and task cancellation

0.5.1 (2017-03-08)
==================

- Added MANIFEST.in

0.5.0 (2017-02-08)
==================

- Added gets and cas commands

0.4.0 (2016-09-26)
==================

- Make max_size strict #14

0.3.0 (2016-03-11)
==================

- Dockerize tests

- Reuse memcached connections in Client Pool #4

- Fix stats parse to compatible more mc class software #5

0.2 (2015-12-15)
================

- Make the library Python 3.5 compatible

0.1 (2014-06-18)
================

- Initial release
