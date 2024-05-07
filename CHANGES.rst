=======
CHANGES
=======

.. towncrier release notes start

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
