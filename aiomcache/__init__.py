"""memcached client, based on mixpanel's memcache_client library

Usage example::

    import aiomcache
    mc = aiomcache.Client("127.0.0.1", 11211, timeout=1, connect_timeout=5)
    yield from mc.set("some_key", "Some value")
    value = yield from mc.get("some_key")
    yield from mc.delete("another_key")
"""

from .client import *
from .exceptions import *

__all__ = (client.__all__ +
           exceptions.__all__
)
