"""memcached client, based on mixpanel's memcache_client library

Usage example::

    import aiomcache
    mc = aiomcache.Client("127.0.0.1", 11211, timeout=1, connect_timeout=5)
    await mc.set("some_key", "Some value")
    value = await mc.get("some_key")
    await mc.delete("another_key")
"""

from .client import Client
from .exceptions import ClientException, ValidationException

__all__ = ('Client', 'ClientException', 'ValidationException')

__version__ = "0.7.0rc0"
