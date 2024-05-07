"""memcached client, based on mixpanel's memcache_client library

Usage example::

    import aiomcache
    mc = aiomcache.Client("127.0.0.1", 11211)
    await mc.set("some_key", "Some value")
    value = await mc.get("some_key")
    await mc.delete("another_key")
"""

from .client import Client, FlagClient
from .exceptions import ClientException, ValidationException

__all__ = ("Client", "ClientException", "FlagClient", "ValidationException")

__version__ = "0.8.2"
