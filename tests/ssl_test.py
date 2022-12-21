import ssl
import sys
from asyncio import StreamReader, StreamWriter
from unittest import mock

import pytest

from aiomcache import Client
from .conftest import McacheParams


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python3.8")
async def test_ssl_params_forwarded_from_client() -> None:
    client = Client("host", port=11211, ssl=True, ssl_handshake_timeout=20)

    with mock.patch(
        "asyncio.open_connection",
        return_value=(
            mock.create_autospec(StreamReader),
            mock.create_autospec(StreamWriter),
        ),
        autospec=True,
    ) as oc:
        await client._pool.acquire()

    oc.assert_called_with("host", 11211, ssl=True, ssl_handshake_timeout=20)


async def test_ssl_client_fails_against_plaintext_server(
    mcache_params: McacheParams,
) -> None:
    client = Client(**mcache_params, ssl=True)
    # If SSL was correctly enabled, this should
    # fail, since SSL isn't enabled on the memcache
    # server.
    with pytest.raises(ssl.SSLError):
        await client.get(b"key")
