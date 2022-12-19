from unittest import mock
from unittest.mock import MagicMock

import pytest
import ssl
from aiomcache import Client
from .conftest import McacheParams


@pytest.mark.asyncio
async def test_ssl_params_forwarded_from_client() -> None:
        client = Client("host", port=11211, ssl=True, ssl_handshake_timeout=20)

        with mock.patch("asyncio.open_connection", return_value=(MagicMock, MagicMock)) as oc:
            await client._pool._create_new_conn()

        oc.assert_called_once_with("host", 11211, ssl=True, ssl_handshake_timeout=20)

@pytest.mark.asyncio
async def test_ssl_client_fails_against_plaintext_server(
    mcache_params: McacheParams
) -> None:
        client = Client(**mcache_params, ssl=True)
        # If SSL was correctly enabled, this should
        # fail, since SSL isn't enabled on the memcache
        # server.
        with pytest.raises(ssl.SSLError):
            await client.get(b"key")
