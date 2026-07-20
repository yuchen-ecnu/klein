# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
import numpy as np
import orjson
import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.config.serve_options import ServeOptions
from ray.klein.runtime.serve_client import EmbeddedProxyClient


def _client(
    *,
    max_attempts: int = 3,
    request_timeout: int = 30,
    connect_timeout: int = 5,
    limit_per_host: int = 2,
    connection_limit: int = 4,
) -> EmbeddedProxyClient:
    config = Configuration()
    config.set(ServeOptions.DEPLOYMENT_NAME, "orders")
    config.set(ServeOptions.PROXY_ENDPOINTS, "http://proxy")
    config.set(ServeOptions.CLIENT_MAX_ATTEMPTS, max_attempts)
    config.set(ServeOptions.RETRY_BACKOFF_MAX, 0.0)
    config.set(ServeOptions.HTTP_TIMEOUT, request_timeout)
    config.set(ServeOptions.HTTP_CONNECT_TIMEOUT, connect_timeout)
    config.set(ServeOptions.HTTP_LIMIT_PER_HOST, limit_per_host)
    config.set(ServeOptions.HTTP_CONNECTION_LIMIT, connection_limit)
    metric_group = Mock()
    metric_group.builtin_histogram.return_value = Mock()
    metric_group.builtin_counter.return_value = Mock()
    return EmbeddedProxyClient(SimpleNamespace(config=config, metric_group=metric_group))


@pytest.mark.asyncio
async def test_session_configures_httpx_for_existing_client_semantics() -> None:
    client = _client()
    fake_session = Mock(spec=httpx.AsyncClient)
    fake_session.is_closed = False

    with patch("ray.klein.runtime.serve_client.httpx.AsyncClient", return_value=fake_session) as factory:
        assert client.session is fake_session

    options = factory.call_args.kwargs
    limits = options["limits"]
    timeout = options["timeout"]
    assert limits.max_connections == 4
    assert limits.max_keepalive_connections == 4
    assert limits.keepalive_expiry == 15.0
    assert timeout.connect == 5
    assert timeout.pool == 5
    assert timeout.read is None
    assert timeout.write is None
    assert options["follow_redirects"] is True
    assert options["max_redirects"] == 10
    assert options["trust_env"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable_status", [429, 499, 503])
async def test_retryable_status_reuses_request_id(retryable_status: int) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        status = retryable_status if len(requests) == 1 else 200
        return httpx.Response(status, json={"ok": status == 200})

    client = _client()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as session:
        client._session = session
        client._session_loop = asyncio.get_running_loop()
        with patch.object(client, "_backoff", new=AsyncMock()) as backoff:
            result = await client.post_request_with_retry({"value": np.array([1, 2])})

    assert result == {"ok": True}
    assert len(requests) == 2
    assert requests[0].headers["X-Request-ID"] == requests[1].headers["X-Request-ID"]
    assert requests[0].headers["Content-Type"] == "application/octet-stream"
    assert orjson.loads(requests[0].content) == {"value": [1, 2]}
    backoff.assert_awaited_once_with(0)


@pytest.mark.asyncio
async def test_non_retryable_client_error_stops_after_one_attempt() -> None:
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, json={"error": "bad request"})

    client = _client(max_attempts=3)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as session:
        client._session = session
        client._session_loop = asyncio.get_running_loop()
        with pytest.raises(RuntimeError) as raised:
            await client.post_request_with_retry({"value": np.array([1])})

    assert calls == 1
    assert isinstance(raised.value.__cause__, httpx.HTTPStatusError)
    assert raised.value.__cause__.response.status_code == 400


@pytest.mark.asyncio
async def test_transport_error_is_retried() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"ok": True})

    client = _client(max_attempts=2)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as session:
        client._session = session
        client._session_loop = asyncio.get_running_loop()
        with patch.object(client, "_backoff", new=AsyncMock()):
            assert await client.post_request_with_retry({"value": np.array([1])}) == {"ok": True}

    assert len(requests) == 2


@pytest.mark.asyncio
async def test_total_timeout_covers_the_whole_attempt() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(1)
        return httpx.Response(200, json={"ok": True})

    client = _client(max_attempts=1)
    client._http_timeout = 0.01
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as session:
        client._session = session
        client._session_loop = asyncio.get_running_loop()
        with pytest.raises(RuntimeError) as raised:
            await client.post_request_with_retry({"value": np.array([1])})

    assert isinstance(raised.value.__cause__, asyncio.TimeoutError)


@pytest.mark.asyncio
async def test_per_host_limit_serializes_requests_to_one_origin() -> None:
    active_requests = 0
    peak_requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active_requests, peak_requests
        active_requests += 1
        peak_requests = max(peak_requests, active_requests)
        await asyncio.sleep(0.01)
        active_requests -= 1
        return httpx.Response(200, json={"ok": True})

    client = _client(limit_per_host=1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as session:
        with patch("ray.klein.runtime.serve_client.httpx.AsyncClient", return_value=session) as factory:
            await asyncio.gather(
                client._post("http://proxy/one", b"{}", "one"),
                client._post("http://proxy/two", b"{}", "two"),
            )

    assert peak_requests == 1
    factory.assert_called_once()


@pytest.mark.asyncio
async def test_per_host_wait_uses_connect_timeout() -> None:
    request_started = asyncio.Event()
    release_request = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        request_started.set()
        await release_request.wait()
        return httpx.Response(200, json={"ok": True})

    client = _client(limit_per_host=1)
    client._http_connect_timeout = 0.01
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as session:
        client._session = session
        client._session_loop = asyncio.get_running_loop()
        first_request = asyncio.create_task(client._post("http://proxy/one", b"{}", "one"))
        await request_started.wait()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await client._post("http://proxy/two", b"{}", "two")
        finally:
            release_request.set()
        await first_request


def test_close_uses_httpx_async_close_and_clears_host_limits() -> None:
    client = _client()
    session = SimpleNamespace(is_closed=False, aclose=AsyncMock())
    client._session = session
    client._session_loop = None
    client._host_semaphore("http://proxy/request")

    client.close()

    session.aclose.assert_awaited_once_with()
    assert client._session is None
    assert client._host_semaphores == {}
