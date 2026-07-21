# SPDX-License-Identifier: Apache-2.0
"""Asynchronous client operator for an external Klein Serve deployment."""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from typing import Any
from urllib.parse import urlencode, urlsplit

import httpx
import numpy as np
import orjson

from ray.klein._internal.logging import get_logger
from ray.klein.api.function import Function
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.config.serve_options import ServeOptions
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.serve_serialization import numpy_encoder

logger = get_logger(__name__)
_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 499})


class EmbeddedProxyClient(Function):
    """Forward operator batches to a configured Klein Serve proxy."""

    def __init__(self, runtime_context: RuntimeContext) -> None:
        if runtime_context is None:
            raise TypeError("runtime_context must be a RuntimeContext")
        self.config = runtime_context.config
        self.deployment_name = self.config.get(ServeOptions.DEPLOYMENT_NAME)
        self.route_prefix = self.config.get(ServeOptions.ROUTE_PREFIX)
        self.max_attempts = self._positive(ServeOptions.CLIENT_MAX_ATTEMPTS)
        self.slow_request_warning = self._positive(ServeOptions.CLIENT_SLOW_REQUEST_WARNING)
        self.request_timeout = self._positive(ServeOptions.HTTP_TIMEOUT)
        self.connect_timeout = self._positive(ServeOptions.HTTP_CONNECT_TIMEOUT)
        self.http_limit_per_host = self._non_negative(ServeOptions.HTTP_LIMIT_PER_HOST)
        self.http_connection_limit = self._non_negative(ServeOptions.HTTP_CONNECTION_LIMIT)
        configured_backoff = self.config.get(ServeOptions.RETRY_BACKOFF_MAX)
        if configured_backoff < 0:
            raise ValueError(f"{ServeOptions.RETRY_BACKOFF_MAX.key} must be non-negative")
        self.retry_backoff_max = min(
            configured_backoff,
            10.0,
        )
        if not self.deployment_name:
            raise RuntimeError(
                f"deployment-name is required when ray_serve_enabled=True; set {ServeOptions.DEPLOYMENT_NAME.key}"
            )
        raw_endpoints = self.config.get(ServeOptions.PROXY_ENDPOINTS)
        self.endpoints = [
            endpoint.strip().rstrip("/") for endpoint in (raw_endpoints or "").split(",") if endpoint.strip()
        ]
        if not self.endpoints:
            raise RuntimeError(f"At least one proxy endpoint is required; set {ServeOptions.PROXY_ENDPOINTS.key}")
        self._session: httpx.AsyncClient | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None
        self._host_semaphores: dict[tuple[str, str, int | None], asyncio.Semaphore] = {}
        self.request_duration: Histogram = runtime_context.metric_group.builtin_histogram(
            KleinMetrics.SERVE_REQUEST_DURATION_MS
        )
        self.request_failures: Counter = runtime_context.metric_group.builtin_counter(
            KleinMetrics.SERVE_REQUEST_FAILURES
        )

    @property
    def session(self) -> httpx.AsyncClient:
        if self._session is None or self._session.is_closed:
            self._session_loop = asyncio.get_running_loop()
            self._host_semaphores.clear()
            connection_limit = self.http_connection_limit or None
            connect_timeout = self.connect_timeout or None
            self._session = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=connection_limit,
                    max_keepalive_connections=connection_limit,
                    keepalive_expiry=15.0,
                ),
                timeout=httpx.Timeout(
                    None,
                    connect=connect_timeout,
                    pool=connect_timeout,
                ),
                follow_redirects=True,
                max_redirects=10,
                trust_env=False,
            )
        return self._session

    def _host_semaphore(self, url: str) -> asyncio.Semaphore | None:
        if self.http_limit_per_host == 0:
            return None
        parsed = urlsplit(url)
        key = (parsed.scheme, parsed.hostname or "", parsed.port)
        semaphore = self._host_semaphores.get(key)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self.http_limit_per_host)
            self._host_semaphores[key] = semaphore
        return semaphore

    async def _post(self, url: str, body: bytes, request_id: str) -> httpx.Response:
        session = self.session

        async def send() -> httpx.Response:
            async with session.stream(
                "POST",
                url,
                content=body,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Request-ID": request_id,
                },
            ) as response:
                if response.status_code >= 400:
                    response.raise_for_status()
                await response.aread()
                return response

        semaphore = self._host_semaphore(url)
        if semaphore is None:
            return await send()
        await asyncio.wait_for(
            semaphore.acquire(),
            timeout=self.connect_timeout or None,
        )
        try:
            return await send()
        finally:
            semaphore.release()

    def _positive(self, option) -> Any:
        value = self.config.get(option)
        if isinstance(value, bool) or value <= 0:
            raise ValueError(f"{option.key} must be positive")
        return value

    def _non_negative(self, option) -> Any:
        value = self.config.get(option)
        if isinstance(value, bool) or value < 0:
            raise ValueError(f"{option.key} must be non-negative")
        return value

    async def _backoff(
        self,
        attempt: int,
        *,
        remaining: float | None = None,
        retry_after: float | None = None,
    ) -> None:
        if retry_after is None:
            maximum = min(1.5 ** min(attempt, 64), self.retry_backoff_max)
            delay = random.uniform(0, maximum)
        else:
            delay = retry_after
        if remaining is not None:
            delay = min(delay, max(0.0, remaining))
        if delay > 0:
            await asyncio.sleep(delay)

    async def __call__(self, data: dict[str, np.ndarray]) -> Any:
        started_at = time.monotonic()
        try:
            return await self.post_request_with_retry(data)
        except Exception:
            self.request_failures.inc()
            raise
        finally:
            self.request_duration.observe_elapsed(started_at)

    async def post_request_with_retry(self, payload: dict[str, np.ndarray]) -> Any:
        body = orjson.dumps(payload, default=numpy_encoder)
        started_at = time.monotonic()
        request_id = str(uuid.uuid4())
        last_error: Exception | None = None
        slow_warning_emitted = False
        attempts_made = 0
        last_url: str | None = None
        last_status: int | None = None
        deadline = started_at + self.request_timeout
        deadline_exhausted = False

        for attempt in range(self.max_attempts):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                deadline_exhausted = True
                if last_error is None:
                    last_error = asyncio.TimeoutError(f"logical Serve request exceeded {self.request_timeout}s timeout")
                break
            selected_url = self._request_url()
            last_url = selected_url
            last_status = None
            attempts_made = attempt + 1
            retry_after: float | None = None
            try:
                response = await asyncio.wait_for(
                    self._post(selected_url, body, request_id),
                    timeout=remaining,
                )
                return response.json()
            except httpx.HTTPStatusError as error:
                last_error = error
                last_status = error.response.status_code
                if last_status not in _RETRYABLE_HTTP_STATUSES and not 500 <= last_status < 600:
                    break
                retry_after = self._retry_after_seconds(error)
            except (httpx.TooManyRedirects, httpx.TransportError, asyncio.TimeoutError) as error:
                last_error = error

            slow_warning_emitted = self._warn_if_slow(
                started_at,
                slow_warning_emitted,
                attempt,
                selected_url,
                request_id,
                last_error,
            )
            if attempt + 1 < self.max_attempts:
                await self._backoff(
                    attempt,
                    remaining=deadline - time.monotonic(),
                    retry_after=retry_after,
                )

        elapsed = time.monotonic() - started_at
        deadline_exhausted = deadline_exhausted or elapsed >= self.request_timeout
        raise RuntimeError(
            f"Serve request failed after {attempts_made}/{self.max_attempts} attempts in {elapsed:.1f}s "
            f"(request_id={request_id}, url={last_url}, status={last_status}, "
            f"deadline_exhausted={deadline_exhausted})"
        ) from last_error

    @staticmethod
    def _retry_after_seconds(error: httpx.HTTPStatusError) -> float | None:
        if error.response.status_code != 429:
            return None
        raw_value = error.response.headers.get("Retry-After")
        if raw_value is None:
            return None
        try:
            value = float(raw_value)
        except ValueError:
            return None
        return value if value >= 0 else None

    def _request_url(self) -> str:
        query = urlencode(
            {
                "rayService": self.deployment_name,
                "routePrefix": self.route_prefix,
            }
        )
        return f"{random.choice(self.endpoints)}/api/ray/proxy?{query}"

    def _warn_if_slow(
        self,
        started_at: float,
        already_emitted: bool,
        attempt: int,
        url: str,
        request_id: str,
        error: Exception | None,
    ) -> bool:
        elapsed = time.monotonic() - started_at
        if already_emitted or elapsed < self.slow_request_warning:
            return already_emitted
        logger.warning(
            "Slow proxy request after %.1fs: attempt %s/%s, url=%s, request_id=%s, last_error=%r",
            elapsed,
            attempt + 1,
            self.max_attempts,
            url,
            request_id,
            error,
        )
        return True

    def close(self) -> None:
        session, self._session = self._session, None
        loop, self._session_loop = self._session_loop, None
        self._host_semaphores.clear()
        if session is None or session.is_closed:
            return
        if loop is not None and loop.is_running():
            if loop is self._running_loop():
                loop.create_task(session.aclose())
            else:
                asyncio.run_coroutine_threadsafe(session.aclose(), loop).result(timeout=5)
            return
        asyncio.run(session.aclose())

    @staticmethod
    def _running_loop() -> asyncio.AbstractEventLoop | None:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None
