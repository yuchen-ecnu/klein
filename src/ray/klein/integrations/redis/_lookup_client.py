# SPDX-License-Identifier: Apache-2.0
"""Shared Redis lookup lifecycle and metrics."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

import redis

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.integrations.redis.redis_connection_config import RedisConnectionConfig
from ray.klein.integrations.redis.writer import format_redis_key
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Histogram

Record = dict[str, Any]
LookupKeyExtractor = Callable[[Record], Any | Sequence[Any]]


class _RedisLookupClient:
    """Own the connection pool, key formatting, and lookup metrics."""

    def __init__(
        self,
        connection: RedisConnectionConfig,
        key: LookupKeyExtractor,
        *,
        key_prefix: str | None,
        delimiter: str,
        runtime_context: RuntimeContext | None,
    ) -> None:
        if not isinstance(connection, RedisConnectionConfig):
            raise TypeError("connection must be a RedisConnectionConfig")
        if not callable(key):
            raise TypeError("key must be callable")
        if key_prefix is not None and not key_prefix.strip():
            raise ValueError("key_prefix must be None or a non-empty string")
        if not delimiter:
            raise ValueError("delimiter must be non-empty")

        self._key = key
        self._key_prefix = key_prefix
        self._delimiter = delimiter
        self._pool: redis.ConnectionPool | None = connection.create_pool()
        self.runtime_info = runtime_context.runtime_info if runtime_context else RuntimeInfo()

        metric_group = runtime_context.metric_group if runtime_context else None
        self._duration_metric: Histogram | None = None
        self._batch_size_metric: Histogram | None = None
        self._failure_metric: Counter | None = None
        if metric_group is not None:
            self._duration_metric = metric_group.builtin_histogram(KleinMetrics.REDIS_LOOKUP_DURATION_MS)
            self._batch_size_metric = metric_group.builtin_histogram(KleinMetrics.REDIS_LOOKUP_BATCH_RECORDS)
            self._failure_metric = metric_group.builtin_counter(KleinMetrics.REDIS_FAILURES)

    def client(self) -> redis.Redis:
        if self._pool is None:
            raise RuntimeError("Redis lookup is closed")
        return redis.Redis(connection_pool=self._pool)

    def single_key(self, record: Record) -> str:
        return format_redis_key(self._key(record), self._key_prefix, self._delimiter)

    def batch_keys(self, record: Record) -> list[str]:
        return self._format_batch_keys(self._key(record))

    def resolve_keys(self, record: Record) -> str | list[str]:
        """Resolve a row key or a vector of keys from the backend's input shape.

        Ray Data's element-wise ``map`` and ``filter`` lowerings remain row-wise
        even when Klein's streaming operator has batching configured. The
        extracted value is therefore the reliable distinction: a streaming
        batch produces a key vector, while a Ray Data row produces one scalar.
        """

        raw_keys = self._key(record)
        if self.runtime_info.batch_enabled and self._is_key_collection(raw_keys):
            return self._format_batch_keys(raw_keys)
        return format_redis_key(raw_keys, self._key_prefix, self._delimiter)

    @staticmethod
    def _is_key_collection(raw_keys: Any) -> bool:
        return not isinstance(raw_keys, (str, bytes, bytearray, memoryview, Mapping)) and isinstance(raw_keys, Iterable)

    def _format_batch_keys(self, raw_keys: Any) -> list[str]:
        if not self._is_key_collection(raw_keys):
            raise TypeError("Redis batch key extractor must return a non-empty iterable")
        try:
            keys = list(raw_keys)
        except TypeError as error:
            raise TypeError("Redis batch key extractor must return a non-empty iterable") from error
        if not keys:
            raise ValueError("Redis batch key extractor returned an empty sequence")
        return [format_redis_key(key, self._key_prefix, self._delimiter) for key in keys]

    def record_success(self, started_at: float, batch_size: int | None = None) -> None:
        if self._duration_metric is not None:
            self._duration_metric.observe_elapsed(started_at)
        if batch_size is not None and self._batch_size_metric is not None:
            self._batch_size_metric.observe(batch_size)

    def record_failure(self) -> None:
        if self._failure_metric is not None:
            self._failure_metric.inc()

    def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.disconnect()
