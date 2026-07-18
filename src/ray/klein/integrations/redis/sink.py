# SPDX-License-Identifier: Apache-2.0
"""Buffered, transactional Redis sink."""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

import redis
from redis import RedisError

from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.integrations.redis.redis_connection_config import RedisConnectionConfig
from ray.klein.integrations.redis.redis_sink_config import RedisSinkConfig
from ray.klein.integrations.redis.writer import KeyExtractor, RedisWriter, ValueExtractor
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Histogram

logger = get_logger(__name__)

Record = dict[str, Any]


class RedisSink(SinkFunction):
    """Buffer records and replace Redis values in transactional batches."""

    def __init__(
        self,
        connection: RedisConnectionConfig,
        key: KeyExtractor,
        value: ValueExtractor,
        *,
        config: RedisSinkConfig | None = None,
    ) -> None:
        if not isinstance(connection, RedisConnectionConfig):
            raise TypeError("connection must be a RedisConnectionConfig")
        self._connection = connection
        self._config = RedisSinkConfig() if config is None else config
        if not isinstance(self._config, RedisSinkConfig):
            raise TypeError("config must be a RedisSinkConfig")

        self._writer = RedisWriter(
            self._config.data_type,
            key,
            value,
            key_prefix=self._config.key_prefix,
            delimiter=self._config.delimiter,
            ttl=self._config.ttl,
        )
        self._buffer: deque[Record] = deque()
        self._inflight = 0
        self._buffer_changed = threading.Condition()
        self._flush_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_thread: threading.Thread | None = None
        self._background_error: Exception | None = None
        self._last_flush_at = time.monotonic()
        self._pool: redis.ConnectionPool | None = None

        self._flush_duration_metric: Histogram | None = None
        self._flush_batch_size_metric: Histogram | None = None
        self._failure_metric: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        if self._flush_thread is not None and self._flush_thread.is_alive():
            return

        self._replace_pool()
        metric_group = runtime_context.metric_group
        if metric_group is not None:
            self._flush_duration_metric = metric_group.builtin_histogram(KleinMetrics.REDIS_FLUSH_DURATION_MS)
            self._flush_batch_size_metric = metric_group.builtin_histogram(KleinMetrics.REDIS_FLUSH_BATCH_RECORDS)
            self._failure_metric = metric_group.builtin_counter(KleinMetrics.REDIS_FAILURES)

        self._last_flush_at = time.monotonic()
        self._background_error = None
        self._stop_event.clear()
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            name=f"klein-redis-sink-{runtime_context.task_index}",
            daemon=True,
        )
        self._flush_thread.start()
        logger.info("Opened Redis sink for subtask %s", runtime_context.task_index)

    def write(self, value: Record) -> None:
        if self._pool is None:
            raise RuntimeError("Redis sink must be opened before write()")

        with self._buffer_changed:
            while len(self._buffer) + self._inflight >= self._config.buffer_capacity:
                self._raise_background_error()
                if self._stop_event.is_set():
                    raise RuntimeError("Redis sink is closed")
                self._buffer_changed.wait()
            self._raise_background_error()
            self._buffer.append(value)
            self._buffer_changed.notify_all()

    def flush(self) -> None:
        self._raise_background_error()
        if self._pool is None:
            with self._buffer_changed:
                if self._buffer:
                    raise RuntimeError("Redis sink must be opened before flush()")
            return
        self._flush_available(force=True)
        self._raise_background_error()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._stop_event.set()
            with self._buffer_changed:
                self._buffer_changed.notify_all()

            thread, self._flush_thread = self._flush_thread, None
            if thread is not None and thread is not threading.current_thread():
                thread.join()

            pool, self._pool = self._pool, None
            if pool is not None:
                pool.disconnect()

    def _replace_pool(self) -> None:
        # Batch retries are coordinated by the sink. Disabling command-level
        # retries prevents an accidental retry-count multiplication.
        replacement = self._connection.create_pool(retries=0)
        previous, self._pool = self._pool, replacement
        if previous is not None:
            previous.disconnect()

    def _flush_loop(self) -> None:
        try:
            while True:
                with self._buffer_changed:
                    while not self._stop_event.is_set():
                        if len(self._buffer) >= self._config.batch_size:
                            break
                        if not self._buffer:
                            self._buffer_changed.wait()
                            continue
                        remaining = self._config.flush_interval.total_seconds() - (
                            time.monotonic() - self._last_flush_at
                        )
                        if remaining <= 0:
                            break
                        self._buffer_changed.wait(remaining)
                    if self._stop_event.is_set():
                        return
                self._flush_available(force=False)
        except Exception as error:
            with self._buffer_changed:
                self._background_error = error
                self._stop_event.set()
                self._buffer_changed.notify_all()
            logger.exception("Redis sink background flush failed")

    def _flush_available(self, *, force: bool) -> None:
        with self._flush_lock:
            while self._should_flush(force):
                self._write_with_retries()
                self._last_flush_at = time.monotonic()

    def _should_flush(self, force: bool) -> bool:
        with self._buffer_changed:
            size = len(self._buffer)
        return size > 0 and (
            force
            or size >= self._config.batch_size
            or time.monotonic() - self._last_flush_at >= self._config.flush_interval.total_seconds()
        )

    def _write_with_retries(self) -> None:
        for attempt in range(self._connection.max_retries + 1):
            try:
                self._write_batch()
                return
            except (RedisError, OSError) as error:
                if self._failure_metric is not None:
                    self._failure_metric.inc()
                if attempt >= self._connection.max_retries:
                    raise

                delay = min(
                    self._connection.max_retry_delay.total_seconds(),
                    0.1 * 2**attempt,
                )
                logger.warning(
                    "Redis batch write failed; retrying in %.2fs (%d/%d): %s",
                    delay,
                    attempt + 1,
                    self._connection.max_retries,
                    error,
                )
                if self._stop_event.wait(delay):
                    raise RuntimeError("Redis sink closed while retrying") from error
                self._replace_pool()

    def _take_batch(self) -> list[Record]:
        with self._buffer_changed:
            size = min(len(self._buffer), self._config.batch_size)
            batch = [self._buffer.popleft() for _ in range(size)]
            self._inflight = len(batch)
            return batch

    def _restore_batch(self, batch: list[Record]) -> None:
        with self._buffer_changed:
            self._buffer.extendleft(reversed(batch))
            self._inflight = 0
            self._buffer_changed.notify_all()

    def _complete_batch(self) -> None:
        with self._buffer_changed:
            self._inflight = 0
            self._buffer_changed.notify_all()

    def _raise_background_error(self) -> None:
        if self._background_error is not None:
            raise RuntimeError("Redis sink background flush failed") from self._background_error

    def _write_batch(self) -> None:
        batch = self._take_batch()
        if not batch:
            return

        pool = self._pool
        if pool is None:
            self._restore_batch(batch)
            raise RuntimeError("Redis sink is closed")

        started_at = time.monotonic()
        try:
            with (
                redis.Redis(connection_pool=pool) as client,
                client.pipeline(transaction=True) as pipeline,
            ):
                for record in batch:
                    self._writer.queue(pipeline, record)
                pipeline.execute()
        except Exception:
            self._restore_batch(batch)
            raise

        self._complete_batch()
        if self._flush_duration_metric is not None:
            self._flush_duration_metric.observe_elapsed(started_at)
        if self._flush_batch_size_metric is not None:
            self._flush_batch_size_metric.observe(len(batch))

    def __repr__(self) -> str:
        return f"RedisSink(connection={self._connection!r}, config={self._config!r})"
