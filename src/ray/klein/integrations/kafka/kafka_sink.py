# SPDX-License-Identifier: Apache-2.0
"""Checkpoint-aligned Kafka sink for Klein streaming jobs."""

from __future__ import annotations

import json
import time
from enum import Enum
from typing import Any

from confluent_kafka import KafkaException, Producer

from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Histogram

logger = get_logger(__name__)

_BUFFER_FULL_POLL_TIMEOUT_SECONDS = 10.0
_FLUSH_TIMEOUT_SECONDS = 30.0


class _SerializerFormat(str, Enum):
    JSON = "json"
    STRING = "string"
    BYTES = "bytes"


def _serializer_format(value: str, parameter: str) -> _SerializerFormat:
    try:
        return _SerializerFormat(value)
    except ValueError as error:
        allowed = [serializer.value for serializer in _SerializerFormat]
        raise ValueError(f"{parameter} must be one of {allowed}, got {value!r}") from error


def _serialize(value: Any, serializer: _SerializerFormat) -> bytes:
    if serializer is _SerializerFormat.JSON:
        return json.dumps(value).encode("utf-8")
    if serializer is _SerializerFormat.STRING:
        return str(value).encode("utf-8")
    return value if isinstance(value, bytes) else str(value).encode("utf-8")


class KafkaSink(SinkFunction):
    """Produce streaming records to Kafka and flush them at checkpoints.

    A successful flush means every record before the aligned checkpoint has a
    delivery acknowledgement from Kafka. Replaying an unacknowledged input can
    produce duplicates, so the sink provides at-least-once rather than exactly-once
    delivery.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        key_field: str | None = None,
        key_serializer: str = "string",
        value_serializer: str = "json",
        producer_config: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("topic must be a non-empty string")
        if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must be a non-empty string")
        if key_field is not None and (not isinstance(key_field, str) or not key_field.strip()):
            raise ValueError("key_field must be a non-empty string or None")
        if producer_config is not None and not isinstance(producer_config, dict):
            raise TypeError("producer_config must be a dict or None")

        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._key_field = key_field
        self._key_serializer = _serializer_format(key_serializer, "key_serializer")
        self._value_serializer = _serializer_format(value_serializer, "value_serializer")
        self._producer_config = dict(producer_config or {})
        if self._producer_config.pop("bootstrap.servers", None) is not None:
            logger.warning("Ignoring 'bootstrap.servers' from producer_config; use bootstrap_servers instead.")

        self._producer: Producer | None = None
        self._first_delivery_error: BaseException | None = None
        self._delivery_failure_count = 0
        self._error_metric: Counter | None = None
        self._flush_duration_metric: Histogram | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        if self._producer is not None:
            return
        self._first_delivery_error = None
        self._delivery_failure_count = 0
        self._producer = Producer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                **self._producer_config,
            }
        )
        metric_group = runtime_context.metric_group
        if metric_group is not None:
            self._error_metric = metric_group.builtin_counter(KleinMetrics.KAFKA_ERRORS)
            self._flush_duration_metric = metric_group.builtin_histogram(KleinMetrics.KAFKA_FLUSH_DURATION_MS)
        logger.info("Opened Kafka sink for topic %s on subtask %s", self._topic, runtime_context.task_index)

    def write(self, value: dict[str, Any]) -> None:
        producer = self._require_producer()
        self._raise_delivery_error()
        key = self._serialize_key(value)
        payload = _serialize(value, self._value_serializer)

        try:
            self._produce(producer, payload, key)
        except BufferError:
            producer.poll(_BUFFER_FULL_POLL_TIMEOUT_SECONDS)
            self._raise_delivery_error()
            try:
                self._produce(producer, payload, key)
            except BufferError as error:
                self._record_error()
                raise RuntimeError(
                    f"Kafka producer queue for topic {self._topic!r} remained full after "
                    f"{_BUFFER_FULL_POLL_TIMEOUT_SECONDS:g}s"
                ) from error

        producer.poll(0)
        self._raise_delivery_error()

    def flush(self) -> None:
        producer = self._producer
        if producer is None:
            return
        started_at = time.monotonic()
        remaining = producer.flush(timeout=_FLUSH_TIMEOUT_SECONDS)
        if self._flush_duration_metric is not None:
            self._flush_duration_metric.observe_elapsed(started_at)
        self._raise_delivery_error()
        if remaining:
            self._record_error()
            raise RuntimeError(
                f"Kafka producer still had {remaining} message(s) queued after flushing "
                f"topic {self._topic!r} for {_FLUSH_TIMEOUT_SECONDS:g}s"
            )

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._producer = None

    def _serialize_key(self, value: dict[str, Any]) -> bytes | None:
        if self._key_field is None:
            return None
        key = value.get(self._key_field)
        return None if key is None else _serialize(key, self._key_serializer)

    def _produce(self, producer: Producer, value: bytes, key: bytes | None) -> None:
        producer.produce(
            self._topic,
            value=value,
            key=key,
            on_delivery=self._on_delivery,
        )

    def _on_delivery(self, error: Any, _message: Any) -> None:
        if error is None:
            return
        self._delivery_failure_count += 1
        if self._error_metric is not None:
            self._error_metric.inc()
        if self._first_delivery_error is None:
            self._first_delivery_error = error if isinstance(error, BaseException) else KafkaException(error)

    def _raise_delivery_error(self) -> None:
        if self._first_delivery_error is None:
            return
        raise RuntimeError(
            f"Kafka failed to deliver {self._delivery_failure_count} message(s) to topic {self._topic!r}"
        ) from self._first_delivery_error

    def _record_error(self) -> None:
        if self._error_metric is not None:
            self._error_metric.inc()

    def _require_producer(self) -> Producer:
        if self._producer is None:
            raise RuntimeError("Kafka sink must be opened before write()")
        return self._producer

    def __repr__(self) -> str:
        return f"KafkaSink(topic={self._topic!r}, key_field={self._key_field!r})"
