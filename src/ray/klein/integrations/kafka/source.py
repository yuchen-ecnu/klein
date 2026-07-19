# SPDX-License-Identifier: Apache-2.0
"""Checkpoint-aware, unbounded Kafka source backed by confluent-kafka."""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from ray.klein._internal.logging import get_logger
from ray.klein.api.changelog_row import ChangelogRow
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.formats.canal_json import _normalize_canal_json_options, decode_canal_json
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from confluent_kafka import Consumer, TopicPartition

logger = get_logger(__name__)

_OFFSET_INVALID = -1001
_STATE_VERSION = 1
_METADATA_TIMEOUT_SECONDS = 10.0
_FORMAT_STATE_KEY = "value_format"
_INFLIGHT_STATE_KEY = "format_inflight"

PartitionKey = tuple[str, int]
PartitionOffsets = dict[str, dict[int, int | str]]
StartOffset = int | datetime | Literal["earliest", "latest"] | PartitionOffsets
ValueFormat = Literal["raw", "canal-json"]


class _KafkaSourceMetrics:
    def __init__(self, runtime_context: RuntimeContext) -> None:
        group = runtime_context.metric_group
        self.poll_duration_ms: Histogram = group.builtin_histogram(KleinMetrics.KAFKA_POLL_DURATION_MS)
        self.poll_batch_records: Histogram = group.builtin_histogram(KleinMetrics.KAFKA_POLL_BATCH_RECORDS)
        self.assigned_partitions: Gauge = group.builtin_gauge(KleinMetrics.KAFKA_ASSIGNED_PARTITIONS)
        self.consumer_lag_records: Gauge = group.builtin_gauge(KleinMetrics.KAFKA_CONSUMER_LAG_RECORDS)
        self.commits: Counter = group.builtin_counter(KleinMetrics.KAFKA_COMMITS)
        self.commit_duration_ms: Histogram = group.builtin_histogram(KleinMetrics.KAFKA_COMMIT_DURATION_MS)
        self.errors: Counter = group.builtin_counter(KleinMetrics.KAFKA_ERRORS)


class KafkaSource(SourceFunction):
    """Continuously consume Kafka with deterministic partition ownership.

    Records use the same raw-byte schema as ``ray.data.read_kafka``. Each
    subtask manually owns a deterministic subset of the discovered topic
    partitions, so a source restart can seek to the next offsets captured in a
    Klein checkpoint. Kafka group offsets are committed only after the matching
    checkpoint becomes durable.
    """

    def __init__(
        self,
        topics: str | list[str],
        *,
        bootstrap_servers: str | list[str],
        start_offset: StartOffset = "earliest",
        consumer_config: dict[str, Any] | None = None,
        timeout_ms: int | None = None,
        partition_discovery_interval_ms: int = 30_000,
        max_batch_size: int = 1_000,
        value_format: ValueFormat = "raw",
        format_options: dict[str, Any] | None = None,
    ) -> None:
        self._topics = _normalize_nonempty_strings(topics, "topics")
        self._bootstrap_servers = _normalize_nonempty_strings(bootstrap_servers, "bootstrap_servers")
        _validate_start_offset(start_offset, self._topics)
        _validate_positive_integer("timeout_ms", timeout_ms, optional=True)
        _validate_positive_integer("partition_discovery_interval_ms", partition_discovery_interval_ms)
        _validate_positive_integer("max_batch_size", max_batch_size)
        if value_format not in {"raw", "canal-json"}:
            raise ValueError("value_format must be 'raw' or 'canal-json'")
        if value_format == "raw" and format_options:
            raise ValueError("format_options require a non-raw value_format")
        self._start_offset = start_offset
        self._consumer_config = dict(consumer_config or {})
        self._poll_timeout_seconds = (timeout_ms or 1_000) / 1_000.0
        self._discovery_interval_seconds = partition_discovery_interval_ms / 1_000.0
        self._lag_update_interval_seconds = min(10.0, self._discovery_interval_seconds)
        self._max_batch_size = max_batch_size
        self._value_format: ValueFormat = value_format
        self._format_options = _normalize_canal_json_options(format_options) if value_format == "canal-json" else {}

        self._consumer: Consumer | None = None
        self._metrics: _KafkaSourceMetrics | None = None
        self._task_index = 0
        self._parallelism = 1
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._assigned: set[PartitionKey] = set()
        self._positions: dict[PartitionKey, int] = {}
        self._restored_positions: dict[PartitionKey, int] = {}
        self._checkpoint_positions: dict[int, dict[PartitionKey, int]] = {}
        self._completed_checkpoints: deque[int] = deque()
        self._format_inflight: dict[PartitionKey, tuple[int, int]] = {}
        self._next_discovery_at = 0.0
        self._next_lag_update_at = 0.0

    def open(self, runtime_context: RuntimeContext) -> None:
        from confluent_kafka import Consumer

        self._stop_event.clear()
        self._task_index = runtime_context.task_index
        self._parallelism = runtime_context.parallelism
        self._metrics = _KafkaSourceMetrics(runtime_context)
        config = _build_consumer_config(
            self._bootstrap_servers,
            self._consumer_config,
            default_group_id=f"ray-klein-{runtime_context.job_id}",
        )
        self._consumer = Consumer(config)
        self._refresh_assignment(force=True)

    def run(self, context: SourceContext) -> None:
        consumer = self._require_consumer()
        try:
            while not self._stop_event.is_set():
                self._commit_completed_checkpoints()
                self._refresh_assignment()
                if not self._assigned:
                    self._wait_for_assignment(context)
                    continue

                messages = self._poll(consumer)
                if not messages:
                    context.on_idle()
                    self._update_consumer_lag()
                    continue

                emitted = self._emit_messages(context, messages)
                if emitted and self._metrics is not None:
                    self._metrics.poll_batch_records.observe(emitted)
                else:
                    context.on_idle()
                self._update_consumer_lag()
        finally:
            self._commit_completed_checkpoints()
            consumer.close()
            self._consumer = None

    def snapshot_state(self, checkpoint_id: int) -> dict[str, Any]:
        with self._state_lock:
            positions = dict(self._positions)
            self._checkpoint_positions[checkpoint_id] = positions
            inflight = dict(self._format_inflight)
        state = {
            "version": _STATE_VERSION,
            "positions": _encode_positions(positions),
        }
        if self._value_format != "raw":
            state[_FORMAT_STATE_KEY] = self._value_format
            state[_INFLIGHT_STATE_KEY] = _encode_inflight(inflight)
        return state

    def restore_state(self, state: Any) -> None:
        positions = _decode_state(state)
        checkpoint_format = self._value_format if state is None else _decode_value_format(state)
        if checkpoint_format != self._value_format:
            raise ValueError(
                f"Kafka checkpoint value format {checkpoint_format!r} does not match source format {self._value_format!r}"
            )
        inflight = _decode_inflight(state, positions) if checkpoint_format != "raw" else {}
        with self._state_lock:
            self._restored_positions = positions
            self._positions.update(positions)
            self._format_inflight = inflight
        if self._consumer is not None:
            self._refresh_assignment(force=True)

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        with self._state_lock:
            if checkpoint_id in self._checkpoint_positions:
                self._completed_checkpoints.append(checkpoint_id)

    def cancel(self) -> None:
        self._stop_event.set()

    def close(self) -> None:
        """Release the consumer after the source loop has stopped."""

        self.cancel()
        consumer = self._consumer
        if consumer is not None:
            consumer.close()
            self._consumer = None

    def _wait_for_assignment(self, context: SourceContext) -> None:
        context.on_idle()
        self._stop_event.wait(min(self._poll_timeout_seconds, 1.0))

    def _poll(self, consumer: Consumer) -> list:
        started_at = time.monotonic()
        try:
            return (
                consumer.consume(
                    num_messages=self._max_batch_size,
                    timeout=self._poll_timeout_seconds,
                )
                or []
            )
        except Exception:
            self._record_error()
            raise
        finally:
            if self._metrics is not None:
                self._metrics.poll_duration_ms.observe_elapsed(started_at)

    def _emit_messages(self, context: SourceContext, messages: list) -> int:
        if self._value_format == "canal-json":
            return self._emit_canal_json_messages(context, messages)
        if getattr(context, "batch_collect_supported", False):
            records = []
            for message in messages:
                record = self._message_record(message)
                if record is not None:
                    records.append(record)
                if self._stop_event.is_set():
                    break
            context.collect_many(records)
            return len(records)
        emitted = 0
        for message in messages:
            record = self._message_record(message)
            if record is None:
                continue
            context.collect(record)
            emitted += 1
            if self._stop_event.is_set():
                break
        return emitted

    def _emit_canal_json_messages(self, context: SourceContext, messages: list) -> int:
        emitted = 0
        for message in messages:
            if not self._check_message(message):
                continue
            key = (message.topic(), message.partition())
            offset = message.offset()
            rows = self._decode_canal_json_rows(message, key, offset)
            start_index = self._resume_format_index(key, offset, len(rows))
            if not rows:
                self._finish_formatted_message(key, offset)
                continue

            for index in range(start_index, len(rows)):
                self._prepare_formatted_collect(key, offset, index, len(rows))
                context.collect(rows[index])
                emitted += 1
                if self._stop_event.is_set():
                    break
            if self._stop_event.is_set():
                break
        return emitted

    def _decode_canal_json_rows(self, message, key: PartitionKey, offset: int) -> list[ChangelogRow]:
        try:
            rows = decode_canal_json(message.value(), **self._format_options)
        except (TypeError, ValueError) as error:
            self._record_error()
            raise ValueError(f"Unable to decode canal-json at {key[0]}[{key[1]}] offset {offset}: {error}") from error
        if not self._format_options["include_metadata"]:
            return rows
        return [
            ChangelogRow(
                {
                    **row,
                    "__canal_kafka_topic": message.topic(),
                    "__canal_kafka_partition": message.partition(),
                    "__canal_kafka_offset": message.offset(),
                },
                row_kind=row.row_kind,
            )
            for row in rows
        ]

    def _resume_format_index(self, key: PartitionKey, offset: int, row_count: int) -> int:
        with self._state_lock:
            cursor = self._format_inflight.get(key)
        if cursor is None:
            return 0
        expected_offset, next_index = cursor
        if offset != expected_offset:
            raise RuntimeError(
                f"Formatted Kafka checkpoint expected {key[0]}[{key[1]}] offset {expected_offset}, received {offset}"
            )
        if next_index < 0 or next_index >= row_count:
            raise ValueError("Formatted Kafka checkpoint row cursor is outside the decoded message")
        return next_index

    def _prepare_formatted_collect(self, key: PartitionKey, offset: int, index: int, row_count: int) -> None:
        # Advance before collect(): collect may synchronously take a checkpoint.
        with self._state_lock:
            if index + 1 == row_count:
                self._positions[key] = offset + 1
                self._format_inflight.pop(key, None)
            else:
                self._positions[key] = offset
                self._format_inflight[key] = (offset, index + 1)

    def _finish_formatted_message(self, key: PartitionKey, offset: int) -> None:
        with self._state_lock:
            self._positions[key] = offset + 1
            self._format_inflight.pop(key, None)

    def _message_record(self, message) -> dict[str, Any] | None:
        if not self._check_message(message):
            return None

        key = (message.topic(), message.partition())
        # Advance before collect(): collect may synchronously emit a barrier,
        # whose snapshot must include this record.
        with self._state_lock:
            self._positions[key] = message.offset() + 1
        timestamp_type, timestamp_ms = message.timestamp()
        return {
            "offset": message.offset(),
            "key": message.key(),
            "value": message.value(),
            "topic": message.topic(),
            "partition": message.partition(),
            "timestamp": timestamp_ms,
            "timestamp_type": timestamp_type,
            "headers": dict(message.headers() or []),
        }

    def _check_message(self, message) -> bool:
        """Raise for Kafka errors and return false for partition EOF markers."""

        from confluent_kafka import KafkaError, KafkaException

        error = message.error()
        if error is None:
            return True
        if error.code() == KafkaError._PARTITION_EOF:
            return False
        self._record_error()
        raise KafkaException(error)

    def _refresh_assignment(self, *, force: bool = False) -> None:
        from confluent_kafka import TopicPartition

        consumer = self._require_consumer()
        now = time.monotonic()
        if not force and now < self._next_discovery_at:
            return
        self._next_discovery_at = now + self._discovery_interval_seconds

        all_partitions = self._discover_partitions()
        selected = {key for index, key in enumerate(all_partitions) if index % self._parallelism == self._task_index}
        if not force and selected == self._assigned:
            if self._metrics is not None:
                self._metrics.assigned_partitions.set(len(selected))
            return

        assignments: list[TopicPartition] = []
        next_positions: dict[PartitionKey, int] = {}
        for key in sorted(selected):
            offset = self._resolve_assignment_offset(key)
            next_positions[key] = offset
            assignments.append(TopicPartition(key[0], key[1], offset))
        consumer.assign(assignments)
        with self._state_lock:
            self._assigned = selected
            self._positions = next_positions
            for key in selected:
                self._restored_positions.pop(key, None)
        if self._metrics is not None:
            self._metrics.assigned_partitions.set(len(selected))
        logger.info(
            "Kafka source subtask %d/%d owns %d partition(s)",
            self._task_index,
            self._parallelism,
            len(selected),
        )

    def _discover_partitions(self) -> list[PartitionKey]:
        consumer = self._require_consumer()
        discovered: list[PartitionKey] = []
        try:
            for topic in self._topics:
                metadata = consumer.list_topics(topic=topic, timeout=_METADATA_TIMEOUT_SECONDS)
                topic_metadata = metadata.topics.get(topic)
                if topic_metadata is None or topic_metadata.error is not None:
                    error = None if topic_metadata is None else topic_metadata.error
                    raise RuntimeError(f"Unable to discover Kafka topic {topic!r}: {error or 'not found'}")
                discovered.extend((topic, partition) for partition in topic_metadata.partitions)
        except Exception:
            self._record_error()
            raise
        return sorted(discovered)

    def _resolve_assignment_offset(self, key: PartitionKey) -> int:
        from confluent_kafka import TopicPartition

        consumer = self._require_consumer()
        topic_partition = TopicPartition(key[0], key[1])
        low, high = consumer.get_watermark_offsets(topic_partition, timeout=_METADATA_TIMEOUT_SECONDS)

        with self._state_lock:
            restored = self._restored_positions.get(key)
            current = self._positions.get(key)
        if restored is not None:
            return _clamp_offset(restored, low, high, key)
        if current is not None:
            return _clamp_offset(current, low, high, key)

        committed = consumer.committed([topic_partition], timeout=_METADATA_TIMEOUT_SECONDS)
        if committed and committed[0].offset not in {_OFFSET_INVALID, None} and committed[0].offset >= 0:
            return _clamp_offset(committed[0].offset, low, high, key)

        configured = _partition_start_offset(self._start_offset, key)
        if configured == "earliest":
            return low
        if configured == "latest":
            return high
        if isinstance(configured, datetime):
            timestamp_ms = _datetime_to_ms(configured)
            resolved = consumer.offsets_for_times(
                [TopicPartition(key[0], key[1], timestamp_ms)],
                timeout=_METADATA_TIMEOUT_SECONDS,
            )
            offset = resolved[0].offset if resolved and resolved[0].offset >= 0 else high
            return _clamp_offset(offset, low, high, key)
        return _clamp_offset(configured, low, high, key)

    def _commit_completed_checkpoints(self) -> None:
        consumer = self._consumer
        if consumer is None:
            return
        with self._state_lock:
            if not self._completed_checkpoints:
                return
            completed = max(self._completed_checkpoints)
            self._completed_checkpoints.clear()
            positions = self._checkpoint_positions.get(completed)
        if positions is None:
            return

        from confluent_kafka import TopicPartition

        offsets = [TopicPartition(topic, partition, offset) for (topic, partition), offset in positions.items()]
        if not offsets:
            return
        started_at = time.monotonic()
        try:
            consumer.commit(offsets=offsets, asynchronous=False)
        except Exception:
            self._record_error()
            with self._state_lock:
                self._completed_checkpoints.append(completed)
            logger.warning("Kafka offset commit failed for checkpoint %d; it will be retried", completed, exc_info=True)
            return
        if self._metrics is not None:
            self._metrics.commits.inc()
            self._metrics.commit_duration_ms.observe_elapsed(started_at)
        with self._state_lock:
            stale = [checkpoint_id for checkpoint_id in self._checkpoint_positions if checkpoint_id <= completed]
            for checkpoint_id in stale:
                self._checkpoint_positions.pop(checkpoint_id, None)

    def _update_consumer_lag(self) -> None:
        if self._metrics is None or self._consumer is None:
            return
        now = time.monotonic()
        if now < self._next_lag_update_at:
            return
        self._next_lag_update_at = now + self._lag_update_interval_seconds

        from confluent_kafka import TopicPartition

        with self._state_lock:
            positions = dict(self._positions)
        lag = 0
        try:
            for (topic, partition), position in positions.items():
                _, high = self._consumer.get_watermark_offsets(
                    TopicPartition(topic, partition),
                    timeout=_METADATA_TIMEOUT_SECONDS,
                    cached=True,
                )
                lag += max(0, high - position)
        except Exception:
            logger.debug("Unable to refresh Kafka consumer lag", exc_info=True)
            return
        self._metrics.consumer_lag_records.set(lag)

    def _record_error(self) -> None:
        if self._metrics is not None:
            self._metrics.errors.inc()

    def _require_consumer(self) -> Consumer:
        if self._consumer is None:
            raise RuntimeError("KafkaSource must be opened before it is run")
        return self._consumer


def _normalize_nonempty_strings(value: str | list[str], name: str) -> list[str]:
    values = [value] if isinstance(value, str) else list(value)
    if not values or any(not isinstance(item, str) or not item.strip() for item in values):
        raise ValueError(f"{name} must contain at least one non-empty string")
    return values


def _validate_positive_integer(name: str, value: int | None, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        suffix = " or None" if optional else ""
        raise TypeError(f"{name} must be a positive integer{suffix}")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


def _validate_start_offset(value: StartOffset, topics: list[str]) -> None:
    if isinstance(value, bool):
        raise TypeError("start_offset must not be a boolean")
    if isinstance(value, Mapping):
        _validate_partition_offsets(value, topics)
        return
    _validate_global_start_offset(value)


def _validate_global_start_offset(value: int | datetime | str) -> None:
    if isinstance(value, int):
        if value < 0:
            raise ValueError("start_offset must be non-negative")
        return
    if isinstance(value, datetime):
        return
    if isinstance(value, str):
        if value not in {"earliest", "latest"}:
            raise ValueError("start_offset must be 'earliest' or 'latest'")
        return
    raise TypeError("start_offset must be an int, datetime, offset strategy, or per-partition mapping")


def _validate_partition_offsets(value: Mapping, topics: list[str]) -> None:
    unknown_topics = set(value) - set(topics)
    if unknown_topics:
        raise ValueError(f"start_offset contains unknown topic(s): {sorted(unknown_topics)}")
    for topic, partitions in value.items():
        if not isinstance(partitions, Mapping):
            raise TypeError(f"start_offset[{topic!r}] must be a partition mapping")
        for partition, offset in partitions.items():
            if isinstance(partition, bool) or not isinstance(partition, int) or partition < 0:
                raise ValueError(f"start_offset[{topic!r}] partition IDs must be non-negative integers")
            if not _valid_partition_offset(offset):
                raise ValueError(f"Invalid start offset for {topic}[{partition}]: {offset!r}")


def _valid_partition_offset(value: Any) -> bool:
    return (not isinstance(value, bool) and isinstance(value, int) and value >= 0) or (
        isinstance(value, str) and value in {"earliest", "latest"}
    )


def _build_consumer_config(
    bootstrap_servers: list[str],
    consumer_config: dict[str, Any],
    *,
    default_group_id: str,
) -> dict[str, Any]:
    config: dict[str, Any] = {"bootstrap.servers": ",".join(bootstrap_servers)}
    if "bootstrap.servers" in consumer_config:
        logger.warning("Ignoring bootstrap.servers from consumer_config; use bootstrap_servers instead")
    config.update({key: value for key, value in consumer_config.items() if key != "bootstrap.servers"})
    if config.get("enable.auto.commit") is True:
        raise ValueError("continuous Kafka sources require enable.auto.commit=false")
    config["enable.auto.commit"] = False
    config["enable.auto.offset.store"] = False
    config.setdefault("group.id", default_group_id)
    return config


def _partition_start_offset(value: StartOffset, key: PartitionKey) -> int | datetime | str:
    if not isinstance(value, Mapping):
        return value
    return value.get(key[0], {}).get(key[1], "earliest")


def _datetime_to_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1_000)


def _clamp_offset(offset: int, low: int, high: int, key: PartitionKey) -> int:
    if offset < low:
        logger.warning(
            "Kafka offset %d for %s[%d] is below the retention floor %d; resuming at the floor",
            offset,
            key[0],
            key[1],
            low,
        )
        return low
    return min(offset, high)


def _encode_positions(positions: dict[PartitionKey, int]) -> dict[str, dict[int, int]]:
    encoded: dict[str, dict[int, int]] = {}
    for (topic, partition), offset in positions.items():
        encoded.setdefault(topic, {})[partition] = offset
    return encoded


def _encode_inflight(inflight: dict[PartitionKey, tuple[int, int]]) -> dict[str, dict[int, dict[str, int]]]:
    encoded: dict[str, dict[int, dict[str, int]]] = {}
    for (topic, partition), (offset, next_index) in inflight.items():
        encoded.setdefault(topic, {})[partition] = {"offset": offset, "next_index": next_index}
    return encoded


def _decode_value_format(state: Any) -> ValueFormat:
    if state is None:
        return "raw"
    value = state.get(_FORMAT_STATE_KEY, "raw")
    if value not in {"raw", "canal-json"}:
        raise ValueError("Unsupported Kafka source checkpoint value format")
    return value


def _decode_inflight(state: Any, positions: dict[PartitionKey, int]) -> dict[PartitionKey, tuple[int, int]]:
    if state is None:
        return {}
    raw_inflight = state.get(_INFLIGHT_STATE_KEY, {})
    if not isinstance(raw_inflight, Mapping):
        raise ValueError("Kafka source checkpoint format in-flight state must be a mapping")
    result: dict[PartitionKey, tuple[int, int]] = {}
    for topic, partitions in raw_inflight.items():
        if not isinstance(topic, str) or not isinstance(partitions, Mapping):
            raise ValueError("Invalid Kafka source checkpoint format in-flight state")
        for partition, cursor in partitions.items():
            partition_id = int(partition) if isinstance(partition, str) and partition.isdigit() else partition
            if isinstance(partition_id, bool) or not isinstance(partition_id, int) or not isinstance(cursor, Mapping):
                raise ValueError("Invalid Kafka source checkpoint format in-flight cursor")
            offset = cursor.get("offset")
            next_index = cursor.get("next_index")
            if any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (offset, next_index)
            ):
                raise ValueError("Invalid Kafka source checkpoint format in-flight cursor")
            key = (topic, partition_id)
            if positions.get(key) != offset or next_index == 0:
                raise ValueError("Kafka source checkpoint format cursor does not match its partition position")
            result[key] = (offset, next_index)
    return result


def _decode_state(state: Any) -> dict[PartitionKey, int]:
    if state is None:
        return {}
    if not isinstance(state, Mapping) or state.get("version") != _STATE_VERSION:
        raise ValueError("Unsupported Kafka source checkpoint state")
    raw_positions = state.get("positions")
    if not isinstance(raw_positions, Mapping):
        raise ValueError("Kafka source checkpoint positions must be a mapping")
    result: dict[PartitionKey, int] = {}
    for topic, partitions in raw_positions.items():
        if not isinstance(topic, str) or not isinstance(partitions, Mapping):
            raise ValueError("Invalid Kafka source checkpoint positions")
        for partition, offset in partitions.items():
            partition_id = int(partition) if isinstance(partition, str) and partition.isdigit() else partition
            if (
                isinstance(partition_id, bool)
                or not isinstance(partition_id, int)
                or isinstance(offset, bool)
                or not isinstance(offset, int)
                or offset < 0
            ):
                raise ValueError("Invalid Kafka source checkpoint offset")
            result[topic, partition_id] = offset
    return result


__all__ = ["KafkaSource"]
