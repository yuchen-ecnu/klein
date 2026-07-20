# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import time
from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ray.klein.api.collector import Collector
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Gauge, Histogram
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator
from ray.klein.state.key_group_range import (
    KeyGroupRange,
    assign_key_group_range,
    key_group_for_key,
)
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.managed_state_snapshot import (
    decode_managed_state_snapshot,
    encode_managed_state_snapshot,
)
from ray.klein.state.state_backend_factory import create_state_backend
from ray.klein.state.timer_event import TimerEvent
from ray.klein.state.timer_service import TimerService


class ManagedStateOperator(StreamOperator, OneInputOperator):
    """One-input operator backed by task-local managed keyed state."""

    def __init__(
        self,
        logical_function=None,
        *,
        key_selector: Callable[[Mapping[str, Any]], Any],
        timestamp_selector: Callable[[Mapping[str, Any]], int] | None = None,
    ) -> None:
        super().__init__(logical_function)
        self._key_selector = key_selector
        self._timestamp_selector = timestamp_selector
        self._backend = None
        self._timer_service = None
        self._state_context = None
        self._ttl_cleanup_batch_size = 1000
        self._max_parallelism = 1
        self._key_group_range = KeyGroupRange(0, 0)
        self._timers_fired_metric: Counter | None = None
        self._ttl_cleaned_metric: Counter | None = None
        self._late_records_metric: Counter | None = None
        self._state_size_metric: Gauge | None = None
        self._snapshot_duration_metric: Histogram | None = None
        self._restore_duration_metric: Histogram | None = None
        self._deferred_restore_metrics: tuple[int, float] | None = None

    @property
    def state_context(self) -> KeyedStateContext:
        if self._state_context is None:
            raise RuntimeError("managed state operator is not open")
        return self._state_context

    @property
    def timer_service(self) -> TimerService:
        if self._timer_service is None:
            raise RuntimeError("managed state operator is not open")
        return self._timer_service

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        from ray.klein.config.state_options import StateOptions

        job_id = runtime_context.job_id
        cleanup_batch_size = runtime_context.config.get(StateOptions.TTL_CLEANUP_BATCH_SIZE)
        if cleanup_batch_size < 1:
            raise ValueError("state.ttl.cleanup.batch-size must be at least 1")
        max_parallelism = runtime_context.config.get(StateOptions.MAX_PARALLELISM)
        self._key_group_range = assign_key_group_range(
            max_parallelism,
            runtime_context.parallelism,
            runtime_context.task_index,
        )
        self._max_parallelism = max_parallelism
        self._backend = create_state_backend(
            runtime_context.config,
            job_id,
            runtime_context.state_backend_task_name,
            reset=True,
        )
        self._timer_service = TimerService(self._backend)
        self._state_context = KeyedStateContext(self._backend, self._timer_service)
        self._ttl_cleanup_batch_size = cleanup_batch_size
        try:
            super().open(collector, runtime_context)
            group = self.metric_group
            self._timers_fired_metric = group.builtin_counter(KleinMetrics.TIMERS_FIRED)
            self._ttl_cleaned_metric = group.builtin_counter(KleinMetrics.TTL_ENTRIES_CLEANED)
            self._late_records_metric = group.builtin_counter(KleinMetrics.LATE_RECORDS_DROPPED)
            self._state_size_metric = group.builtin_gauge(KleinMetrics.MANAGED_STATE_SIZE_BYTES)
            self._snapshot_duration_metric = group.builtin_histogram(KleinMetrics.STATE_SNAPSHOT_DURATION_MS)
            self._restore_duration_metric = group.builtin_histogram(KleinMetrics.STATE_RESTORE_DURATION_MS)
        except Exception:
            self._backend.close()
            self._backend = None
            self._timer_service = None
            self._state_context = None
            raise

    def process_element(self, record: Record) -> None:
        key, timestamp = self._key_and_timestamp(record)
        if timestamp is not None and (isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0):
            raise ValueError("stateful timestamp selectors must return a non-negative integer")
        key_group = key_group_for_key(key, self._max_parallelism)
        if key_group not in self._key_group_range:
            raise RuntimeError(
                f"key group {key_group} is not owned by task range {self._key_group_range}; "
                "keyed routing and state max-parallelism must match"
            )
        self.state_context.bind(key, timestamp)
        self.process_managed_element(record, self.state_context)
        for event in self.timer_service.pop_due_processing_time_timers():
            self._fire_timer(event)
        self.after_time_advanced(self.state_context)
        self._cleanup_expired()

    @abstractmethod
    def process_managed_element(
        self,
        record: Record,
        context: KeyedStateContext,
    ) -> None:
        """Process one record with the selected key bound to ``context``."""

    def on_timer(self, event: TimerEvent, context: KeyedStateContext) -> None:
        """Optional timer callback implemented by concrete operators."""

    def after_time_advanced(self, context: KeyedStateContext) -> None:
        """Optional maintenance hook after timers observe the new watermark."""

    def emit_result(self, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, Record):
            self.collect(value)
            return
        if isinstance(value, Mapping):
            self.collect(Record(dict(value)))
            return
        if isinstance(value, Iterable) and not isinstance(value, str | bytes):
            for item in value:
                self.emit_result(item)
            return
        raise TypeError("stateful operator output must be a mapping, Record, iterable, or None")

    def snapshot_state(self) -> bytes:
        started_at = time.monotonic()
        try:
            key_groups = self._backend.snapshot_key_groups(
                self._max_parallelism,
                self._key_group_range,
            )
            snapshot = encode_managed_state_snapshot(
                max_parallelism=self._max_parallelism,
                key_group_range=self._key_group_range,
                key_groups=key_groups,
                watermark=self.timer_service.current_watermark,
            )
            if self._state_size_metric is not None:
                self._state_size_metric.set(len(snapshot))
            return snapshot
        finally:
            if self._snapshot_duration_metric is not None:
                self._snapshot_duration_metric.observe_elapsed(started_at)

    def restore_state(self, snapshot: bytes) -> None:
        self.restore_state_fragments((snapshot,))

    def restore_state_fragments(
        self,
        snapshots: Iterable[bytes],
        *,
        publish_metrics: bool = True,
    ) -> None:
        """Restore the key groups owned after a parallelism change.

        Every previous subtask snapshot may be supplied. Only groups in this
        task's newly assigned range are materialized, so rescaling does not load
        unrelated RocksDB state into the worker.
        """

        started_at = time.monotonic()
        serialized = tuple(snapshots)
        try:
            payloads = [decode_managed_state_snapshot(snapshot) for snapshot in serialized]
            if not payloads:
                return
            self._restore_key_group_payloads(payloads)
        finally:
            sample = (sum(map(len, serialized)), (time.monotonic() - started_at) * 1000)
            if publish_metrics:
                self._publish_restore_metrics(sample)
            else:
                self._deferred_restore_metrics = sample

    def publish_deferred_restore_metrics(self) -> None:
        """Publish metrics captured while a pending rescale runtime was hidden."""

        sample = self._deferred_restore_metrics
        if sample is None:
            return
        self._publish_restore_metrics(sample)
        self._deferred_restore_metrics = None

    def _publish_restore_metrics(self, sample: tuple[int, float]) -> None:
        state_size, duration_ms = sample
        if self._state_size_metric is not None:
            self._state_size_metric.set(state_size)
        if self._restore_duration_metric is not None:
            self._restore_duration_metric.observe(duration_ms)

    def _restore_key_group_payloads(self, payloads: list[dict[str, Any]]) -> None:
        selected: dict[int, bytes] = {}
        watermarks: list[int] = []
        for payload in payloads:
            self._validate_key_group_payload(payload)
            watermarks.append(payload.get("watermark", -1))
            self._select_owned_key_groups(selected, payload.get("key_groups", {}))
        self._backend.restore_key_groups(selected)
        self.timer_service.restore_watermark(min(watermarks, default=-1))

    def _validate_key_group_payload(self, payload: dict[str, Any]) -> None:
        if payload.get("max_parallelism") != self._max_parallelism:
            raise ValueError("state.keyed.max-parallelism must match the value stored in the checkpoint")

    def _select_owned_key_groups(
        self,
        selected: dict[int, bytes],
        key_groups: Mapping[int, bytes],
    ) -> None:
        for key_group, group_snapshot in key_groups.items():
            if key_group not in self._key_group_range:
                continue
            previous = selected.get(key_group)
            if previous is not None and previous != group_snapshot:
                raise ValueError(f"checkpoint contains conflicting key group {key_group}")
            selected[key_group] = group_snapshot

    @property
    def stateful(self) -> bool:
        return True

    def close(self) -> None:
        try:
            if self._backend is not None:
                self._backend.close()
        finally:
            self._backend = None
            self._timer_service = None
            self._state_context = None
            self._deferred_restore_metrics = None
            super().close()

    def on_idle(self) -> None:
        for event in self.timer_service.pop_due_processing_time_timers():
            self._fire_timer(event)
        self._cleanup_expired()

    def on_event_time_watermark(self, timestamp: int) -> None:
        for event in self.timer_service.advance_watermark(timestamp):
            self._fire_timer(event)
        self.after_time_advanced(self.state_context)
        self._cleanup_expired()

    def _fire_timer(self, event: TimerEvent) -> None:
        if self._timers_fired_metric is not None:
            self._timers_fired_metric.inc()
        self.state_context.bind(event.key, event.timestamp)
        self.on_timer(event, self.state_context)

    def _record_late_drop(self) -> None:
        if self._late_records_metric is not None:
            self._late_records_metric.inc()

    def _cleanup_expired(self) -> None:
        cleaned = self._backend.cleanup_expired(limit=self._ttl_cleanup_batch_size)
        if cleaned and self._ttl_cleaned_metric is not None:
            self._ttl_cleaned_metric.inc(cleaned)

    def _key_and_timestamp(self, record: Record) -> tuple[Any, int | None]:
        key = self._key_selector(record.block)
        timestamp = self._timestamp_selector(record.block) if self._timestamp_selector is not None else record.timestamp
        return key, timestamp
