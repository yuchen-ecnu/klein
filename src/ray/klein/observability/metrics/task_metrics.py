# SPDX-License-Identifier: Apache-2.0
"""Task-level instrumentation facade used by the streaming runtime."""

from __future__ import annotations

import time
from dataclasses import dataclass

from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metric_group import MetricGroup
from ray.klein.observability.metrics.metrics import Counter, Gauge, Histogram


@dataclass(frozen=True, slots=True)
class TaskMetrics:
    barriers_in: Counter
    barriers_out: Counter
    input_buffer_records: Gauge
    input_buffer_capacity_records: Gauge
    input_buffer_utilization: Gauge
    input_buffer_bytes: Gauge
    input_buffer_capacity_bytes: Gauge
    input_buffer_byte_utilization: Gauge
    emit_queue_batches: Gauge
    emit_queue_capacity_batches: Gauge
    transport_requests: Counter
    transport_batch_rows: Histogram
    transport_batch_bytes: Histogram
    transport_send_duration_ms: Histogram
    transport_inflight_requests: Gauge
    checkpoint_barrier_latency_ms: Histogram
    current_input_watermark_ms: Gauge
    current_output_watermark_ms: Gauge
    watermark_lag_ms: Gauge
    idle_inputs: Gauge
    backpressure_events: Counter
    backpressure_duration_ms: Histogram
    state_object_store_writes: Counter
    state_object_store_restores: Counter
    state_durable_restore_fallbacks: Counter
    state_object_store_bytes: Gauge
    replay_buffer_records: Gauge
    replay_buffer_bytes: Gauge

    @classmethod
    def create(
        cls,
        group: MetricGroup,
        input_buffer_capacity: int,
        input_buffer_byte_capacity: int,
        emit_queue_capacity: int,
    ) -> TaskMetrics:
        metrics = cls(
            barriers_in=group.builtin_counter(KleinMetrics.BARRIERS_IN),
            barriers_out=group.builtin_counter(KleinMetrics.BARRIERS_OUT),
            input_buffer_records=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_RECORDS),
            input_buffer_capacity_records=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_CAPACITY_RECORDS),
            input_buffer_utilization=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_UTILIZATION),
            input_buffer_bytes=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_BYTES),
            input_buffer_capacity_bytes=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_CAPACITY_BYTES),
            input_buffer_byte_utilization=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_BYTE_UTILIZATION),
            emit_queue_batches=group.builtin_gauge(KleinMetrics.EMIT_QUEUE_BATCHES),
            emit_queue_capacity_batches=group.builtin_gauge(KleinMetrics.EMIT_QUEUE_CAPACITY_BATCHES),
            transport_requests=group.builtin_counter(KleinMetrics.TRANSPORT_REQUESTS),
            transport_batch_rows=group.builtin_histogram(KleinMetrics.TRANSPORT_BATCH_ROWS),
            transport_batch_bytes=group.builtin_histogram(KleinMetrics.TRANSPORT_BATCH_BYTES),
            transport_send_duration_ms=group.builtin_histogram(KleinMetrics.TRANSPORT_SEND_DURATION_MS),
            transport_inflight_requests=group.builtin_gauge(KleinMetrics.TRANSPORT_INFLIGHT_REQUESTS),
            checkpoint_barrier_latency_ms=group.builtin_histogram(KleinMetrics.CHECKPOINT_BARRIER_LATENCY_MS),
            current_input_watermark_ms=group.builtin_gauge(KleinMetrics.CURRENT_INPUT_WATERMARK_MS),
            current_output_watermark_ms=group.builtin_gauge(KleinMetrics.CURRENT_OUTPUT_WATERMARK_MS),
            watermark_lag_ms=group.builtin_gauge(KleinMetrics.WATERMARK_LAG_MS),
            idle_inputs=group.builtin_gauge(KleinMetrics.IDLE_INPUTS),
            backpressure_events=group.builtin_counter(KleinMetrics.BACKPRESSURE_EVENTS),
            backpressure_duration_ms=group.builtin_histogram(KleinMetrics.BACKPRESSURE_DURATION_MS),
            state_object_store_writes=group.builtin_counter(KleinMetrics.STATE_OBJECT_STORE_WRITES),
            state_object_store_restores=group.builtin_counter(KleinMetrics.STATE_OBJECT_STORE_RESTORES),
            state_durable_restore_fallbacks=group.builtin_counter(KleinMetrics.STATE_DURABLE_RESTORE_FALLBACKS),
            state_object_store_bytes=group.builtin_gauge(KleinMetrics.STATE_OBJECT_STORE_BYTES),
            replay_buffer_records=group.builtin_gauge(KleinMetrics.REPLAY_BUFFER_RECORDS),
            replay_buffer_bytes=group.builtin_gauge(KleinMetrics.REPLAY_BUFFER_BYTES),
        )
        metrics.input_buffer_capacity_records.set(input_buffer_capacity)
        metrics.input_buffer_capacity_bytes.set(input_buffer_byte_capacity)
        metrics.emit_queue_capacity_batches.set(emit_queue_capacity)
        metrics.update_input_buffer(0, input_buffer_capacity, 0, input_buffer_byte_capacity)
        metrics.emit_queue_batches.set(0)
        metrics.transport_inflight_requests.set(0)
        metrics.idle_inputs.set(0)
        metrics.replay_buffer_records.set(0)
        metrics.replay_buffer_bytes.set(0)
        return metrics

    def update_input_buffer(self, size: int, capacity: int, size_bytes: int, byte_capacity: int) -> None:
        self.input_buffer_records.set(size)
        self.input_buffer_utilization.set(0 if capacity <= 0 else min(1.0, size / capacity))
        self.input_buffer_bytes.set(size_bytes)
        self.input_buffer_byte_utilization.set(0 if byte_capacity <= 0 else min(1.0, size_bytes / byte_capacity))

    def observe_barrier(self, emitted_at_ms: int | None) -> None:
        if emitted_at_ms is None:
            return
        self.checkpoint_barrier_latency_ms.observe(max(0, int(time.time() * 1000) - emitted_at_ms))

    def update_watermarks(self, input_watermark: int, output_watermark: int, idle_inputs: int) -> None:
        if input_watermark >= 0:
            self.current_input_watermark_ms.set(input_watermark)
        if output_watermark >= 0:
            self.current_output_watermark_ms.set(output_watermark)
            self.watermark_lag_ms.set(max(0, int(time.time() * 1000) - output_watermark))
        self.idle_inputs.set(idle_inputs)
