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

    @classmethod
    def create(cls, group: MetricGroup, input_buffer_capacity: int) -> TaskMetrics:
        metrics = cls(
            barriers_in=group.builtin_counter(KleinMetrics.BARRIERS_IN),
            barriers_out=group.builtin_counter(KleinMetrics.BARRIERS_OUT),
            input_buffer_records=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_RECORDS),
            input_buffer_capacity_records=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_CAPACITY_RECORDS),
            input_buffer_utilization=group.builtin_gauge(KleinMetrics.INPUT_BUFFER_UTILIZATION),
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
        )
        metrics.input_buffer_capacity_records.set(input_buffer_capacity)
        metrics.update_input_buffer(0, input_buffer_capacity)
        metrics.idle_inputs.set(0)
        metrics.replay_buffer_records.set(0)
        return metrics

    def update_input_buffer(self, size: int, capacity: int) -> None:
        self.input_buffer_records.set(size)
        self.input_buffer_utilization.set(0 if capacity <= 0 else min(1.0, size / capacity))

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
