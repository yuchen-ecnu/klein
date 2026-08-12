# SPDX-License-Identifier: Apache-2.0
"""Runtime state containers and rescale identities owned by a StreamTask."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum, auto

from ray.klein.observability.metrics.task_metrics import TaskMetrics
from ray.klein.runtime.collector.task_output import TaskOutput
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.coordinator.checkpoint_strategy import CheckpointStrategy
from ray.klein.runtime.event_time.input_watermark_tracker import InputWatermarkTracker
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.scheduler.task_deployment_descriptor import TaskDeploymentDescriptor
from ray.klein.runtime.worker.async_ordered_runner import AsyncOrderedRunner
from ray.klein.runtime.worker.emit_pipeline import EmitPipeline
from ray.klein.runtime.worker.input_batch_accumulator import InputBatchAccumulator
from ray.klein.runtime.worker.pump import InboxEnvelope, InboxPump
from ray.klein.runtime.worker.watermark import WatermarkController
from ray.klein.runtime.worker.weighted_queue import WeightedQueue
from ray.klein.state.object_store_snapshot_cache import ObjectStoreSnapshotCache


def _operator_runtime_identity(operator: OperatorSpec) -> tuple[object, ...]:
    """Return the stable part of an OperatorSpec across Ray serialization."""

    operator_class = operator.operator_class
    return (
        operator.id,
        operator.name,
        operator.operator_type,
        operator_class.__module__,
        operator_class.__qualname__,
        operator.owns_state,
        tuple(_operator_runtime_identity(child) for child in operator.children),
    )


def _runtime_rescale_descriptor_identity(descriptor: TaskDeploymentDescriptor) -> tuple[object, ...]:
    """Stable identity for idempotent prepare retries across serialization."""

    input_channels = getattr(descriptor, "input_channels", None)
    return (
        descriptor.vertex_id,
        descriptor.task_name,
        descriptor.task_generation,
        descriptor.task_index,
        descriptor.parallelism,
        descriptor.namespace,
        descriptor.restore_operation_id,
        _operator_runtime_identity(descriptor.operator),
        tuple(getattr(descriptor, "input_vertex_ids", ())),
        None if input_channels is None else tuple(input_channels),
        tuple(
            (
                edge.target_task_names,
                edge.control_target_indices,
                edge.topology_epoch,
            )
            for edge in getattr(descriptor, "out_edges", ())
        ),
    )


class _OperatorRunner:
    """Sync/async record processing and configured UDF error handling."""

    def __init__(self, state: _RuntimeState) -> None:
        self._state = state

    def process(self, record: Record) -> None:
        try:
            self._state.operator.invoke_process(record)
        except Exception as error:
            if not self._state.operator.should_ignore_exception(error):
                raise

    async def process_async(self, record: Record) -> list[Record]:
        """Compute one async operator result without touching the emit path."""

        try:
            records = await self._state.operator.invoke_process_async(record)
        except Exception as error:
            if not self._state.operator.should_ignore_exception(error):
                raise
            records = []
        return records or []


@dataclass(slots=True)
class _RuntimeState:
    """Components initialized by ``setup_and_run``."""

    inbox: WeightedQueue[InboxEnvelope]
    operator: StreamOperator
    output: TaskOutput | None
    executor: ThreadPoolExecutor
    input_batches: InputBatchAccumulator
    checkpoint_strategy: CheckpointStrategy
    metrics: TaskMetrics
    is_async_operator: bool = False
    async_runner: AsyncOrderedRunner | None = None
    pipelined: bool = False
    runner: _OperatorRunner | None = None
    state_snapshot_cache: ObjectStoreSnapshotCache | None = None
    event_time_tracker: InputWatermarkTracker | None = None


@dataclass(slots=True)
class _TaskRuntime:
    """One complete operator runtime owned by a StreamTask actor."""

    descriptor: TaskDeploymentDescriptor
    context: TaskRuntimeContext
    state: _RuntimeState
    watermark: WatermarkController
    emit: EmitPipeline
    pump: InboxPump
    state_backend_task_name: str
    closed: bool = False
    async_runner_closed: bool = False
    emit_closed: bool = False
    operator_closed: bool = False
    close_task: asyncio.Task[None] | None = None
    backend_discarded: bool = False


@dataclass(slots=True)
class _RuntimeRescaleTransaction:
    operation_id: str
    previous: _TaskRuntime
    pending: _TaskRuntime


class _RuntimeRescaleOutcome(Enum):
    COMMITTED = auto()
    ROLLED_BACK = auto()


@dataclass(slots=True)
class _CumulativeProgress:
    """Counters retained when one actor swaps to a freshly built runtime."""

    rows_in: int = 0
    rows_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    busy_ns: int = 0
    backpressure_ns: int = 0
    backpressure_events: int = 0
    barriers_in: int = 0
    barriers_out: int = 0

    def add_runtime(self, runtime: _TaskRuntime, successor: _TaskRuntime) -> None:
        operator = runtime.state.operator
        output = runtime.state.output
        self.rows_in += operator.records_in
        self.rows_out += operator.records_out
        self.bytes_in += operator.bytes_in
        self.bytes_out += operator.bytes_out
        self.busy_ns += operator.processing_duration_ns
        if output is not None:
            self.backpressure_ns += output.backpressure_duration_ns
            self.backpressure_events += output.backpressure_events
        barriers_in = self.barriers_in + int(runtime.state.metrics.barriers_in.value)
        barriers_out = self.barriers_out + int(runtime.state.metrics.barriers_out.value)
        self.barriers_in = max(0, barriers_in - int(successor.state.metrics.barriers_in.value))
        self.barriers_out = max(0, barriers_out - int(successor.state.metrics.barriers_out.value))
