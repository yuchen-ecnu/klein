# SPDX-License-Identifier: Apache-2.0
"""StreamTask — the Ray async actor shell for one operator of the exec graph.

This is intentionally thin: it owns the actor lifecycle and the RPC surface
(``put`` / ``emit_barrier`` / ``replay_buffered_to`` / ``progress_counts`` /
health), and wires together the components that do the work:

* :class:`InboxPump`         — consumes the inbox, drives the operator.
* :class:`EmitPipeline`      — drains buffered emit-ops on the loop (pipelined).
* :class:`WatermarkController` — replay-watermark state + per-mode advance.
* :class:`TaskOutput`        — routes/sends emitted records downstream.
* the snapshot strategy      — barrier alignment / checkpointing.

Concurrency model:

* ``put`` / ``emit_barrier`` are async and back an ``asyncio.Queue`` inbox — a
  full inbox makes ``put`` suspend, which is the backpressure signal.
* One pump task (the AsyncWorker loop) consumes the inbox; the user operator
  runs in a single-thread executor (operators aren't thread-safe; blocking UDFs
  must not stall the actor loop). Async operators are awaited on the loop.
* Non-source tasks pipeline emits (collect buffers, the EmitPipeline drains on
  the loop); sources emit inline from inside their blocking source loop.

``SourceStreamTask`` subclasses this and overrides the pump (it produces instead
of consuming) via ``_on_setup_done`` + its own ``_run``.
"""

import asyncio
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from typing import TYPE_CHECKING

import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.logging import get_logger
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.observability.diagnostics import current_exception_diagnostic
from ray.klein.observability.metrics.task_metrics import TaskMetrics
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.collector.edge_output import DeliveryMode, EdgeOutput
from ray.klein.runtime.collector.task_output import TaskOutput
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.coordinator.checkpoint_strategy import CheckpointStrategy
from ray.klein.runtime.event_time.input_watermark_tracker import InputWatermarkTracker
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.message import (
    Barrier,
    DeliveryChannel,
    PutAck,
    Record,
    RescaleBarrier,
    StreamControl,
)
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.worker.async_ordered_runner import AsyncOrderedRunner
from ray.klein.runtime.worker.async_worker import AsyncWorker
from ray.klein.runtime.worker.emit_pipeline import EmitPipeline
from ray.klein.runtime.worker.input_batch_accumulator import InputBatchAccumulator
from ray.klein.runtime.worker.pump import (
    InboxEnvelope,
    InboxPump,
    inbox_envelope_bytes,
    inbox_envelope_rows,
)
from ray.klein.runtime.worker.watermark import WatermarkController, WatermarkMode
from ray.klein.runtime.worker.weighted_queue import WeightedQueue
from ray.klein.state.object_store_snapshot_cache import ObjectStoreSnapshotCache
from ray.klein.state.state_backend_factory import discard_state_backend
from ray.klein.state.state_snapshot_reference import StateSnapshotReference

if TYPE_CHECKING:
    from ray.klein.runtime.job_manager.progress import SubtaskCounts
    from ray.klein.runtime.scheduler.task_deployment_descriptor import (
        TaskDeploymentDescriptor,
    )


logger = get_logger(__name__)

_RETIRED_RUNTIME_LIMIT = 8
_RETIRED_RUNTIME_CLOSE_TIMEOUT_SECONDS = 30.0
_RETIRED_RUNTIME_RETRY_DELAY_SECONDS = 0.25


def _operator_runtime_identity(operator) -> tuple:
    """Return the stable part of an OperatorSpec across Ray serialization.

    User functions are cloudpickled for every actor RPC, so ordinary dataclass
    equality can report two copies of the same logical operator as different.
    The execution-graph identity and operator recipe shape remain stable and
    are sufficient here; the JobMaster separately guarantees that a rescale
    changes only parallelism before it sends any descriptor to an actor.
    """

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


def _runtime_rescale_descriptor_identity(descriptor: "TaskDeploymentDescriptor") -> tuple:
    """Stable identity for idempotent prepare retries across serialization."""

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

    def __init__(self, state: "_RuntimeState") -> None:
        self._state = state

    def process(self, record: Record) -> None:
        try:
            self._state.operator.invoke_process(record)
        except Exception as error:
            if not self._state.operator.should_ignore_exception(error):
                raise

    async def process_async(self, record: Record) -> list[Record]:
        """Compute the async operator's output for one record (no emit).

        Returns the records the operator would emit; the caller
        (AsyncOrderedRunner's consumer) is responsible for collecting them in
        order. A UDF exception that the configured policy ignores yields an empty
        list (emit nothing); a fatal one propagates so the task can fail.
        """
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
    # Ordered concurrency window for an async operator (None for sync operators);
    # set in setup_and_run after the pump exists.
    async_runner: "AsyncOrderedRunner | None" = None
    pipelined: bool = False
    # The operator runner, set right after construction.
    runner: _OperatorRunner | None = None
    state_snapshot_cache: ObjectStoreSnapshotCache | None = None
    event_time_tracker: InputWatermarkTracker | None = None


@dataclass(slots=True)
class _TaskRuntime:
    """One complete operator runtime owned by a StreamTask actor.

    The actor normally exposes exactly one active instance through its legacy
    ``_state``/``_watermark``/``_emit``/``_pump`` pointers. During retained-actor
    rescaling a second instance can be built and restored here without changing
    any of those live pointers.
    """

    descriptor: "TaskDeploymentDescriptor"
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
        # TaskMetricGroup caches metric handles.  A retained actor can therefore
        # expose the same readable Counter through both runtimes; blindly adding
        # the predecessor and then reading the successor would double count it.
        # Rebase the offset against the successor's raw value instead.  This also
        # preserves continuity when the successor owns a fresh counter at zero.
        barriers_in = self.barriers_in + int(runtime.state.metrics.barriers_in.value)
        barriers_out = self.barriers_out + int(runtime.state.metrics.barriers_out.value)
        self.barriers_in = max(0, barriers_in - int(successor.state.metrics.barriers_in.value))
        self.barriers_out = max(0, barriers_out - int(successor.state.metrics.barriers_out.value))


class StreamTask(AsyncWorker):
    """Async Ray actor that runs one operator of the execution graph."""

    def __init__(self, descriptor: "TaskDeploymentDescriptor") -> None:
        self._descriptor = descriptor
        self._task_name = descriptor.task_name
        self._task_generation = descriptor.task_generation
        super().__init__()
        self._vertex_id: ExecutionVertexId = descriptor.vertex_id
        self._job_manager: KleinActorHandle = klein.get_actor_by_name(
            ComponentName.KLEIN_JOB_MANAGER, namespace=descriptor.namespace
        )
        self._eof_reached: bool = False
        self._running: bool = False
        self._drain_requested: bool = False
        self._state: _RuntimeState | None = None
        self._watermark: WatermarkController | None = None
        self._emit: EmitPipeline | None = None
        self._pump: InboxPump | None = None
        self._active_runtime: _TaskRuntime | None = None
        self._runtime_rescale_transaction: _RuntimeRescaleTransaction | None = None
        self._runtime_rescale_preparing_operation_id: str | None = None
        self._runtime_rescale_outcomes: dict[str, _RuntimeRescaleOutcome] = {}
        self._runtime_rescale_lock_obj: asyncio.Lock | None = None
        self._retired_runtimes: list[_TaskRuntime] = []
        self._retired_runtime_ids: set[int] = set()
        self._retired_runtime_queue: asyncio.Queue[_TaskRuntime] = asyncio.Queue(maxsize=_RETIRED_RUNTIME_LIMIT)
        self._retired_runtime_cleanup_task: asyncio.Task[None] | None = None
        self._retired_runtime_cleanup_errors: dict[int, BaseException] = {}
        self._retired_runtime_cleanup_attempts: dict[int, int] = {}
        self._cumulative_progress = _CumulativeProgress()
        self._last_checkpoint_id: int | None = None
        self._last_checkpoint_state_size_bytes = 0
        self._rescale_operation_id: str | None = None
        self._rescale_role: str | None = None
        self._rescale_expected_senders: set[ExecutionVertexId] = set()
        self._rescale_seen_senders: set[ExecutionVertexId] = set()
        self._rescale_edge_indices: tuple[int, ...] = ()
        self._rescale_ready_obj: asyncio.Event | None = None
        self._rescale_resume_obj: asyncio.Event | None = None
        self._rescale_snapshot = None
        self._rescale_tombstones: list[str] = []
        self._topology_operation_id: str | None = None
        self._topology_previous_descriptor: TaskDeploymentDescriptor | None = None
        self._topology_pending_descriptor: TaskDeploymentDescriptor | None = None
        self._topology_active = False
        self._topology_commit_tombstones: list[str] = []

    # --- small accessors used by the components / subclass ---

    @property
    def eof_reached(self) -> bool:
        return self._eof_reached

    def is_running(self) -> bool:
        locally_fenced = self._rescale_operation_id is not None and (
            self._rescale_role == "replacement"
            or (self._rescale_ready_obj is not None and self._rescale_ready_obj.is_set())
        )
        return self._running and self.healthy and not locally_fenced

    @property
    def _runtime_state(self) -> _RuntimeState:
        state = self._state
        if state is None:
            raise RuntimeError(f"{self._task_name}: _state is None — setup_and_run() not called")
        return state

    # --- setup ---

    async def setup_and_run(self) -> None:
        if self._running:
            return
        runtime = await self._build_runtime(self._descriptor)
        self._install_runtime(runtime)
        try:
            await self._on_setup_done(runtime.context)
            await self.start()
        except BaseException:
            try:
                await self._close_runtime(runtime, discard_backend=True)
            finally:
                self._clear_installed_runtime(runtime)
            raise
        self._running = True

    async def _build_runtime(
        self,
        descriptor: "TaskDeploymentDescriptor",
        *,
        state_backend_task_name: str | None = None,
        publish_metrics: bool = True,
    ) -> _TaskRuntime:
        """Build and restore a runtime without exposing it to input RPCs."""

        backend_task_name = state_backend_task_name or descriptor.task_name
        runtime_context = self._build_runtime_context(
            descriptor,
            state_backend_task_name=backend_task_name,
        )
        runtime_context.checkpoint_strategy.open()
        delivery_mode = (
            DeliveryMode.INLINE if descriptor.operator.operator_type is OperatorType.SOURCE else DeliveryMode.PIPELINED
        )
        output = self._build_output(delivery_mode, descriptor)
        operator = descriptor.operator.build(descriptor.output_queue)
        executor: ThreadPoolExecutor | None = None
        try:
            operator.open(output, runtime_context)
            input_buffer_max_bytes = runtime_context.config.get(PipelineOptions.INPUT_BUFFER_MAX_BYTES)
            emit_queue_max_batches = descriptor.config.get(PipelineOptions.EMIT_QUEUE_MAX_BATCHES)
            inbox = WeightedQueue(
                descriptor.input_buffer_size,
                inbox_envelope_rows,
                max_bytes=input_buffer_max_bytes,
                size_bytes=inbox_envelope_bytes,
            )
            # Single-thread executor so the (non-thread-safe) operator runs off loop.
            executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"{descriptor.task_name}-op")
            task_metrics = TaskMetrics.create(
                descriptor.metric_group,
                descriptor.input_buffer_size,
                input_buffer_max_bytes,
                emit_queue_max_batches,
                initialize=publish_metrics,
            )
            state = _RuntimeState(
                inbox=inbox,
                operator=operator,
                output=output,
                executor=executor,
                input_batches=InputBatchAccumulator(runtime_context.runtime_info),
                checkpoint_strategy=runtime_context.checkpoint_strategy,
                is_async_operator=runtime_context.runtime_info.async_enabled,
                pipelined=delivery_mode is DeliveryMode.PIPELINED,
                metrics=task_metrics,
            )
            state.runner = _OperatorRunner(state)
            state.state_snapshot_cache = self._build_state_snapshot_cache(operator, descriptor)
            state.event_time_tracker = InputWatermarkTracker(descriptor.input_vertex_ids)
            if output is not None:
                self._attach_output_metrics(output, state)

            await self._restore_operator_state(
                runtime_context,
                descriptor=descriptor,
                state=state,
                record_metrics=publish_metrics,
            )

            # Replay watermark + emit pipeline.
            watermark = self._build_watermark(operator, output, descriptor)
            emit = EmitPipeline(
                output,
                watermark,
                self.handle_exception,
                descriptor.task_name,
                queue_maxsize=emit_queue_max_batches,
                queue_size_observer=state.metrics.emit_queue_batches.set,
            )

            # The pump owns dispatch of records returned by the input accumulator.
            from ray.klein.config.event_time_options import EventTimeOptions

            idle_check_interval = descriptor.config.get(EventTimeOptions.IDLE_INPUT_CHECK_INTERVAL)
            if idle_check_interval.total_seconds() <= 0:
                raise ValueError("event-time.idle-input.check-interval must be greater than zero")
            pump = InboxPump(
                self,
                state,
                watermark,
                emit,
                inbox_timeout=idle_check_interval.total_seconds(),
                input_vertex_ids=descriptor.input_vertex_ids,
            )
            watermark.bind(
                output,
                operator,
                executor,
                emit,
                pump.flush_input,
            )

            # Async operator: an ordered concurrency window lets up to
            # async_buffer_size requests run in flight while emission stays in input
            # order (see AsyncOrderedRunner).
            if state.is_async_operator:
                capacity = runtime_context.runtime_info.async_buffer_size or 1
                state.async_runner = AsyncOrderedRunner(
                    capacity=capacity,
                    on_result=pump.on_async_result,
                    on_fatal=self.handle_exception,
                    task_name=descriptor.task_name,
                )
            return _TaskRuntime(
                descriptor,
                runtime_context,
                state,
                watermark,
                emit,
                pump,
                backend_task_name,
            )
        except BaseException:
            try:
                if executor is None:
                    operator.close()
                else:
                    await asyncio.get_running_loop().run_in_executor(executor, operator.close)
            except Exception:
                logger.exception("Failed to close a partially built runtime for %s", descriptor.task_name)
            finally:
                if executor is not None:
                    executor.shutdown(wait=False)
                discard_state_backend(descriptor.config, descriptor.namespace, backend_task_name)
            raise

    def _install_runtime(self, runtime: _TaskRuntime) -> None:
        """Atomically redirect every actor data-plane pointer to one runtime."""

        self._descriptor = runtime.descriptor
        self._state = runtime.state
        self._watermark = runtime.watermark
        self._emit = runtime.emit
        self._pump = runtime.pump
        self._active_runtime = runtime

    def _clear_installed_runtime(self, runtime: _TaskRuntime) -> None:
        if self._active_runtime is not runtime:
            return
        self._state = None
        self._watermark = None
        self._emit = None
        self._pump = None
        self._active_runtime = None

    @staticmethod
    def _start_runtime_components(runtime: _TaskRuntime) -> None:
        if runtime.state.pipelined:
            runtime.emit.start()
        if runtime.state.async_runner is not None:
            runtime.state.async_runner.start()

    @staticmethod
    def _initialize_runtime_metrics(runtime: _TaskRuntime) -> None:
        runtime.state.metrics.initialize_runtime(
            runtime.descriptor.input_buffer_size,
            runtime.descriptor.config.get(PipelineOptions.INPUT_BUFFER_MAX_BYTES),
            runtime.descriptor.config.get(PipelineOptions.EMIT_QUEUE_MAX_BATCHES),
        )
        if isinstance(runtime.state.operator, ManagedStateOperator):
            runtime.state.operator.publish_deferred_restore_metrics()

    async def _close_runtime(
        self,
        runtime: _TaskRuntime,
        timeout: float = 30.0,
        *,
        discard_backend: bool,
    ) -> None:
        if not runtime.closed:
            close_task = runtime.close_task
            if close_task is None or close_task.done():
                if close_task is not None:
                    # Retrieve the previous failure before replacing the task.
                    # A successful task would already have marked the runtime
                    # closed, so only a failed attempt reaches this branch.
                    with suppress(BaseException):
                        close_task.result()
                close_task = asyncio.create_task(
                    self._close_runtime_components(runtime, timeout),
                    name=f"{self._task_name}-close-{runtime.state_backend_task_name}",
                )
                runtime.close_task = close_task
            done, _pending = await asyncio.wait(
                (close_task,),
                timeout=max(0.0, timeout),
            )
            if not done:
                raise TimeoutError(f"timed out closing runtime {runtime.state_backend_task_name} after {timeout:.1f}s")
            await close_task

        if discard_backend and runtime.operator_closed and not runtime.backend_discarded:
            discard_state_backend(
                runtime.descriptor.config,
                runtime.descriptor.namespace,
                runtime.state_backend_task_name,
            )
            runtime.backend_discarded = True

    async def _close_runtime_components(
        self,
        runtime: _TaskRuntime,
        timeout: float,
    ) -> None:
        errors = [
            await self._close_runtime_async_runner(runtime, timeout),
            await self._close_runtime_emit(runtime, timeout),
            await self._close_runtime_operator(runtime),
        ]
        runtime.closed = runtime.async_runner_closed and runtime.emit_closed and runtime.operator_closed
        first_error = next((error for error in errors if error is not None), None)
        if first_error is not None:
            raise first_error

    @staticmethod
    async def _close_runtime_async_runner(runtime: _TaskRuntime, timeout: float) -> BaseException | None:
        if runtime.state.async_runner is None:
            runtime.async_runner_closed = True
            return None
        if not runtime.async_runner_closed:
            try:
                await runtime.state.async_runner.shutdown(timeout)
                runtime.async_runner_closed = True
            except BaseException as error:
                return error
        return None

    @staticmethod
    async def _close_runtime_emit(runtime: _TaskRuntime, timeout: float) -> BaseException | None:
        if not runtime.emit_closed:
            try:
                await runtime.emit.shutdown(timeout)
                runtime.emit_closed = True
            except BaseException as error:
                return error
        return None

    @staticmethod
    async def _close_runtime_operator(runtime: _TaskRuntime) -> BaseException | None:
        if not runtime.operator_closed:
            try:
                await asyncio.get_running_loop().run_in_executor(
                    runtime.state.executor,
                    runtime.state.operator.close,
                )
                runtime.operator_closed = True
                runtime.state.executor.shutdown(wait=False)
            except BaseException as error:
                return error
        return None

    async def setup_for_rescale(self, operation_id: str) -> None:
        """Restore a replacement task completely while keeping its pump fenced."""

        if self._running:
            if self._rescale_operation_id == operation_id and self._rescale_role == "replacement":
                return
            raise RuntimeError(f"{self._task_name} is already running outside rescale {operation_id}")
        self._begin_rescale(operation_id, "replacement")
        try:
            await self.setup_and_run()
        except BaseException:
            self._clear_rescale()
            raise
        self._rescale_ready.set()

    async def setup_and_run_with_descriptor(
        self,
        descriptor: "TaskDeploymentDescriptor",
    ) -> None:
        """Bootstrap a Ray-rebuilt actor with the latest committed topology."""

        if self._running:
            if self._topology_operation_id is not None and self._topology_active:
                self.commit_topology_reconfiguration(self._topology_operation_id)
            if self._rescale_operation_id is not None:
                self.resume_rescale(self._rescale_operation_id)
            return
        if descriptor.vertex_id != self._vertex_id:
            raise ValueError("a rebuilt task must keep its execution vertex id")
        if descriptor.task_name != self._task_name:
            raise ValueError("a rebuilt task must keep its task name")
        if descriptor.task_generation != self._task_generation:
            raise ValueError("a rebuilt task must keep its task generation")
        # A retained Ray actor keeps its original constructor recipe across an
        # actor-process restart. The scheduler's descriptor is authoritative once
        # that actor is not running, including a parallelism adopted by a committed
        # local rescale. Live tasks still reject own-parallelism changes through
        # prepare_topology_reconfiguration().
        self._descriptor = descriptor
        await self.setup_and_run()

    def _build_watermark(
        self,
        operator: StreamOperator,
        output: TaskOutput | None,
        descriptor: "TaskDeploymentDescriptor | None" = None,
    ) -> WatermarkController:
        from ray.klein.config.pipeline_options import PipelineOptions

        descriptor = self._descriptor if descriptor is None else descriptor
        config = descriptor.config
        enabled = config.get(PipelineOptions.REPLAY_BUFFER_ENABLED)
        flush_interval_batches = config.get(PipelineOptions.REPLAY_WATERMARK_FLUSH_BATCHES)
        replay_max_bytes = config.get(PipelineOptions.REPLAY_BUFFER_MAX_BYTES)
        is_sink = operator.operator_type is OperatorType.SINK
        if output is not None:
            # Sender identity is also used by multi-input/stateful operators;
            # it must exist even when replay buffering itself is disabled.
            output.configure_replay(
                False,
                self._vertex_id,
                replay_max_bytes,
                sender_task_name=self._task_name,
                topology_epochs=tuple(edge.topology_epoch for edge in descriptor.out_edges),
            )
        if not enabled:
            mode = WatermarkMode.DISABLED
        elif is_sink:
            mode = WatermarkMode.SINK
        elif output is not None:
            mode = WatermarkMode.PIPELINED
            output.configure_replay(
                True,
                self._vertex_id,
                replay_max_bytes,
                sender_task_name=self._task_name,
                topology_epochs=tuple(edge.topology_epoch for edge in descriptor.out_edges),
            )
        else:
            mode = WatermarkMode.DISABLED
        return WatermarkController(mode, flush_interval_batches, namespace=descriptor.namespace)

    def _build_state_snapshot_cache(
        self,
        operator: StreamOperator,
        descriptor: "TaskDeploymentDescriptor | None" = None,
    ) -> ObjectStoreSnapshotCache | None:
        if not operator.stateful:
            return None
        import ray
        from ray.klein.config.state_options import StateOptions

        descriptor = self._descriptor if descriptor is None else descriptor
        enabled = descriptor.config.get(StateOptions.OBJECT_STORE_CACHE_ENABLED)
        # Debug mode has no Ray Object Store. The same codec remains exercised,
        # with snapshots kept inline.
        enabled = enabled and not klein.is_debug_mode()
        return ObjectStoreSnapshotCache(
            ray.put,
            klein.get,
            min_size_bytes=descriptor.config.get(StateOptions.OBJECT_STORE_CACHE_MIN_BYTES),
            enabled=enabled,
        )

    async def _restore_operator_state(
        self,
        runtime_context: TaskRuntimeContext,
        *,
        descriptor: "TaskDeploymentDescriptor | None" = None,
        state: _RuntimeState | None = None,
        record_metrics: bool = True,
    ) -> None:
        descriptor = self._descriptor if descriptor is None else descriptor
        state = self._runtime_state if state is None else state
        operator = state.operator
        cache = state.state_snapshot_cache
        if not operator.stateful or cache is None:
            return
        strategy = runtime_context.checkpoint_strategy
        restore_operation_id = descriptor.restore_operation_id
        references = (
            tuple(await strategy.restore_rescale_operator_states_async(restore_operation_id))
            if restore_operation_id is not None
            else tuple(await strategy.restore_operator_states_async())
        )
        if restore_operation_id is not None and not references:
            raise RuntimeError(f"managed state for operator rescale {restore_operation_id} is unavailable")
        if not references:
            return
        try:
            payloads = await self._materialize_state(cache, references)
            hot_restores = sum(1 for reference in references if reference.object_ref is not None)
            if hot_restores and record_metrics:
                state.metrics.state_object_store_restores.inc(hot_restores)
        except Exception:
            if restore_operation_id is not None:
                raise
            durable_references = tuple(await strategy.restore_durable_operator_states_async())
            if not durable_references or durable_references == references:
                raise
            logger.warning(
                "Hot Object Store state for %s is unavailable; restoring the durable checkpoint.",
                self._task_name,
            )
            if record_metrics:
                state.metrics.state_durable_restore_fallbacks.inc()
            payloads = await self._materialize_state(cache, durable_references)
        await self._apply_operator_state(
            operator,
            payloads,
            executor=state.executor,
            publish_metrics=record_metrics,
        )

    @staticmethod
    async def _materialize_state(cache: ObjectStoreSnapshotCache, references: tuple) -> tuple[bytes, ...]:
        return tuple(
            await asyncio.gather(*(asyncio.to_thread(cache.materialize, reference) for reference in references))
        )

    async def _apply_operator_state(
        self,
        operator: StreamOperator,
        payloads: tuple[bytes, ...],
        *,
        executor: ThreadPoolExecutor | None = None,
        publish_metrics: bool = True,
    ) -> None:
        if not isinstance(operator, ManagedStateOperator):
            raise TypeError(f"stateful operator {type(operator).__name__} must inherit ManagedStateOperator")
        await asyncio.get_running_loop().run_in_executor(
            self._runtime_state.executor if executor is None else executor,
            partial(
                operator.restore_state_fragments,
                payloads,
                publish_metrics=publish_metrics,
            ),
        )

    def snapshot_operator_state(self, barrier_id: int) -> int:
        """Snapshot after alignment and register before forwarding the barrier."""

        operator = self._runtime_state.operator
        cache = self._runtime_state.state_snapshot_cache
        self._last_checkpoint_id = barrier_id
        self._last_checkpoint_state_size_bytes = 0
        if not operator.stateful or cache is None:
            return 0
        reference = cache.cache(operator.snapshot_state())
        self._last_checkpoint_state_size_bytes = reference.size_bytes
        if reference.object_ref is not None:
            self._runtime_state.metrics.state_object_store_writes.inc()
            self._runtime_state.metrics.state_object_store_bytes.set(reference.size_bytes)
        else:
            self._runtime_state.metrics.state_object_store_bytes.set(0)
        if not self._runtime_state.checkpoint_strategy.register_operator_state(barrier_id, reference):
            raise RuntimeError(f"failed to register managed state for barrier {barrier_id}")
        return reference.size_bytes

    def register_checkpoint_metrics(self, barrier: Barrier, state_size_bytes: int = 0) -> None:
        """Publish one aligned checkpoint sample for dashboard drill-down."""

        state = self._runtime_state
        output = state.output
        latency_ms = 0.0
        if barrier.timestamp is not None:
            latency_ms = max(0.0, int(time.time() * 1000) - barrier.timestamp)
        self._last_checkpoint_id = barrier.id
        self._last_checkpoint_state_size_bytes = max(0, state_size_bytes)
        state.checkpoint_strategy.register_operator_metrics(
            barrier.id,
            {
                "alignment_duration_ms": state.checkpoint_strategy.last_alignment_duration_ms,
                "barrier_latency_ms": latency_ms,
                "state_size_bytes": self._last_checkpoint_state_size_bytes,
                "rows_in": state.operator.records_in,
                "rows_out": state.operator.records_out,
                "backpressure_events": 0 if output is None else output.backpressure_events,
                "backpressure_duration_ms": (0.0 if output is None else output.backpressure_duration_ns / 1_000_000),
            },
        )

    def reset_inflight_before(self, cutoff_barrier_id: int) -> int:
        """Reclaim barrier alignment state after coordinator epoch recovery."""

        if self._state is None:
            return 0
        removed = self._state.checkpoint_strategy.reset_inflight_before(cutoff_barrier_id)
        if self._pump is not None:
            removed += self._pump.reset_inflight_before(cutoff_barrier_id)
        return removed

    def discard_checkpoint(self, barrier_id: int) -> int:
        """Reclaim one timed-out/aborted checkpoint from this task aligner."""

        if self._state is None:
            return 0
        removed = self._state.checkpoint_strategy.discard_checkpoint(barrier_id)
        if self._pump is not None:
            removed += self._pump.discard_checkpoint(barrier_id)
        return removed

    def prepare_sink_commit(self, barrier_id: int) -> None:
        """Pre-commit and register a transactional sink before barrier ack."""

        committable = self._runtime_state.operator.prepare_checkpoint(barrier_id)
        if committable is None:
            return
        if not isinstance(committable, SinkCommittable):
            raise TypeError("transactional sink prepare_checkpoint() must return a SinkCommittable or None")
        if self._runtime_state.checkpoint_strategy.register_sink_committable(barrier_id, committable):
            return
        committable.abort()
        raise RuntimeError(f"failed to register sink transaction for barrier {barrier_id}")

    async def _on_setup_done(self, runtime_context: TaskRuntimeContext) -> None:
        """Hook for subclasses (e.g. source restore) after operator open,
        before the pump starts. Async so a subclass can ``await`` coordinator
        RPCs without blocking the actor event loop."""

    def _build_runtime_context(
        self,
        descriptor: "TaskDeploymentDescriptor | None" = None,
        *,
        state_backend_task_name: str | None = None,
    ) -> TaskRuntimeContext:
        from ray.klein.runtime.coordinator.checkpoint_strategy import (
            AlignedCheckpointStrategy,
        )

        descriptor = self._descriptor if descriptor is None else descriptor
        coordinator = klein.get_actor_by_name(
            ComponentName.KLEIN_CHECKPOINT_COORDINATOR,
            namespace=descriptor.namespace,
        )
        checkpoint_strategy = AlignedCheckpointStrategy(
            coordinator,
            descriptor.barrier_split,
            descriptor.vertex_id,
            descriptor.operator.operator_type,
            descriptor.config,
            is_committer=descriptor.is_committer,
            synchronous_notify=descriptor.operator.transactional_sink,
            metric_group=descriptor.metric_group,
            input_vertex_ids=descriptor.input_vertex_ids,
        )
        return TaskRuntimeContext(
            descriptor.task_name,
            descriptor.task_index,
            descriptor.parallelism,
            descriptor.config,
            descriptor.metric_group,
            checkpoint_strategy,
            descriptor.operator.runtime_info,
            descriptor.namespace,
            state_backend_task_name=state_backend_task_name,
        )

    def _build_output(
        self,
        delivery_mode: DeliveryMode,
        descriptor: "TaskDeploymentDescriptor | None" = None,
    ) -> TaskOutput | None:
        descriptor = self._descriptor if descriptor is None else descriptor
        edges = [self._build_output_edge(edge, delivery_mode, descriptor) for edge in descriptor.out_edges]
        if not edges:
            return None
        return TaskOutput(edges)

    def _build_output_edge(
        self,
        edge,
        delivery_mode: DeliveryMode,
        descriptor: "TaskDeploymentDescriptor | None" = None,
    ) -> EdgeOutput:
        descriptor = self._descriptor if descriptor is None else descriptor
        targets = [klein.get_actor_by_name(name, namespace=descriptor.namespace) for name in edge.target_task_names]
        if any(target is None for target in targets):
            missing = [name for name, target in zip(edge.target_task_names, targets, strict=True) if target is None]
            raise RuntimeError(f"downstream task actor(s) not found: {missing}")
        return EdgeOutput(
            targets,
            edge.partitioner.build(),
            control_targets=edge.control_target_indices,
            output_buffer_max_rows=edge.output_buffer_max_rows,
            target_task_names=edge.target_task_names,
            put_timeout=edge.put_timeout,
            namespace=descriptor.namespace,
            delivery_mode=delivery_mode,
        )

    @staticmethod
    def _attach_output_metrics(output: TaskOutput, state: _RuntimeState) -> None:
        output.attach_runtime_metrics(
            state.metrics.replay_buffer_records.set,
            state.metrics.replay_buffer_bytes.set,
            state.metrics.backpressure_events,
            state.metrics.backpressure_duration_ms,
            transport_requests=state.metrics.transport_requests,
            transport_batch_rows=state.metrics.transport_batch_rows,
            transport_batch_bytes=state.metrics.transport_batch_bytes,
            transport_send_duration_ms=state.metrics.transport_send_duration_ms,
            transport_inflight_observer=state.metrics.transport_inflight_requests.set,
        )

    async def reconfigure_topology(
        self,
        descriptor: "TaskDeploymentDescriptor",
        timeout: float = 30.0,
    ) -> None:
        """Immediately hot-swap topology for recovery/compatibility callers."""

        operation_id = f"immediate-topology-{time.monotonic_ns()}"
        await self.prepare_topology_reconfiguration(operation_id, descriptor, timeout)
        self.activate_topology_reconfiguration(operation_id)
        self.commit_topology_reconfiguration(operation_id)

    async def prepare_topology_reconfiguration(
        self,
        operation_id: str,
        descriptor: "TaskDeploymentDescriptor",
        timeout: float = 30.0,
    ) -> bool:
        """Prepare new routes while retaining the exact old edge journals.

        The operator instance and its actor stay alive. The JobMaster invokes
        this only after the direct upstream/target/downstream participants have
        aligned a local rescale fence. No live route is changed by this phase.
        """

        self._validate_topology_reconfiguration(descriptor)
        if self._topology_operation_id is not None:
            if self._topology_operation_id == operation_id and self._topology_pending_descriptor == descriptor:
                return True
            raise RuntimeError(f"topology transaction {self._topology_operation_id} is already active")

        state = self._runtime_state
        output_changed = descriptor.out_edges != self._descriptor.out_edges
        if output_changed and self._emit is not None and state.pipelined:
            await self._emit.wait_idle(timeout)
        if not self.healthy:
            raise RuntimeError(f"{self._task_name} became unhealthy before topology reconfiguration")

        state.checkpoint_strategy.validate_barrier_reconfiguration()
        pump = getattr(self, "_pump", None)
        if pump is not None:
            pump.validate_checkpoint_reconfiguration()

        if output_changed:
            if state.output is None:
                raise RuntimeError("a terminal task cannot gain output edges during runtime rescale")
            delivery_mode = DeliveryMode.INLINE if descriptor.operator.source else DeliveryMode.PIPELINED
            state.output.prepare_edge_swap(
                operation_id,
                [
                    None if edge == previous else self._build_output_edge(edge, delivery_mode)
                    for previous, edge in zip(
                        self._descriptor.out_edges,
                        descriptor.out_edges,
                        strict=True,
                    )
                ],
            )
        self._topology_operation_id = operation_id
        self._topology_previous_descriptor = self._descriptor
        self._topology_pending_descriptor = descriptor
        self._topology_active = False
        return True

    def activate_topology_reconfiguration(self, operation_id: str) -> bool:
        """Atomically expose a prepared actor-local topology transaction."""

        previous, descriptor = self._require_topology_transaction(operation_id)
        if self._topology_active:
            return True
        state = self._runtime_state
        output_changed = descriptor.out_edges != previous.out_edges
        try:
            if output_changed:
                if state.output is None:
                    raise RuntimeError("a terminal task cannot activate output edges")
                state.output.activate_edge_swap(operation_id)
                self._configure_output_replay(state.output, descriptor)
                self._attach_output_metrics(state.output, state)
            state.checkpoint_strategy.reconfigure_barrier_split(
                dict(descriptor.barrier_split),
                descriptor.input_vertex_ids,
            )
            pump = getattr(self, "_pump", None)
            if pump is not None:
                pump.reconfigure_checkpoint_inputs(descriptor.input_vertex_ids)
            if state.event_time_tracker is not None:
                state.event_time_tracker.reconfigure_inputs(descriptor.input_vertex_ids)
            self._descriptor = descriptor
            self._topology_active = True
            return True
        except BaseException:
            self._restore_topology_transaction(operation_id, previous, output_changed)
            raise

    def rollback_topology_reconfiguration(self, operation_id: str) -> bool:
        """Restore the old descriptor and original edge objects before commit."""

        if operation_id in self._topology_commit_tombstones:
            return False
        if self._topology_operation_id is None:
            # No transaction means this actor is already on its exact old
            # topology (never prepared, or a previous rollback succeeded).
            # Treat that state as an idempotent rollback success so a partial
            # batched prepare does not force a whole-job recovery.
            return True
        previous, descriptor = self._require_topology_transaction(operation_id)
        self._restore_topology_transaction(
            operation_id,
            previous,
            descriptor.out_edges != previous.out_edges,
        )
        return True

    def commit_topology_reconfiguration(self, operation_id: str) -> bool:
        """Release retained old journals at the irreversible local commit."""

        if operation_id in self._topology_commit_tombstones:
            return True
        previous, descriptor = self._require_topology_transaction(operation_id)
        if not self._topology_active:
            raise RuntimeError(f"topology transaction {operation_id} has not been activated")
        if descriptor.out_edges != previous.out_edges:
            state = self._runtime_state
            if state.output is None:
                raise RuntimeError("a terminal task cannot commit output edges")
            state.output.commit_edge_swap(operation_id)
        self._clear_topology_transaction()
        self._topology_commit_tombstones.append(operation_id)
        del self._topology_commit_tombstones[:-16]
        return True

    def _restore_topology_transaction(
        self,
        operation_id: str,
        previous: "TaskDeploymentDescriptor",
        output_changed: bool,
    ) -> None:
        state = self._runtime_state
        # Restore independent pieces best-effort so an injected failure in one
        # component cannot strand the actor on a split topology.
        failures: list[Exception] = []
        self._descriptor = previous
        try:
            state.checkpoint_strategy.reconfigure_barrier_split(
                dict(previous.barrier_split),
                previous.input_vertex_ids,
            )
        except Exception as error:
            failures.append(error)
            logger.exception("Failed to restore checkpoint alignment after topology rollback")
        pump = getattr(self, "_pump", None)
        if pump is not None:
            try:
                pump.reconfigure_checkpoint_inputs(previous.input_vertex_ids)
            except Exception as error:
                failures.append(error)
                logger.exception("Failed to restore checkpoint input gates after topology rollback")
        if state.event_time_tracker is not None:
            try:
                state.event_time_tracker.reconfigure_inputs(previous.input_vertex_ids)
            except Exception as error:
                failures.append(error)
                logger.exception("Failed to restore watermark inputs after topology rollback")
        if output_changed and state.output is not None:
            try:
                state.output.rollback_edge_swap(operation_id)
                self._configure_output_replay(state.output, previous)
                self._attach_output_metrics(state.output, state)
            except Exception as error:
                failures.append(error)
                logger.exception("Failed to restore output edges after topology rollback")
        self._clear_topology_transaction()
        if failures:
            raise RuntimeError(f"failed to restore {len(failures)} component(s) after topology rollback") from failures[
                0
            ]

    def _require_topology_transaction(
        self,
        operation_id: str,
    ) -> tuple["TaskDeploymentDescriptor", "TaskDeploymentDescriptor"]:
        if self._topology_operation_id != operation_id:
            raise RuntimeError(f"topology transaction {operation_id} has not been prepared")
        previous = self._topology_previous_descriptor
        descriptor = self._topology_pending_descriptor
        if previous is None or descriptor is None:
            raise RuntimeError(f"topology transaction {operation_id} is incomplete")
        return previous, descriptor

    def _clear_topology_transaction(self) -> None:
        self._topology_operation_id = None
        self._topology_previous_descriptor = None
        self._topology_pending_descriptor = None
        self._topology_active = False

    def _validate_topology_reconfiguration(self, descriptor: "TaskDeploymentDescriptor") -> None:
        if self._state is None or not self._running:
            raise RuntimeError(f"{self._task_name} is not running")
        if descriptor.vertex_id != self._vertex_id:
            raise ValueError("a live task can only be reconfigured for the same execution vertex")
        if descriptor.parallelism != self._descriptor.parallelism:
            raise ValueError("reconfigure_topology cannot resize the live task's own operator")
        if descriptor.task_name != self._descriptor.task_name:
            raise ValueError("reconfigure_topology cannot rename a live task")
        if descriptor.task_generation != self._task_generation:
            raise ValueError("reconfigure_topology cannot change a live task's generation")

    def _configure_output_replay(
        self,
        output: TaskOutput,
        descriptor: "TaskDeploymentDescriptor",
    ) -> None:
        enabled = descriptor.config.get(PipelineOptions.REPLAY_BUFFER_ENABLED)
        max_bytes = descriptor.config.get(PipelineOptions.REPLAY_BUFFER_MAX_BYTES)
        output.configure_replay(
            enabled,
            self._vertex_id,
            max_bytes,
            sender_task_name=self._task_name,
            topology_epochs=tuple(edge.topology_epoch for edge in descriptor.out_edges),
        )

    # --- actor loop ---

    async def start(self) -> None:
        # Launch the emit-worker (pipelined mode) before the pump so it's ready
        # to drain the first ops.
        if self._runtime_state.pipelined and self._emit is not None:
            self._emit.start()
        # Launch the async ordered runner's FIFO consumer before the pump starts
        # feeding it computes.
        if self._runtime_state.async_runner is not None:
            self._runtime_state.async_runner.start()
        await AsyncWorker.start(self)

    async def _run(self) -> None:
        if self._rescale_role == "replacement":
            await self._rescale_resume.wait()
            return
        if self._rescale_role in {"upstream", "target", "downstream"} and self._rescale_ready.is_set():
            await self._rescale_resume.wait()
            return
        await self._pump.run_once()

    @property
    def _rescale_ready(self) -> asyncio.Event:
        if self._rescale_ready_obj is None:
            self._rescale_ready_obj = asyncio.Event()
        return self._rescale_ready_obj

    @property
    def _rescale_resume(self) -> asyncio.Event:
        if self._rescale_resume_obj is None:
            self._rescale_resume_obj = asyncio.Event()
        return self._rescale_resume_obj

    def _begin_rescale(self, operation_id: str, role: str) -> bool:
        if self._rescale_operation_id == operation_id:
            if self._rescale_role != role:
                raise RuntimeError(
                    f"{self._task_name} already participates in rescale {operation_id} as {self._rescale_role}"
                )
            return False
        if self._rescale_operation_id is not None:
            raise RuntimeError(f"{self._task_name} already participates in rescale {self._rescale_operation_id}")
        self._rescale_operation_id = operation_id
        self._rescale_role = role
        self._rescale_seen_senders.clear()
        self._rescale_snapshot = None
        self._rescale_ready.clear()
        self._rescale_resume.clear()
        return True

    async def prepare_runtime_rescale(
        self,
        operation_id: str,
        descriptor: "TaskDeploymentDescriptor",
    ) -> bool:
        async with self._runtime_rescale_lock:
            return await self._prepare_runtime_rescale_locked(operation_id, descriptor)

    async def _prepare_runtime_rescale_locked(
        self,
        operation_id: str,
        descriptor: "TaskDeploymentDescriptor",
    ) -> bool:
        """Build a retained actor's next runtime without exposing it to input.

        The old target must already be paused at its aligned local barrier. The
        pending runtime owns a separate inbox, operator, output, executor,
        checkpoint strategy, watermark, emit pipeline and managed-state backend.
        Only ``commit_runtime_rescale`` redirects the actor's live pointers.
        """

        outcome = self._runtime_rescale_outcomes.get(operation_id)
        if outcome is not None:
            return outcome is _RuntimeRescaleOutcome.COMMITTED
        transaction = self._runtime_rescale_transaction
        if transaction is not None:
            return self._validate_existing_runtime_rescale(transaction, operation_id, descriptor)
        if self._runtime_rescale_preparing_operation_id is not None:
            raise RuntimeError(
                f"runtime rescale {self._runtime_rescale_preparing_operation_id} is already being prepared"
            )
        self._require_retired_runtime_capacity()
        self._runtime_rescale_preparing_operation_id = operation_id
        try:
            if self._rescale_operation_id != operation_id or self._rescale_role != "target":
                raise RuntimeError(f"{self._task_name} is not an old target of rescale {operation_id}")
            if self._rescale_ready_obj is None or not self._rescale_ready_obj.is_set():
                raise RuntimeError(f"{self._task_name} has not reached the rescale cut")
            if not self._running or self._active_runtime is None:
                raise RuntimeError(f"{self._task_name} has no live runtime to retain")
            self._validate_runtime_rescale_descriptor(descriptor, operation_id)

            previous = self._active_runtime
            backend_task_name = f"{descriptor.task_name}.__rescale__.{operation_id}"
            pending = await self._build_runtime(
                descriptor,
                state_backend_task_name=backend_task_name,
                publish_metrics=False,
            )
            if self._active_runtime is not previous:
                await self._close_runtime(pending, discard_backend=True)
                raise RuntimeError(f"{self._task_name}'s active runtime changed while preparing rescale")
            self._runtime_rescale_transaction = _RuntimeRescaleTransaction(
                operation_id,
                previous,
                pending,
            )
            return True
        except BaseException:
            # Nothing has been published yet and _build_runtime owns cleanup of
            # every partially constructed component. Remember that this actor is
            # already back on its exact old runtime so a coordinator-wide
            # rollback remains idempotent even when one actor failed to prepare.
            self._remember_runtime_rescale_outcome(operation_id, committed=False)
            raise
        finally:
            self._runtime_rescale_preparing_operation_id = None

    @staticmethod
    def _validate_existing_runtime_rescale(
        transaction: _RuntimeRescaleTransaction,
        operation_id: str,
        descriptor: "TaskDeploymentDescriptor",
    ) -> bool:
        if transaction.operation_id != operation_id:
            raise RuntimeError(f"runtime rescale {transaction.operation_id} is already prepared")
        if _runtime_rescale_descriptor_identity(transaction.pending.descriptor) != _runtime_rescale_descriptor_identity(
            descriptor
        ):
            raise ValueError(f"runtime rescale {operation_id} was retried with a different descriptor")
        return True

    async def commit_runtime_rescale(self, operation_id: str) -> bool:
        async with self._runtime_rescale_lock:
            return await self._commit_runtime_rescale_locked(operation_id)

    async def _commit_runtime_rescale_locked(self, operation_id: str) -> bool:
        """Atomically publish a prepared runtime and retire its predecessor."""

        outcome = self._runtime_rescale_outcomes.get(operation_id)
        if outcome is not None:
            return outcome is _RuntimeRescaleOutcome.COMMITTED
        transaction = self._require_runtime_rescale_transaction(operation_id)
        if self._active_runtime is not transaction.previous:
            raise RuntimeError(f"{self._task_name}'s previous runtime is no longer active")

        # These consumers are idle because the pending inbox/output are still
        # unreachable. Starting them cannot expose the pending runtime; the five
        # pointer assignments in _install_runtime are the actor-local commit point.
        self._start_runtime_components(transaction.pending)
        self._progress_offset().add_runtime(transaction.previous, transaction.pending)
        self._install_runtime(transaction.pending)
        self._runtime_rescale_transaction = None
        self._remember_runtime_rescale_outcome(operation_id, committed=True)
        try:
            self._initialize_runtime_metrics(transaction.pending)
        except Exception:
            logger.exception("Failed to publish committed runtime metrics for %s", self._task_name)
        # Cleanup is deliberately outside the commit latency. Admission was
        # checked during prepare, so this bounded enqueue cannot block here.
        self._retire_runtime(transaction.previous)
        return True

    async def rollback_runtime_rescale(self, operation_id: str) -> bool:
        async with self._runtime_rescale_lock:
            return await self._rollback_runtime_rescale_locked(operation_id)

    async def _rollback_runtime_rescale_locked(self, operation_id: str) -> bool:
        """Discard a pending runtime while leaving the exact old runtime active."""

        outcome = self._runtime_rescale_outcomes.get(operation_id)
        if outcome is not None:
            return outcome is _RuntimeRescaleOutcome.ROLLED_BACK
        transaction = self._runtime_rescale_transaction
        if transaction is None:
            # Batched coordinator preparation can fail before this actor sees
            # the request. Being on the untouched old runtime is already the
            # desired idempotent rollback state.
            return True
        if transaction.operation_id != operation_id:
            raise RuntimeError(f"runtime rescale {transaction.operation_id} is already prepared")
        await self._close_runtime(transaction.pending, discard_backend=True)
        self._runtime_rescale_transaction = None
        self._remember_runtime_rescale_outcome(operation_id, committed=False)
        return True

    @property
    def _runtime_rescale_lock(self) -> asyncio.Lock:
        if getattr(self, "_runtime_rescale_lock_obj", None) is None:
            self._runtime_rescale_lock_obj = asyncio.Lock()
        return self._runtime_rescale_lock_obj

    def _validate_runtime_rescale_descriptor(
        self,
        descriptor: "TaskDeploymentDescriptor",
        operation_id: str,
    ) -> None:
        current = self._descriptor
        if descriptor.vertex_id != self._vertex_id:
            raise ValueError("a retained task must keep its execution vertex id")
        if descriptor.task_name != self._task_name:
            raise ValueError("a retained task must keep its Ray actor name")
        if descriptor.task_generation != self._task_generation:
            raise ValueError("a retained task must keep its task generation")
        if descriptor.task_index != current.task_index:
            raise ValueError("a retained task must keep its subtask index")
        if descriptor.namespace != current.namespace:
            raise ValueError("a retained task cannot move to another job namespace")
        if _operator_runtime_identity(descriptor.operator) != _operator_runtime_identity(current.operator):
            raise ValueError("runtime rescale cannot replace the retained task's operator")
        if descriptor.operator.source:
            raise ValueError("source operators cannot prepare a retained runtime")
        if descriptor.parallelism <= 0:
            raise ValueError("runtime rescale parallelism must be positive")
        if descriptor.operator.stateful and descriptor.restore_operation_id != operation_id:
            raise ValueError("a managed-state runtime must restore the active rescale cut")

    def _require_runtime_rescale_transaction(self, operation_id: str) -> _RuntimeRescaleTransaction:
        transaction = self._runtime_rescale_transaction
        if transaction is None or transaction.operation_id != operation_id:
            raise RuntimeError(f"runtime rescale {operation_id} has not been prepared")
        return transaction

    def _remember_runtime_rescale_outcome(self, operation_id: str, *, committed: bool) -> None:
        self._runtime_rescale_outcomes[operation_id] = (
            _RuntimeRescaleOutcome.COMMITTED if committed else _RuntimeRescaleOutcome.ROLLED_BACK
        )
        while len(self._runtime_rescale_outcomes) > 16:
            self._runtime_rescale_outcomes.pop(next(iter(self._runtime_rescale_outcomes)))

    def _initialize_retired_runtime_cleanup(self) -> None:
        """Lazily initialize cleanup state for lightweight unit-test actors."""

        if not hasattr(self, "_retired_runtimes"):
            self._retired_runtimes = []
        if not hasattr(self, "_retired_runtime_ids"):
            self._retired_runtime_ids = {id(runtime) for runtime in self._retired_runtimes}
        if not hasattr(self, "_retired_runtime_queue"):
            self._retired_runtime_queue = asyncio.Queue(maxsize=_RETIRED_RUNTIME_LIMIT)
            for runtime in self._retired_runtimes:
                self._retired_runtime_queue.put_nowait(runtime)
        if not hasattr(self, "_retired_runtime_cleanup_task"):
            self._retired_runtime_cleanup_task = None
        if not hasattr(self, "_retired_runtime_cleanup_errors"):
            self._retired_runtime_cleanup_errors = {}
        if not hasattr(self, "_retired_runtime_cleanup_attempts"):
            self._retired_runtime_cleanup_attempts = {}

    def _require_retired_runtime_capacity(self) -> None:
        self._initialize_retired_runtime_cleanup()
        self._ensure_retired_runtime_cleanup()
        if len(self._retired_runtime_ids) >= _RETIRED_RUNTIME_LIMIT:
            raise RuntimeError(
                f"{self._task_name} has {_RETIRED_RUNTIME_LIMIT} retired runtimes awaiting cleanup; "
                "rejecting rescale until cleanup catches up"
            )

    def _retire_runtime(self, runtime: _TaskRuntime) -> None:
        self._initialize_retired_runtime_cleanup()
        runtime_id = id(runtime)
        if runtime_id in self._retired_runtime_ids:
            return
        if len(self._retired_runtime_ids) >= _RETIRED_RUNTIME_LIMIT:
            raise RuntimeError("retired runtime cleanup queue is full after rescale admission")
        self._retired_runtime_ids.add(runtime_id)
        self._retired_runtimes.append(runtime)
        self._retired_runtime_queue.put_nowait(runtime)
        self._ensure_retired_runtime_cleanup()

    def _ensure_retired_runtime_cleanup(self) -> None:
        cleanup_task = self._retired_runtime_cleanup_task
        if not self._retired_runtime_ids or (cleanup_task is not None and not cleanup_task.done()):
            return
        cleanup_task = asyncio.create_task(
            self._cleanup_retired_runtimes(),
            name=f"{self._task_name}-retired-runtime-cleanup",
        )
        self._retired_runtime_cleanup_task = cleanup_task
        cleanup_task.add_done_callback(self._on_retired_runtime_cleanup_done)

    def _on_retired_runtime_cleanup_done(self, cleanup_task: asyncio.Task[None]) -> None:
        if self._retired_runtime_cleanup_task is cleanup_task:
            self._retired_runtime_cleanup_task = None
        error = None if cleanup_task.cancelled() else cleanup_task.exception()
        if error is not None:
            logger.error("Retired runtime cleanup worker failed for %s: %s", self._task_name, error)
        if self._retired_runtime_ids:
            asyncio.get_running_loop().call_soon(self._ensure_retired_runtime_cleanup)

    async def _cleanup_retired_runtimes(self) -> None:
        while self._retired_runtime_ids:
            runtime = await self._retired_runtime_queue.get()
            runtime_id = id(runtime)
            try:
                await self._close_runtime(
                    runtime,
                    _RETIRED_RUNTIME_CLOSE_TIMEOUT_SECONDS,
                    discard_backend=True,
                )
            except asyncio.CancelledError:
                self._retired_runtime_queue.put_nowait(runtime)
                raise
            except BaseException as error:
                self._retired_runtime_cleanup_errors[runtime_id] = error
                attempts = self._retired_runtime_cleanup_attempts.get(runtime_id, 0) + 1
                self._retired_runtime_cleanup_attempts[runtime_id] = attempts
                self._retired_runtime_queue.put_nowait(runtime)
                logger.warning(
                    "Retired runtime cleanup attempt %d failed for %s/%s: %s",
                    attempts,
                    self._task_name,
                    runtime.state_backend_task_name,
                    error,
                )
                await asyncio.sleep(_RETIRED_RUNTIME_RETRY_DELAY_SECONDS)
            else:
                self._retired_runtime_ids.discard(runtime_id)
                self._retired_runtimes = [item for item in self._retired_runtimes if item is not runtime]
                self._retired_runtime_cleanup_errors.pop(runtime_id, None)
                self._retired_runtime_cleanup_attempts.pop(runtime_id, None)
            finally:
                self._retired_runtime_queue.task_done()

    async def _await_retired_runtime_cleanup(self, timeout: float) -> BaseException | None:
        self._initialize_retired_runtime_cleanup()
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
        while self._retired_runtime_ids:
            self._ensure_retired_runtime_cleanup()
            cleanup_task = self._retired_runtime_cleanup_task
            if cleanup_task is None:
                break
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            done, _pending = await asyncio.wait((cleanup_task,), timeout=remaining)
            if not done:
                break
        if not self._retired_runtime_ids:
            return None
        details = []
        for runtime in self._retired_runtimes:
            error = self._retired_runtime_cleanup_errors.get(id(runtime))
            suffix = f": {error}" if error is not None else ""
            details.append(f"{runtime.state_backend_task_name}{suffix}")
        return TimeoutError(
            f"{len(self._retired_runtime_ids)} retired runtime(s) remain after {timeout:.1f}s: " + ", ".join(details)
        )

    async def prepare_rescale_upstream(
        self,
        operation_id: str,
        target_operator_id: int,
        edge_indices: tuple[int, ...],
        timeout: float,
    ) -> bool:
        """Fence selected outputs after all earlier input and then pause."""

        requested_edges = tuple(edge_indices)
        if not requested_edges:
            raise ValueError("an upstream rescale participant needs a target output edge")
        new_participant = self._begin_rescale(operation_id, "upstream")
        if not new_participant:
            if self._rescale_edge_indices != requested_edges:
                raise ValueError(f"{self._task_name} retried rescale {operation_id} with different output edges")
            await asyncio.wait_for(self._rescale_ready.wait(), timeout=timeout)
            return True
        self._rescale_edge_indices = requested_edges
        await self._runtime_state.inbox.put(InboxEnvelope(RescaleBarrier(operation_id, target_operator_id)))
        await asyncio.wait_for(self._rescale_ready.wait(), timeout=timeout)
        return True

    def prepare_rescale_target(self, operation_id: str) -> None:
        expected_senders = set(self._descriptor.input_vertex_ids)
        if not expected_senders:
            raise ValueError("source operators cannot be locally rescaled")
        if not self._begin_rescale(operation_id, "target"):
            if self._rescale_expected_senders != expected_senders:
                raise ValueError(f"{self._task_name} retried rescale {operation_id} with different senders")
            return
        self._rescale_expected_senders = expected_senders

    def prepare_rescale_downstream(
        self,
        operation_id: str,
        expected_senders: tuple[ExecutionVertexId, ...],
    ) -> None:
        requested_senders = set(expected_senders)
        if not requested_senders:
            raise ValueError("a downstream rescale participant needs at least one target input")
        if not self._begin_rescale(operation_id, "downstream"):
            if self._rescale_expected_senders != requested_senders:
                raise ValueError(f"{self._task_name} retried rescale {operation_id} with different senders")
            return
        self._rescale_expected_senders = requested_senders

    async def await_rescale_ready(
        self,
        operation_id: str,
        timeout: float,
    ) -> StateSnapshotReference | None:
        if self._rescale_operation_id != operation_id:
            raise ValueError(f"{self._task_name} is not participating in rescale {operation_id}")
        await asyncio.wait_for(self._rescale_ready.wait(), timeout=timeout)
        return self._rescale_snapshot

    def resume_rescale(self, operation_id: str) -> bool:
        if operation_id in self._rescale_tombstones:
            return True
        if self._rescale_operation_id is None:
            # A batched participant prepare may fail before reaching every
            # actor. An untouched actor is already resumed for this operation.
            return True
        if self._rescale_operation_id != operation_id:
            return False
        if getattr(self, "_runtime_rescale_preparing_operation_id", None) == operation_id:
            raise RuntimeError(f"runtime rescale {operation_id} is still being prepared")
        transaction = getattr(self, "_runtime_rescale_transaction", None)
        if transaction is not None and transaction.operation_id == operation_id:
            raise RuntimeError(f"runtime rescale {operation_id} must be committed or rolled back before resume")
        if self._topology_operation_id == operation_id and self._topology_active:
            self.commit_topology_reconfiguration(operation_id)
        self._rescale_resume.set()
        self._clear_rescale()
        return True

    def _clear_rescale(self) -> None:
        if self._rescale_operation_id is not None:
            self._rescale_tombstones.append(self._rescale_operation_id)
            del self._rescale_tombstones[:-16]
        self._rescale_operation_id = None
        self._rescale_role = None
        self._rescale_expected_senders.clear()
        self._rescale_seen_senders.clear()
        self._rescale_edge_indices = ()
        self._rescale_snapshot = None
        self._rescale_ready_obj = None
        self._rescale_resume_obj = None

    async def handle_rescale_barrier(
        self,
        barrier: RescaleBarrier,
        sender_vertex_id: ExecutionVertexId | None,
    ) -> None:
        """Process one local topology fence in ordered inbox position."""

        if barrier.operation_id in self._rescale_tombstones:
            return
        if barrier.operation_id != self._rescale_operation_id:
            raise RuntimeError(f"unexpected rescale barrier {barrier.operation_id} at {self._task_name}")
        await self._drain_rescale_boundary()
        if sender_vertex_id is None:
            await self._originate_rescale_barrier(barrier)
            return
        await self._receive_rescale_barrier(barrier, sender_vertex_id)

    async def _drain_rescale_boundary(self) -> None:
        state = self._runtime_state
        if state.async_runner is not None:
            await self._pump.flush_input_async()
            await state.async_runner.barrier()
        else:
            await asyncio.get_running_loop().run_in_executor(state.executor, self._pump.flush_input)
        if self._emit is not None:
            await self._emit.wait_idle(30.0)

    async def _originate_rescale_barrier(self, barrier: RescaleBarrier) -> None:
        state = self._runtime_state
        if self._rescale_role != "upstream" or state.output is None:
            raise RuntimeError("only a prepared upstream task may originate a rescale barrier")
        await asyncio.get_running_loop().run_in_executor(
            state.executor,
            state.output.collect_to_edges,
            barrier,
            self._rescale_edge_indices,
        )
        if self._emit is not None:
            await self._emit.wait_idle(30.0)
        self._rescale_ready.set()

    async def _receive_rescale_barrier(
        self,
        barrier: RescaleBarrier,
        sender_vertex_id: ExecutionVertexId,
    ) -> None:
        if self._rescale_role not in {"target", "downstream"}:
            raise RuntimeError(f"{self._task_name} was not prepared to receive a rescale barrier")
        if sender_vertex_id not in self._rescale_expected_senders:
            raise RuntimeError(f"unexpected rescale sender {sender_vertex_id} at {self._task_name}")
        if sender_vertex_id in self._rescale_seen_senders:
            return
        self._rescale_seen_senders.add(sender_vertex_id)
        if self._rescale_seen_senders != self._rescale_expected_senders:
            return

        if self._rescale_role == "target":
            await self._finish_target_rescale_barrier(barrier)
        else:
            await self._finish_downstream_rescale_barrier()
        self._rescale_ready.set()

    async def _finish_target_rescale_barrier(self, barrier: RescaleBarrier) -> None:
        self._rescale_snapshot = await asyncio.get_running_loop().run_in_executor(
            self._runtime_state.executor,
            self._snapshot_and_forward_rescale_barrier,
            barrier,
        )
        if self._emit is not None:
            await self._emit.wait_idle(30.0)

    async def _finish_downstream_rescale_barrier(self) -> None:
        if self._watermark is None:
            return
        await self._watermark.advance()
        if self._emit is not None:
            await self._emit.wait_idle(30.0)

    def _snapshot_and_forward_rescale_barrier(
        self,
        barrier: RescaleBarrier,
    ) -> StateSnapshotReference | None:
        state = self._runtime_state
        state.operator.flush()
        snapshot = None
        if state.operator.stateful:
            cache = state.state_snapshot_cache
            if not isinstance(state.operator, ManagedStateOperator) or cache is None:
                raise TypeError("stateful rescale target must use managed state")
            snapshot = cache.cache(state.operator.snapshot_state())
        if state.output is not None:
            state.output.flush(force=True)
            state.output.collect(barrier)
        return snapshot

    # --- inbox RPC surface ---

    async def emit_barrier(
        self,
        barrier: Barrier,
        sender_vertex_id: ExecutionVertexId | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> int:
        if self._state is None:
            return 0
        if self._pump is not None:
            if barrier.coordinated:
                if not self._pump.announce_coordinated_barrier(
                    barrier,
                    sender_vertex_id,
                    delivery_channel,
                ):
                    return self._update_buffer_size_metrics()
            else:
                await self._pump.wait_for_checkpoint_input(sender_vertex_id, barrier, delivery_channel)
        envelope = InboxEnvelope(barrier, sender_vertex_id, delivery_channel=delivery_channel)
        put_control = getattr(self._state.inbox, "put_control", None)
        if barrier.coordinated and callable(put_control):
            await put_control(envelope)
        else:
            await self._state.inbox.put(envelope)
        self._state.metrics.barriers_in.inc()
        return self._update_buffer_size_metrics()

    async def emit_stream_control(
        self,
        control: StreamControl,
        sender_vertex_id: object = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> int:
        """Enqueue an ordered event-time control from one physical input."""

        if self._state is None:
            return 0
        if self._pump is not None:
            await self._pump.wait_for_checkpoint_input(sender_vertex_id, control, delivery_channel)
        await self._state.inbox.put(InboxEnvelope(control, sender_vertex_id, delivery_channel=delivery_channel))
        return self._update_buffer_size_metrics()

    async def put(
        self,
        record: Record | Sequence[Record],
        timeout: float | None = None,
        sender_vertex_id: object = None,
        batch_sequence: int | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> PutAck:
        """Enqueue a record (or batch) onto the inbox.

        Backpressure: a full inbox suspends ``inbox.put``. With ``timeout`` we
        bound the wait and return ``accepted=False`` on expiry so the caller's
        partitioner can re-route; without a timeout the put awaits free space.

        The acknowledgement carries the replay ``forwarded_sequence`` watermark for
        ``sender_vertex_id`` — the largest sequence from that sender whose output this task
        has already forwarded onward.

        ``WeightedQueue.put`` mutates the inbox only after its final await, so a
        timed-out/cancelled call has not enqueued the envelope and may safely be
        retried on another eligible target. Klein remains at-least-once across
        task recovery and replay, so operators must still tolerate duplicates.
        """
        if self._state is None:
            return PutAck(False, 0, self._forwarded_sequence_for(delivery_channel or sender_vertex_id))

        async def admit() -> None:
            if self._pump is not None:
                await self._pump.wait_for_checkpoint_input(sender_vertex_id, record, delivery_channel)
            await self._state.inbox.put(InboxEnvelope(record, sender_vertex_id, batch_sequence, delivery_channel))

        try:
            if timeout is None:
                await admit()
            else:
                await asyncio.wait_for(admit(), timeout=timeout)
        except asyncio.TimeoutError:
            return PutAck(
                False,
                self._update_buffer_size_metrics(),
                self._forwarded_sequence_for(delivery_channel or sender_vertex_id),
            )
        return PutAck(
            True,
            self._update_buffer_size_metrics(),
            self._forwarded_sequence_for(delivery_channel or sender_vertex_id),
        )

    async def try_put(
        self,
        record: Record | Sequence[Record],
        sender_vertex_id: object = None,
        batch_sequence: int | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> PutAck:
        """Try one atomic inbox admission without waiting for a timeout.

        Upstream target lanes use this fast path to probe the eligible retry
        ring. A full task responds immediately, so another worker can accept the
        batch without paying ``input-buffer.put-timeout`` for every candidate.
        """
        if self._state is None:
            return PutAck(False, 0, self._forwarded_sequence_for(delivery_channel or sender_vertex_id))
        accepted = False
        if self._pump is None or not self._pump.checkpoint_input_blocked(sender_vertex_id, delivery_channel):
            accepted = await self._state.inbox.try_put(
                InboxEnvelope(record, sender_vertex_id, batch_sequence, delivery_channel)
            )
        return PutAck(
            accepted,
            self._update_buffer_size_metrics(),
            self._forwarded_sequence_for(delivery_channel or sender_vertex_id),
        )

    def acknowledge_delivery(self, edge_index: int, target_index: int, forwarded_sequence: int) -> None:
        """Release one replay lane after an explicit downstream durability ack."""
        if self._state is not None and self._state.output is not None:
            self._state.output.acknowledge_delivery(edge_index, target_index, forwarded_sequence)

    def _forwarded_sequence_for(self, sender_vertex_id: object) -> int:
        return self._watermark.forwarded_sequence_for(sender_vertex_id) if self._watermark is not None else -1

    def _update_buffer_size_metrics(self) -> int:
        current_buffer_size = self._state.inbox.qsize()
        self._state.metrics.update_input_buffer(
            current_buffer_size,
            self._descriptor.input_buffer_size,
            self._state.inbox.byte_size,
            self._descriptor.config.get(PipelineOptions.INPUT_BUFFER_MAX_BYTES),
        )
        return current_buffer_size

    def progress_counts(self) -> "SubtaskCounts":
        """This subtask's throughput counters for the CLI progress view.

        Sync + side-effect-free, cheap to poll. Returns zeroed counts before the
        operator is built (a freshly Ray-restarted but not-yet-setup actor) so the
        JobManager snapshot reads uniform ``SubtaskCounts`` from every subtask."""
        from ray.klein.runtime.job_manager.progress import SubtaskCounts

        if self._state is None:
            return SubtaskCounts()
        cumulative = self._progress_offset()
        return SubtaskCounts(
            rows_in=cumulative.rows_in + self._state.operator.records_in,
            rows_out=cumulative.rows_out + self._state.operator.records_out,
            bytes_in=cumulative.bytes_in + self._state.operator.bytes_in,
            bytes_out=cumulative.bytes_out + self._state.operator.bytes_out,
            queued=self._state.inbox.qsize(),
            capacity=self._descriptor.input_buffer_size,
            busy_ns=cumulative.busy_ns + self._state.operator.processing_duration_ns,
            backpressure_ns=cumulative.backpressure_ns
            + (0 if self._state.output is None else self._state.output.backpressure_duration_ns),
            backpressure_events=cumulative.backpressure_events
            + (0 if self._state.output is None else self._state.output.backpressure_events),
            barriers_in=cumulative.barriers_in + int(self._state.metrics.barriers_in.value),
            barriers_out=cumulative.barriers_out + int(self._state.metrics.barriers_out.value),
            checkpoint_alignment_ms=self._state.checkpoint_strategy.last_alignment_duration_ms,
            checkpoint_barrier_latency_ms=self._state.metrics.checkpoint_barrier_latency_ms.last,
            checkpoint_state_size_bytes=self._last_checkpoint_state_size_bytes,
            last_checkpoint_id=self._last_checkpoint_id,
        )

    def _progress_offset(self) -> _CumulativeProgress:
        # A few compatibility/unit-test constructors predate this field and
        # bypass __init__; laziness also keeps restored actor objects safe.
        cumulative = getattr(self, "_cumulative_progress", None)
        if cumulative is None:
            cumulative = _CumulativeProgress()
            self._cumulative_progress = cumulative
        return cumulative

    # --- recovery: replay buffered records to a rebuilt downstream ---

    async def replay_buffered_to(self, downstream_name: str) -> int:
        """Re-deliver buffered records to a just-rebuilt downstream task.

        Enqueues the still-unacknowledged records on the emit queue so they are
        resent in sequence order, serialized with live emits on the
        same FIFO consumer. Idempotent. Returns the number of replay ops enqueued.
        """
        if self._state is None or self._state.output is None or self._emit is None:
            return 0
        commands = self._state.output.replay_commands_for(downstream_name)
        if not commands:
            return 0
        self._state.output.refresh_downstream(downstream_name)
        await self._emit.enqueue_commands(commands)
        logger.warning(
            "Replaying %d buffered op(s) of %s to rebuilt downstream %s.",
            len(commands),
            self._task_name,
            downstream_name,
        )
        return len(commands)

    # --- end-of-stream / completion ---

    def _check_end_of_stream(self) -> bool:
        # A bounded sink (take(n)) that hit its limit requests a graceful drain
        # ONCE, then keeps running — it must stay alive to receive the upstream
        # EndOfData, align it, flush, and report FINISHED via the barrier path.
        if self._state.operator.end_of_stream and not self._drain_requested:
            logger.info(
                "Operator %s reached end of stream (e.g. take(n) limit); draining the job gracefully.",
                self._task_name,
            )
            self._drain_requested = True
            klein.get(self._job_manager.drain())
            return True
        return False

    def report_eof_finished(self) -> None:
        """Report FINISHED after the final EndOfData aligned (called from the pump)."""
        klein.get(
            self._job_manager.update_stream_task_status(
                self._vertex_id,
                ExecutionVertexStatus.FINISHED,
                task_name=self._task_name,
                task_generation=self._task_generation,
            )
        )
        self._eof_reached = True

    # --- failure / lifecycle ---

    def handle_exception(self, exc: Exception) -> None:
        logger.error("Stream task %s failed", self._task_name, exc_info=exc)
        error_message = current_exception_diagnostic()
        asyncio.get_running_loop().create_task(self._report_failure(error_message))

    async def _report_failure(self, error_message: str) -> None:
        try:
            await klein.aget(
                self._job_manager.update_stream_task_status(
                    self._vertex_id,
                    ExecutionVertexStatus.FAILED,
                    error_message,
                    self._task_name,
                    self._task_generation,
                )
            )
        finally:
            await self.stop()

    async def stop(self, timeout: float = 30.0) -> None:
        await self._stop_stream_task(timeout, release_rescale=True)

    async def retire_rescale(self, operation_id: str, timeout: float = 30.0) -> bool:
        """Stop a scale-in delta without reopening its fenced data path."""

        if self._rescale_operation_id != operation_id or self._rescale_role != "target":
            raise RuntimeError(f"{self._task_name} is not a retired target of rescale {operation_id}")
        await self._stop_stream_task(timeout, release_rescale=False)
        return True

    async def _stop_stream_task(self, timeout: float, *, release_rescale: bool) -> None:
        logger.debug("Stopping stream task %s", self._task_name)
        if release_rescale and self._rescale_resume_obj is not None:
            self._rescale_resume_obj.set()
        if self._pump is not None:
            self._pump.release_all_checkpoint_inputs()
        await super().stop(timeout)
        async with self._runtime_rescale_lock:
            await self._close_task_runtimes(timeout)

    async def _close_task_runtimes(self, timeout: float) -> None:
        errors: list[tuple[str, BaseException]] = []
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)

        def remaining() -> float:
            return max(0.0, deadline - asyncio.get_running_loop().time())

        transaction = self._runtime_rescale_transaction
        if transaction is not None:
            try:
                await self._close_runtime(
                    transaction.pending,
                    remaining(),
                    discard_backend=True,
                )
            except BaseException as error:
                errors.append(("pending runtime", error))
            finally:
                self._runtime_rescale_transaction = None
        retired_error = await self._await_retired_runtime_cleanup(remaining())
        if retired_error is not None:
            errors.append(("retired runtimes", retired_error))
        runtime = self._active_runtime
        if runtime is None:
            self._running = False
            logger.info("Stream task %s stopped without initialized runtime state", self._task_name)
            self._raise_runtime_cleanup_errors(errors)
            return
        try:
            await self._close_runtime(runtime, remaining(), discard_backend=False)
        except BaseException as error:
            errors.append(("active runtime", error))
        finally:
            self._clear_installed_runtime(runtime)
            self._running = False
            logger.info("Stream task %s stopped", self._task_name)
        self._raise_runtime_cleanup_errors(errors)

    def _raise_runtime_cleanup_errors(
        self,
        errors: list[tuple[str, BaseException]],
    ) -> None:
        if not errors:
            return
        summary = "; ".join(f"{phase}: {error}" for phase, error in errors)
        raise RuntimeError(f"Failed to close all runtimes for {self._task_name}: {summary}") from errors[0][1]

    def _get_name(self) -> str:
        return self._task_name

    def health_info(self) -> tuple[bool, str]:
        if not self.is_running():
            return False, f"Operator {self._task_name} is not running."
        return True, ""
