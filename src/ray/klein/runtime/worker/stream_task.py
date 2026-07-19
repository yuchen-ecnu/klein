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
from dataclasses import dataclass
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
from ray.klein.state.state_snapshot_reference import StateSnapshotReference

if TYPE_CHECKING:
    from ray.klein.runtime.job_manager.progress import SubtaskCounts
    from ray.klein.runtime.scheduler.task_deployment_descriptor import (
        TaskDeploymentDescriptor,
    )


logger = get_logger(__name__)


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
        runtime_context = self._build_runtime_context()
        runtime_context.checkpoint_strategy.open()
        delivery_mode = (
            DeliveryMode.INLINE
            if self._descriptor.operator.operator_type is OperatorType.SOURCE
            else DeliveryMode.PIPELINED
        )
        output = self._build_output(delivery_mode)
        operator = self._descriptor.operator.build(self._descriptor.output_queue)
        operator.open(output, runtime_context)

        input_buffer_max_bytes = runtime_context.config.get(PipelineOptions.INPUT_BUFFER_MAX_BYTES)
        emit_queue_max_batches = self._descriptor.config.get(PipelineOptions.EMIT_QUEUE_MAX_BATCHES)
        inbox = WeightedQueue(
            self._descriptor.input_buffer_size,
            inbox_envelope_rows,
            max_bytes=input_buffer_max_bytes,
            size_bytes=inbox_envelope_bytes,
        )
        # Single-thread executor so the (non-thread-safe) operator runs off loop.
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"{self._task_name}-op")
        task_metrics = TaskMetrics.create(
            self._descriptor.metric_group,
            self._descriptor.input_buffer_size,
            input_buffer_max_bytes,
            emit_queue_max_batches,
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
        state.state_snapshot_cache = self._build_state_snapshot_cache(operator)
        state.event_time_tracker = InputWatermarkTracker(self._descriptor.input_vertex_ids)
        self._state = state

        if output is not None:
            self._attach_output_metrics(output, state)

        await self._restore_operator_state(runtime_context)

        # Replay watermark + emit pipeline.
        self._watermark = self._build_watermark(operator, output)
        self._emit = EmitPipeline(
            output,
            self._watermark,
            self.handle_exception,
            self._task_name,
            queue_maxsize=emit_queue_max_batches,
            queue_size_observer=state.metrics.emit_queue_batches.set,
        )

        # The pump owns dispatch of records returned by the input accumulator.
        from ray.klein.config.event_time_options import EventTimeOptions

        idle_check_interval = self._descriptor.config.get(EventTimeOptions.IDLE_INPUT_CHECK_INTERVAL)
        if idle_check_interval.total_seconds() <= 0:
            raise ValueError("event-time.idle-input.check-interval must be greater than zero")
        self._pump = InboxPump(
            self,
            state,
            self._watermark,
            self._emit,
            inbox_timeout=idle_check_interval.total_seconds(),
        )
        self._watermark.bind(
            output,
            operator,
            executor,
            self._emit,
            self._pump.flush_input,
        )

        # Async operator: an ordered concurrency window lets up to
        # async_buffer_size requests run in flight while emission stays in input
        # order (see AsyncOrderedRunner). Sized from the operator's
        # async_buffer_size; the runner's FIFO consumer emits via the pump.
        if state.is_async_operator:
            capacity = runtime_context.runtime_info.async_buffer_size or 1
            state.async_runner = AsyncOrderedRunner(
                capacity=capacity,
                on_result=self._pump.on_async_result,
                on_fatal=self.handle_exception,
                task_name=self._task_name,
            )

        await self._on_setup_done(runtime_context)
        await self.start()
        self._running = True

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
        if descriptor.parallelism != self._descriptor.parallelism:
            raise ValueError("a rebuilt task cannot change its own parallelism")
        self._descriptor = descriptor
        await self.setup_and_run()

    def _build_watermark(self, operator: StreamOperator, output: TaskOutput | None) -> WatermarkController:
        from ray.klein.config.pipeline_options import PipelineOptions

        config = self._descriptor.config
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
                topology_epochs=tuple(edge.topology_epoch for edge in self._descriptor.out_edges),
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
                topology_epochs=tuple(edge.topology_epoch for edge in self._descriptor.out_edges),
            )
        else:
            mode = WatermarkMode.DISABLED
        return WatermarkController(mode, flush_interval_batches, namespace=self._descriptor.namespace)

    def _build_state_snapshot_cache(
        self,
        operator: StreamOperator,
    ) -> ObjectStoreSnapshotCache | None:
        if not operator.stateful:
            return None
        import ray
        from ray.klein.config.state_options import StateOptions

        enabled = self._descriptor.config.get(StateOptions.OBJECT_STORE_CACHE_ENABLED)
        # Debug mode has no Ray Object Store. The same codec remains exercised,
        # with snapshots kept inline.
        enabled = enabled and not klein.is_debug_mode()
        return ObjectStoreSnapshotCache(
            ray.put,
            klein.get,
            min_size_bytes=self._descriptor.config.get(StateOptions.OBJECT_STORE_CACHE_MIN_BYTES),
            enabled=enabled,
        )

    async def _restore_operator_state(
        self,
        runtime_context: TaskRuntimeContext,
    ) -> None:
        operator = self._runtime_state.operator
        cache = self._runtime_state.state_snapshot_cache
        if not operator.stateful or cache is None:
            return
        strategy = runtime_context.checkpoint_strategy
        restore_operation_id = self._descriptor.restore_operation_id
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
            if hot_restores:
                self._runtime_state.metrics.state_object_store_restores.inc(hot_restores)
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
            self._runtime_state.metrics.state_durable_restore_fallbacks.inc()
            payloads = await self._materialize_state(cache, durable_references)
        await self._apply_operator_state(operator, payloads)

    @staticmethod
    async def _materialize_state(cache: ObjectStoreSnapshotCache, references: tuple) -> tuple[bytes, ...]:
        return tuple(
            await asyncio.gather(*(asyncio.to_thread(cache.materialize, reference) for reference in references))
        )

    async def _apply_operator_state(self, operator: StreamOperator, payloads: tuple[bytes, ...]) -> None:
        if not isinstance(operator, ManagedStateOperator):
            raise TypeError(f"stateful operator {type(operator).__name__} must inherit ManagedStateOperator")
        await asyncio.get_running_loop().run_in_executor(
            self._runtime_state.executor,
            operator.restore_state_fragments,
            payloads,
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

    def _build_runtime_context(self) -> TaskRuntimeContext:
        from ray.klein.runtime.coordinator.checkpoint_strategy import (
            AlignedCheckpointStrategy,
        )

        coordinator = klein.get_actor_by_name(
            ComponentName.KLEIN_CHECKPOINT_COORDINATOR,
            namespace=self._descriptor.namespace,
        )
        checkpoint_strategy = AlignedCheckpointStrategy(
            coordinator,
            self._descriptor.barrier_split,
            self._descriptor.vertex_id,
            self._descriptor.operator.operator_type,
            self._descriptor.config,
            is_committer=self._descriptor.is_committer,
            synchronous_notify=self._descriptor.operator.transactional_sink,
            metric_group=self._descriptor.metric_group,
        )
        return TaskRuntimeContext(
            self._descriptor.task_name,
            self._descriptor.task_index,
            self._descriptor.parallelism,
            self._descriptor.config,
            self._descriptor.metric_group,
            checkpoint_strategy,
            self._descriptor.operator.runtime_info,
            self._descriptor.namespace,
        )

    def _build_output(self, delivery_mode: DeliveryMode) -> TaskOutput | None:
        edges = [self._build_output_edge(edge, delivery_mode) for edge in self._descriptor.out_edges]
        if not edges:
            return None
        return TaskOutput(edges)

    def _build_output_edge(self, edge, delivery_mode: DeliveryMode) -> EdgeOutput:
        targets = [
            klein.get_actor_by_name(name, namespace=self._descriptor.namespace) for name in edge.target_task_names
        ]
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
            namespace=self._descriptor.namespace,
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
            state.checkpoint_strategy.reconfigure_barrier_split(dict(descriptor.barrier_split))
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
            return False
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
            state.checkpoint_strategy.reconfigure_barrier_split(dict(previous.barrier_split))
        except Exception as error:
            failures.append(error)
            logger.exception("Failed to restore checkpoint alignment after topology rollback")
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

    def _begin_rescale(self, operation_id: str, role: str) -> None:
        if self._rescale_operation_id not in {None, operation_id}:
            raise RuntimeError(f"{self._task_name} already participates in rescale {self._rescale_operation_id}")
        self._rescale_operation_id = operation_id
        self._rescale_role = role
        self._rescale_seen_senders.clear()
        self._rescale_snapshot = None
        self._rescale_ready.clear()
        self._rescale_resume.clear()

    async def prepare_rescale_upstream(
        self,
        operation_id: str,
        target_operator_id: int,
        edge_indices: tuple[int, ...],
        timeout: float,
    ) -> bool:
        """Fence selected outputs after all earlier input and then pause."""

        self._begin_rescale(operation_id, "upstream")
        self._rescale_edge_indices = tuple(edge_indices)
        if not self._rescale_edge_indices:
            raise ValueError("an upstream rescale participant needs a target output edge")
        await self._runtime_state.inbox.put(InboxEnvelope(RescaleBarrier(operation_id, target_operator_id)))
        await asyncio.wait_for(self._rescale_ready.wait(), timeout=timeout)
        return True

    def prepare_rescale_target(self, operation_id: str) -> None:
        self._begin_rescale(operation_id, "target")
        self._rescale_expected_senders = set(self._descriptor.input_vertex_ids)
        if not self._rescale_expected_senders:
            raise ValueError("source operators cannot be locally rescaled")

    def prepare_rescale_downstream(
        self,
        operation_id: str,
        expected_senders: tuple[ExecutionVertexId, ...],
    ) -> None:
        self._begin_rescale(operation_id, "downstream")
        self._rescale_expected_senders = set(expected_senders)
        if not self._rescale_expected_senders:
            raise ValueError("a downstream rescale participant needs at least one target input")

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
        if self._rescale_operation_id != operation_id:
            return False
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

    async def emit_barrier(self, barrier: Barrier) -> int:
        if self._state is None:
            return 0
        await self._state.inbox.put(InboxEnvelope(barrier))
        self._state.metrics.barriers_in.inc()
        return self._update_buffer_size_metrics()

    async def emit_stream_control(
        self,
        control: StreamControl,
        sender_vertex_id: object = None,
    ) -> int:
        """Enqueue an ordered event-time control from one physical input."""

        if self._state is None:
            return 0
        await self._state.inbox.put(InboxEnvelope(control, sender_vertex_id))
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
        try:
            if timeout is None:
                await self._state.inbox.put(InboxEnvelope(record, sender_vertex_id, batch_sequence, delivery_channel))
            else:
                await asyncio.wait_for(
                    self._state.inbox.put(InboxEnvelope(record, sender_vertex_id, batch_sequence, delivery_channel)),
                    timeout=timeout,
                )
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
        return SubtaskCounts(
            rows_in=self._state.operator.records_in,
            rows_out=self._state.operator.records_out,
            bytes_in=self._state.operator.bytes_in,
            bytes_out=self._state.operator.bytes_out,
            queued=self._state.inbox.qsize(),
            capacity=self._descriptor.input_buffer_size,
            busy_ns=self._state.operator.processing_duration_ns,
            backpressure_ns=(0 if self._state.output is None else self._state.output.backpressure_duration_ns),
            backpressure_events=(0 if self._state.output is None else self._state.output.backpressure_events),
            barriers_in=int(self._state.metrics.barriers_in.value),
            barriers_out=int(self._state.metrics.barriers_out.value),
            checkpoint_alignment_ms=self._state.checkpoint_strategy.last_alignment_duration_ms,
            checkpoint_barrier_latency_ms=self._state.metrics.checkpoint_barrier_latency_ms.last,
            checkpoint_state_size_bytes=self._last_checkpoint_state_size_bytes,
            last_checkpoint_id=self._last_checkpoint_id,
        )

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
        logger.debug("Stopping stream task %s", self._task_name)
        if self._rescale_resume_obj is not None:
            self._rescale_resume_obj.set()
        await super().stop(timeout)
        if self._state is None:
            self._running = False
            logger.info("Stream task %s stopped without initialized runtime state", self._task_name)
            return
        try:
            # Shut the async runner down first so any still-in-flight result is
            # emitted while the emit pipeline below is still alive to drain it.
            if self._state.async_runner is not None:
                await self._state.async_runner.shutdown(timeout)
            if self._emit is not None:
                await self._emit.shutdown(timeout)
        finally:
            if self._state.operator is not None:
                await asyncio.get_running_loop().run_in_executor(
                    self._state.executor,
                    self._state.operator.close,
                )
            self._state.executor.shutdown(wait=False)
            self._running = False
            logger.info("Stream task %s stopped", self._task_name)

    def _get_name(self) -> str:
        return self._task_name

    def health_info(self) -> tuple[bool, str]:
        if not self.is_running():
            return False, f"Operator {self._task_name} is not running."
        return True, ""
