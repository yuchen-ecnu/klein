# SPDX-License-Identifier: Apache-2.0
"""OutputCollector — orchestrates record emission from one operator downstream.

This is a thin coordinator over four single-responsibility components:

* :class:`Router`        — partition a record to ``(target_index, payload)`` pairs.
* :class:`OutputBatcher` — per-target micro-batching before the wire.
* :class:`ReplayBuffer`  — sequence assignment + replay FIFO + forwarded watermark.
* :class:`EmitEngine`    — the actual send (sync inline / async pipelined),
                           retry/reroute, and replay commit.

Two emit modes (set via :meth:`configure_pipelining`):

* **inline** (sources, tests): ``collect()`` runs on the executor thread and
  sends synchronously via ``EmitEngine.send_sync`` (blocking ``klein.get`` is the
  backpressure signal).
* **pipelined** (non-source tasks): ``collect()`` only *buffers* emit-ops into
  ``_pending`` on the executor thread; the StreamTask pump detaches them on the
  actor loop and awaits :meth:`aemit`, so process(N+1) overlaps emit(N).

``num_records_out`` is counted here, the single chokepoint every emitted record
crosses, including direct source-context emissions.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.collector import Collector
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.collector.batcher import OutputBatcher
from ray.klein.runtime.collector.emit_engine import EmitEngine
from ray.klein.runtime.collector.replay_buffer import ReplayBuffer
from ray.klein.runtime.collector.router import Router
from ray.klein.runtime.context.runtime_context import OperatorRuntimeContext
from ray.klein.runtime.message import Barrier, Record, StreamControl
from ray.klein.runtime.partitioning.partitioner import Partitioner


class _EmitOp:
    """Typed operation transferred from the executor to the actor loop."""


@dataclass(frozen=True, slots=True)
class _DataEmit(_EmitOp):
    target: int
    records: tuple[Record, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", tuple(self.records))


@dataclass(frozen=True, slots=True)
class _BarrierEmit(_EmitOp):
    barrier: Barrier


@dataclass(frozen=True, slots=True)
class _ControlEmit(_EmitOp):
    control: StreamControl


@dataclass(frozen=True, slots=True)
class _ReplayEmit(_EmitOp):
    target: int
    sequence: int
    records: tuple[Record, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", tuple(self.records))


@dataclass(frozen=True, slots=True)
class _ChildEmit(_EmitOp):
    child_index: int
    operation: _EmitOp


logger = get_logger(__name__)


class OutputCollector(Collector):
    """Routes records from one operator to its downstream tasks."""

    def __init__(
        self,
        target_tasks: list[KleinActorHandle],
        partitioner: Partitioner,
        output_buffer_size: int,
        target_operator_names: list[str],
        put_timeout: int,
    ) -> None:
        super().__init__()
        self._target_tasks = target_tasks
        self._partitioner_func = partitioner
        self._output_buffer_size = output_buffer_size
        self._target_operator_names = target_operator_names
        self._put_timeout = put_timeout

        n = len(target_tasks)
        self._router = Router(partitioner, target_tasks)
        self._replay = ReplayBuffer(n)
        self._engine = EmitEngine(
            target_tasks,
            target_operator_names,
            self._router,
            self._replay,
            put_timeout,
        )
        # Built for real in open() with the configured internal batch size + an
        # idle-flush threshold derived from batch_timeout. Until then use an
        # unbatched holder (batch_size=1 -> every record flushes immediately), so
        # the rare pre-open path (e.g. the _collect test helper) still works.
        self._batcher: OutputBatcher = OutputBatcher(n, 1)
        # Emit pipelining (see configure_pipelining). When True, collect() (executor
        # thread) buffers emit-ops into _pending instead of sending inline.
        self._pipelined: bool = False
        self._pending: list[_EmitOp] = []

    def open(self, op_runtime_context: OperatorRuntimeContext, register_metric: bool = True) -> None:
        super().open(op_runtime_context, register_metric=register_metric)
        self._router.open(op_runtime_context)
        op_config: Configuration = op_runtime_context.config
        internal_batch_size = op_config.get(PipelineOptions.INTERNAL_BATCH_SIZE)
        if internal_batch_size < 0:
            raise ValueError("internal batch size cannot be negative")
        # Idle-flush threshold tracks the operator's batch_timeout (so buffered
        # micro-batches don't linger longer than the configured latency budget),
        # falling back to 3s when batching/timeout isn't configured.
        batch_timeout = op_runtime_context.runtime_info.batch_timeout
        idle_flush_seconds = float(batch_timeout) if batch_timeout else 3.0
        self._batcher = OutputBatcher(len(self._target_tasks), internal_batch_size, idle_flush_seconds)

    def configure_pipelining(self, pipelined: bool) -> None:
        self._pipelined = pipelined

    def attach_runtime_metrics(
        self,
        replay_size_observer: Callable[[int], None],
        backpressure_events: Counter,
        backpressure_duration_ms: Histogram,
    ) -> None:
        self._replay.observe_size_with(replay_size_observer)
        self._engine.attach_backpressure_metrics(backpressure_events, backpressure_duration_ms)

    @property
    def replay_buffered_records(self) -> int:
        return self._replay.buffered_record_count

    @property
    def backpressure_events(self) -> int:
        return self._engine.backpressure_events

    @property
    def backpressure_duration_ns(self) -> int:
        return self._engine.backpressure_duration_ns

    def configure_replay(self, enabled: bool, sender_vertex_id=None) -> None:
        """Turn on replay buffering for a non-sink task.

        ``sender_vertex_id`` identifies this task to downstreams so they can key the
        per-sender forwarded watermark. Called from StreamTask.setup_and_run.
        """
        self._replay.enable(enabled, sender_vertex_id)

    # --- collect (executor thread) ---

    def collect(self, record: Record) -> None:
        # Count every data record that enters the emit path — the one chokepoint
        # crossed by both operator emissions and direct source-context emissions.
        if not isinstance(record, Barrier | StreamControl):
            self._count_out(self._record_rows(record))
        if not self._pipelined:
            self._collect_inline(record)
            return

        # Drain loop-side congestion feedback on THIS (executor) thread, then let
        # the partitioner react to it — backpressure-driven load shift, without
        # the loop thread ever touching partitioner state.
        self._engine.drain_emit_timeouts()

        # Pipelined: buffer emit-ops; no network here. All partitioner/ring
        # decisions happen on this (executor) thread so the loop-side send never
        # touches partitioner state — no lock, no race.
        if isinstance(record, Barrier):
            for i, batches in self._batcher.pop():
                self._pending.append(_DataEmit(i, batches))
            self._pending.append(_BarrierEmit(record))
            return
        if isinstance(record, StreamControl):
            for i, batches in self._batcher.pop(force=True):
                self._pending.append(_DataEmit(i, batches))
            self._pending.append(_ControlEmit(record))
            return
        for target_index, routed in self._router.route(record):
            self._batcher.push(target_index, routed)
            batches: list[Record] = self._batcher.pop_when_reach_limit(target_index)
            if batches:
                self._pending.append(_DataEmit(target_index, batches))
                # Advance the ring at batch boundaries on the executor thread.
                self._router.on_record_emitted(target_index, 0)

    def _collect_inline(self, record: Record) -> None:
        if isinstance(record, Barrier):
            for i, batches in self._batcher.pop():
                self._engine.send_sync(i, batches)
            self._engine.broadcast_sync(record, self._parallelism, self._subtask_index)
            return
        if isinstance(record, StreamControl):
            for i, batches in self._batcher.pop(force=True):
                self._engine.send_sync(i, batches)
            self._engine.broadcast_control_sync(record, self._parallelism, self._subtask_index)
            return
        for target_index, routed in self._router.route(record):
            self._batcher.push(target_index, routed)
            batches: list[Record] = self._batcher.pop_when_reach_limit(target_index)
            if batches:
                self._engine.send_sync(target_index, batches)

    # --- flush / close ---

    def flush(self, force: bool = False) -> None:
        """Flush buffered micro-batches (executor thread, on idle / watermark).

        Pipelined: move the popped batches into _pending so the pump's aemit
        drains them on the loop. Inline: send directly. ``force`` empties the
        batcher unconditionally (used by the replay-watermark flush, which must
        guarantee all processed output has physically left the task before the
        upstream is told it may drop those records).
        """
        if self._pipelined:
            for i, batches in self._batcher.pop(force=force):
                self._pending.append(_DataEmit(i, batches))
        else:
            for i, batches in self._batcher.pop(force=force):
                self._engine.send_sync(i, batches)

    def close(self) -> None:
        logger.debug("Closing output collector for task %s", self._task_name)
        # Flush any buffered micro-batches downstream before shutting down.
        # close() runs on the executor thread during teardown; send inline so the
        # final batches land even though the pump's aemit loop has stopped.
        for i, batches in self._batcher.pop():
            self._engine.send_sync(i, batches)

    # --- pipelined drain (actor loop) ---

    def detach_pending(self) -> list[_EmitOp]:
        """Atomically take the buffered emit-ops (loop thread, executor idle).

        Called by the StreamTask pump right after run_in_executor(process)
        returns — the executor is idle, so swapping _pending out is race-free.
        """
        pending = self._pending
        self._pending = []
        return pending

    async def aemit(self, pending: list[_EmitOp]) -> None:
        """Drain detached emit-ops on the actor loop, in FIFO order.

        Ordering: ops are awaited sequentially, so emit order == collect order
        (data before the barrier that followed it -> checkpoint alignment holds).
        Backpressure: a full downstream suspends ``put`` without blocking the
        loop's other RPCs.
        """
        for operation in pending:
            if isinstance(operation, _BarrierEmit):
                await self._engine.broadcast_async(operation.barrier, self._parallelism, self._subtask_index)
            elif isinstance(operation, _ControlEmit):
                await self._engine.broadcast_control_async(operation.control, self._parallelism, self._subtask_index)
            elif isinstance(operation, _ReplayEmit):
                # Re-send to the same index with the original sequence; do not
                # re-buffer (it's already buffered) and do NOT advance the ring.
                await self._engine.send_async(
                    operation.target,
                    operation.records,
                    is_replay=True,
                    replay_sequence=operation.sequence,
                )
            elif isinstance(operation, _DataEmit):
                await self._engine.send_async(operation.target, operation.records)
            else:
                raise TypeError(f"OutputCollector cannot emit {type(operation).__name__}")

    # --- replay / recovery surface (called by StreamTask) ---

    def replay_ops_for_name(self, downstream_name: str) -> list[_EmitOp]:
        """Ready-to-enqueue replay operations for one downstream.

        StreamTask enqueues these on the emit-queue so re-delivery is serialized
        with normal emits on the same FIFO consumer, in sequence order. Empty if this
        collector doesn't target the name.
        """
        try:
            target_index = self._target_operator_names.index(downstream_name)
        except ValueError:
            return []
        return [
            _ReplayEmit(target_index, sequence, records)
            for sequence, records in self._replay.buffered_for(target_index)
        ]

    def advance_forwarded(self, target_index: int, forwarded_sequence: int) -> None:
        """Drop replay-buffer entries the downstream has confirmed forwarding."""
        self._replay.advance_forwarded(target_index, forwarded_sequence)

    def reresolve_target(self, target_index: int) -> None:
        """Re-resolve a (possibly rebuilt) downstream handle by index."""
        self._engine.reresolve_target(target_index)

    def reresolve_by_name(self, downstream_name: str) -> None:
        """Re-resolve a downstream handle given its actor name (no-op if absent)."""
        self._engine.reresolve_by_name(downstream_name)

    # --- test-only helpers (kept for the replay-buffer / partitioner suites) ---

    async def _aemit_records(
        self,
        target_index: int,
        records: list[Record],
        is_replay: bool = False,
        replay_sequence: int | None = None,
    ) -> None:
        await self._engine.send_async(
            target_index,
            records,
            is_replay=is_replay,
            replay_sequence=replay_sequence,
        )

    def _emit_records(self, target_index: int, records: list[Record], is_replay: bool = False) -> None:
        self._engine.send_sync(target_index, records, is_replay=is_replay)

    def _collect(self, record: Record) -> None:
        # only for test: emit a single bare record (no micro-batching, no list
        # wrapping) straight through the partitioner — mirrors the sync helper
        # the partitioner suite drives.
        import ray.klein as klein

        for initial_target in self._partitioner_func.partition(record):
            target_task = initial_target
            success, buffer_size = False, 0
            while success is False:
                ack = klein.get(self._target_tasks[target_task].put(record, timeout=self._put_timeout))
                success, buffer_size = ack.accepted, ack.buffer_size
                if not success:
                    target_task = self._partitioner_func.on_record_emit_timeout(record, target_task, buffer_size)
            self._partitioner_func.on_record_emitted(target_task, buffer_size)

    # --- serialization (descriptor ships the collector to the worker) ---

    def __reduce__(self) -> tuple[type["OutputCollector"], tuple[Any, ...]]:
        return OutputCollector, (
            self._target_tasks,
            self._partitioner_func,
            self._output_buffer_size,
            self._target_operator_names,
            self._put_timeout,
        )

    def _get_name(self) -> str:
        return f"[{self._task_name}] OutputCollector to {self._target_operator_names}"

    @property
    def healthy(self) -> bool:
        return True


class CollectionCollector(Collector):
    """Combination of multiple collectors (operator fan-out to multiple edges)."""

    def __init__(self, collectors: list[Collector]) -> None:
        super().__init__()
        self.collectors: list[Collector] = collectors

    def open(self, op_runtime_context: OperatorRuntimeContext, register_metric: bool = True) -> None:
        # The parent owns the single operator-level num_records_out metric; the
        # children share this OperatorMetricGroup, so they must NOT re-register it
        # (that collides and gets dropped). They still count into their readable
        # int, which records_out() aggregates via max() across children.
        super().open(op_runtime_context)
        for collector in self.collectors:
            collector.open(op_runtime_context, register_metric=False)

    def collect(self, record: Record) -> None:
        # Count once here (the parent owns the Prometheus num_records_out metric;
        # children don't register it). Every child receives the same record, so
        # one count per record reflects this operator's true output.
        if not isinstance(record, Barrier | StreamControl):
            self._count_out(self._record_rows(record))
        for collector in self.collectors:
            collector.collect(record)

    @property
    def records_out(self) -> int:
        # Each child receives every record (fan-out to multiple downstream
        # operators), so one child's count already equals records emitted by
        # this operator — take the max rather than summing across children.
        return max((collector.records_out for collector in self.collectors), default=0)

    def configure_pipelining(self, pipelined: bool) -> None:
        for collector in self.collectors:
            collector.configure_pipelining(pipelined)

    def attach_runtime_metrics(
        self,
        replay_size_observer: Callable[[int], None],
        backpressure_events: Counter,
        backpressure_duration_ms: Histogram,
    ) -> None:
        def publish_total(_value: int) -> None:
            replay_size_observer(self.replay_buffered_records)

        for collector in self.collectors:
            collector.attach_runtime_metrics(
                publish_total,
                backpressure_events,
                backpressure_duration_ms,
            )

    @property
    def replay_buffered_records(self) -> int:
        return sum(collector.replay_buffered_records for collector in self.collectors)

    @property
    def backpressure_events(self) -> int:
        return sum(collector.backpressure_events for collector in self.collectors)

    @property
    def backpressure_duration_ns(self) -> int:
        return sum(collector.backpressure_duration_ns for collector in self.collectors)

    def configure_replay(self, enabled: bool, sender_vertex_id=None) -> None:
        for collector in self.collectors:
            collector.configure_replay(enabled, sender_vertex_id)

    def replay_ops_for_name(self, downstream_name: str) -> list[_EmitOp]:
        # Tag each operation with its child so aemit routes it back correctly.
        tagged: list[_EmitOp] = []
        for child_index, collector in enumerate(self.collectors):
            tagged.extend(
                _ChildEmit(child_index, operation) for operation in collector.replay_ops_for_name(downstream_name)
            )
        return tagged

    def reresolve_by_name(self, downstream_name: str) -> None:
        for collector in self.collectors:
            collector.reresolve_by_name(downstream_name)

    def detach_pending(self) -> list[_EmitOp]:
        # Tag each operation with its child so aemit routes it back correctly.
        tagged: list[_EmitOp] = []
        for child_index, collector in enumerate(self.collectors):
            tagged.extend(_ChildEmit(child_index, operation) for operation in collector.detach_pending())
        return tagged

    async def aemit(self, pending: list[_EmitOp]) -> None:
        # Regroup per child (preserving order) and delegate.
        per_child: dict[int, list[_EmitOp]] = {}
        order: list[int] = []
        for operation in pending:
            if not isinstance(operation, _ChildEmit):
                raise TypeError(f"CollectionCollector cannot emit {type(operation).__name__}")
            child_index = operation.child_index
            if child_index not in per_child:
                per_child[child_index] = []
                order.append(child_index)
            per_child[child_index].append(operation.operation)
        for child_index in order:
            await self.collectors[child_index].aemit(per_child[child_index])

    def flush(self, force: bool = False) -> None:
        for collector in self.collectors:
            collector.flush(force=force)

    def close(self) -> None:
        for collector in self.collectors:
            collector.close()

    @property
    def healthy(self) -> bool:
        return all(collector.healthy for collector in self.collectors)
