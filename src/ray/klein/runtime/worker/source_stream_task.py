# SPDX-License-Identifier: Apache-2.0
import asyncio
import threading
import time
from collections import deque
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import ray.cloudpickle as cloudpickle

from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.message import MAX_WATERMARK, Barrier, EndOfData, RescaleBarrier, Watermark
from ray.klein.runtime.operator.source import SourceOperator
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry

if TYPE_CHECKING:
    from ray.klein.runtime.scheduler.task_deployment_descriptor import (
        TaskDeploymentDescriptor,
    )


logger = get_logger(__name__)


class SourceStreamTask(StreamTask):
    """Async source actor.

    A source has no inbox — instead of consuming a queue it *produces* data by
    running the (blocking) source loop ``operator.run()``. That loop is driven on
    the StreamTask executor thread so it doesn't pin the actor event loop, while
    the actor stays responsive to checkpoint and health-check RPCs. Barrier
    generation and downstream emission happen on that same executor thread,
    consistent with inline ``TaskOutput`` delivery.
    """

    def __init__(self, descriptor: "TaskDeploymentDescriptor") -> None:
        super().__init__(descriptor)
        self._inflight_source_states: dict[int, Any] = {}
        self._source_rescale_requested = threading.Event()
        self._source_rescale_resume = threading.Event()
        self._source_rescale_loop: asyncio.AbstractEventLoop | None = None
        self._source_rescale_barrier: RescaleBarrier | None = None
        # Set by the scheduler after a local topology commit. The source thread
        # consumes it at its next record/idle boundary, where emitting an
        # ordinary checkpoint barrier is ordered with source records.
        self._forced_checkpoint_requested = threading.Event()
        # Track the coordinated epoch so a timeout can request a replacement.
        # Barrier ordering provides the consistent cut; the source does not
        # need to stop producing while metadata becomes durable.
        self._coordinated_checkpoint_barrier_id: int | None = None
        self._source_shutdown_requested = threading.Event()
        # Distinct from the externally visible FINISHED status. Set as soon as
        # operator.run() returns so a concurrent rescale cannot arm a source
        # whose producer loop has already exited but is still reporting status.
        self._source_exhausted = threading.Event()
        self._requested_checkpoint_ids: deque[int] = deque()
        self._checkpoint_request_lock = threading.Lock()
        self._resolved_checkpoint_floor = 0
        self._checkpoint_wait_stop = threading.Event()

    @property
    def _source_operator(self) -> SourceOperator:
        operator = self._state.operator
        if not isinstance(operator, SourceOperator):
            raise TypeError("SourceStreamTask requires a SourceOperator")
        return operator

    async def _on_setup_done(self, runtime_context: RuntimeContext) -> None:
        # Source delivery mode is fixed to INLINE when TaskOutput is built.
        source_exhausted = getattr(self, "_source_exhausted", None)
        if source_exhausted is None:
            source_exhausted = self._source_exhausted = threading.Event()
        source_exhausted.clear()
        source_shutdown = getattr(self, "_source_shutdown_requested", None)
        if source_shutdown is None:
            source_shutdown = self._source_shutdown_requested = threading.Event()
        source_shutdown.clear()
        self._checkpoint_wait_stop.clear()
        source_operator = self._source_operator
        source_operator.bind_record_emitter(self._on_records_emitted)
        self._source_rescale_loop = asyncio.get_running_loop()
        # Restore progress from the coordinator — works identically on a fresh
        # deploy and on a single-source restart (the coordinator holds the
        # latest source-owned state). Awaited so setup never blocks the actor
        # event loop.
        entry: SourceCheckpointEntry | None = await self._state.checkpoint_strategy.restore_source_state_async()
        if entry is not None:
            logger.debug("Restoring source state from checkpoint %d", entry.checkpoint_id)
            source_operator.restore_state(entry.state)

    async def _run(self) -> None:
        # Run the (blocking) source loop on the executor thread. It returns when
        # the source is exhausted (bounded) or never (unbounded, until cancel).
        await asyncio.get_running_loop().run_in_executor(self._state.executor, self._run_source)
        if self._eof_reached:
            await self.report_eof_finished()
        await self.stop()

    async def stop(self, timeout: float = 30.0) -> None:
        """Stop both the actor task and its blocking source thread.

        Cancelling the asyncio future returned by ``run_in_executor`` does not
        stop the function already running in that thread. Interrupt the source
        first so an unbounded source can leave its loop before the common task
        teardown releases the executor.
        """
        if self._state is not None and self._state.operator is not None:
            self._source_operator.interrupt()
        source_shutdown = getattr(self, "_source_shutdown_requested", None)
        if source_shutdown is None:
            source_shutdown = self._source_shutdown_requested = threading.Event()
        source_shutdown.set()
        source_exhausted = getattr(self, "_source_exhausted", None)
        if source_exhausted is None:
            source_exhausted = self._source_exhausted = threading.Event()
        source_exhausted.set()
        self._checkpoint_wait_stop.set()
        self._source_rescale_resume.set()
        await super().stop(timeout)

    async def prepare_rescale_upstream(
        self,
        operation_id: str,
        target_operator_id: int,
        edge_indices: tuple[int, ...],
        timeout: float,
    ) -> bool:
        requested_edges = tuple(edge_indices)
        if not requested_edges:
            raise ValueError("an upstream rescale participant needs a target output edge")
        barrier = RescaleBarrier(operation_id, target_operator_id)
        new_participant = self._begin_rescale(operation_id, "upstream")
        if not new_participant:
            if self._rescale_edge_indices != requested_edges:
                raise ValueError(f"{self._task_name} retried rescale {operation_id} with different output edges")
            await asyncio.wait_for(self._rescale_ready.wait(), timeout=timeout)
            return True
        self._rescale_edge_indices = requested_edges
        self._source_rescale_barrier = barrier
        self._source_rescale_resume.clear()
        self._source_rescale_requested.set()
        await asyncio.wait_for(self._rescale_ready.wait(), timeout=timeout)
        return True

    def resume_rescale(self, operation_id: str) -> bool:
        if operation_id in self._rescale_tombstones:
            # The first response may have been lost after the source thread was
            # already released. Match the base task's idempotent RPC contract.
            return True
        if self._rescale_operation_id is None:
            return True
        if self._rescale_operation_id != operation_id:
            return False
        self._source_rescale_requested.clear()
        self._source_rescale_barrier = None
        resumed = super().resume_rescale(operation_id)
        self._source_rescale_resume.set()
        return resumed

    def _run_source(self) -> None:
        """Executor-thread body: drive the source operator to completion.

        ``operator.run()`` returns when the source is exhausted, or when a drain
        request set the fn's interrupt flag (graceful take(n) / shutdown). Either
        way we emit a single EndOfData so the downstream alignment chain can
        commit a final checkpoint and reach FINISHED.
        """
        self._source_operator.run()
        source_exhausted = getattr(self, "_source_exhausted", None)
        if source_exhausted is None:
            source_exhausted = self._source_exhausted = threading.Event()
        source_exhausted.set()
        if self._checkpoint_wait_stop.is_set():
            return
        if self._state.output is not None:
            self._state.output.collect(Watermark(MAX_WATERMARK))
            checkpoint_id = self._pop_requested_checkpoint()
            if checkpoint_id is None:
                while self._inflight_source_states and not self._checkpoint_wait_stop.is_set():
                    self._emit_pending_rescale_barrier()
                    time.sleep(0.01)
                if self._checkpoint_wait_stop.is_set():
                    return
            barrier = self._await_terminal_barrier(checkpoint_id)
            while barrier is not None and barrier.coordinated:
                self._emit_checkpoint_barrier(barrier)
                barrier = self._await_terminal_barrier(self._pop_requested_checkpoint())
            if barrier is None:
                return
            self._emit_checkpoint_barrier(barrier)
        # Close the arm-vs-FINISHED race before returning to the actor loop,
        # which performs the status RPC asynchronously.
        self._eof_reached = True

    def _await_end_of_data_barrier(self) -> Barrier | None:
        """Wait out a short rescale checkpoint gate without failing the source."""

        while not self._source_shutdown_requested.is_set():
            barrier = self._generate_barrier(is_eof=True)
            if barrier is not None:
                return barrier
            time.sleep(0.05)
        return None

    def request_drain(self) -> None:
        """Cooperatively stop producing (graceful take(n) / drain).

        Sets the source fn's interrupt flag so run() returns; _run_source then
        emits a normal EndOfData and the standard alignment chain drives the job
        to FINISHED. Idempotent and cheap — safe to call from the JobManager RPC.
        """
        self._source_operator.interrupt()

    def request_checkpoint(self, checkpoint_id: int | None = None) -> bool:
        """Request a local or coordinator-assigned checkpoint at the next boundary."""

        source_exhausted = getattr(self, "_source_exhausted", None)
        if not self._running or self._eof_reached or (source_exhausted is not None and source_exhausted.is_set()):
            return False
        if checkpoint_id is not None:
            if isinstance(checkpoint_id, bool) or not isinstance(checkpoint_id, int):
                raise TypeError("checkpoint_id must be an integer or None")
            if checkpoint_id <= 0:
                raise ValueError("checkpoint_id must be greater than zero")
            with self._checkpoint_request_lock:
                if checkpoint_id <= self._resolved_checkpoint_floor:
                    return False
                if checkpoint_id in self._inflight_source_states or checkpoint_id in self._requested_checkpoint_ids:
                    return False
                self._requested_checkpoint_ids.append(checkpoint_id)
            return True
        self._forced_checkpoint_requested.set()
        return True

    def _pop_requested_checkpoint(self) -> int | None:
        queue = getattr(self, "_requested_checkpoint_ids", None)
        lock = getattr(self, "_checkpoint_request_lock", None)
        if queue is None or lock is None:
            return None
        with lock:
            while queue:
                barrier_id = queue.popleft()
                if barrier_id <= getattr(self, "_resolved_checkpoint_floor", 0):
                    continue
                if barrier_id in self._inflight_source_states:
                    continue
                return barrier_id
            return None

    def _discard_requested_checkpoint(self, barrier_id: int) -> None:
        queue = getattr(self, "_requested_checkpoint_ids", None)
        lock = getattr(self, "_checkpoint_request_lock", None)
        if queue is None or lock is None:
            return
        with lock, suppress(ValueError):
            queue.remove(barrier_id)

    def _remember_resolved_checkpoint(self, barrier_id: int) -> None:
        lock = getattr(self, "_checkpoint_request_lock", None)
        if lock is None:
            return
        with lock:
            self._resolved_checkpoint_floor = max(
                getattr(self, "_resolved_checkpoint_floor", 0),
                barrier_id,
            )

    def notify_source_checkpoint_complete(self, barrier_id: int) -> tuple[bool, Any]:
        return self._notify_source_checkpoint_complete(barrier_id)

    def source_checkpoint_state(self, barrier_id: int) -> tuple[bool, Any]:
        """Peek source state; ownership is released only after durability."""

        if barrier_id not in self._inflight_source_states:
            return False, None
        return True, self._inflight_source_states[barrier_id]

    def discard_source_checkpoint(self, barrier_id: int) -> bool:
        """Release source state held for a barrier that will not commit.

        The coordinator expires a checkpoint whose alignment never completed
        (lost ack, crashed subtask). Without this, the progress captured at
        ``_generate_barrier`` time stays pinned in ``_inflight_barriers``
        forever — an unbounded leak in a long-running source. Idempotent: a
        no-op if the barrier was already committed or discarded.
        """
        existed = barrier_id in self._inflight_source_states
        self._inflight_source_states.pop(barrier_id, None)
        self.discard_checkpoint(barrier_id)
        self._discard_requested_checkpoint(barrier_id)
        self._remember_resolved_checkpoint(barrier_id)
        coordinated_barrier_id = getattr(self, "_coordinated_checkpoint_barrier_id", None)
        if coordinated_barrier_id == barrier_id and self._running and not self._eof_reached:
            self._forced_checkpoint_requested.set()
        self._resume_coordinated_checkpoint(barrier_id)
        if existed:
            logger.debug("Discarded source state for checkpoint barrier %s", barrier_id)
        return existed

    async def abort_checkpoint(self, barrier_id: int) -> bool:
        # The source's sole executor thread runs the connector loop, so the
        # base implementation must not submit aligner work to that executor.
        # Sources have no inbox admission gates; releasing state/request state
        # is sufficient and stays responsive on the actor loop.
        return self.discard_source_checkpoint(barrier_id)

    def reset_inflight_before(self, cutoff_barrier_id: int) -> int:
        """Drop in-flight barriers from a previous coordinator epoch (<= cutoff).

        When the checkpoint coordinator is rebuilt (Tier-1 failover), the source
        keeps running with old-epoch barrier ids still pinned in
        _inflight_barriers — but the rebuilt coordinator has no record of them
        and will never ack, so they would leak forever. The rebuilt coordinator
        re-seeds barrier ids strictly above its epoch floor, so every orphan has
        an id <= that floor; the scheduler passes the floor here. Returns the
        number reclaimed. Idempotent and cheap.
        """
        stale = [barrier_id for barrier_id in self._inflight_source_states if barrier_id <= cutoff_barrier_id]
        for barrier_id in stale:
            self._inflight_source_states.pop(barrier_id, None)
            self._discard_requested_checkpoint(barrier_id)
        queue = getattr(self, "_requested_checkpoint_ids", None)
        lock = getattr(self, "_checkpoint_request_lock", None)
        if queue is not None and lock is not None:
            with lock:
                retained = (barrier_id for barrier_id in queue if barrier_id > cutoff_barrier_id)
                self._requested_checkpoint_ids = deque(retained)
        self._remember_resolved_checkpoint(cutoff_barrier_id)
        coordinated_barrier_id = getattr(self, "_coordinated_checkpoint_barrier_id", None)
        if coordinated_barrier_id is not None and coordinated_barrier_id <= cutoff_barrier_id:
            self._resume_coordinated_checkpoint(coordinated_barrier_id)
        reset_result = self._state.checkpoint_strategy.reset_inflight_before(cutoff_barrier_id)
        aligned = reset_result if isinstance(reset_result, int) else 0
        pump = getattr(self, "_pump", None)
        if pump is not None:
            aligned += pump.reset_inflight_before(cutoff_barrier_id)
        if stale or aligned:
            logger.info(
                "Reclaimed %d orphan in-flight barriers through checkpoint %d after coordinator recovery",
                len(stale) + aligned,
                cutoff_barrier_id,
            )
        return len(stale) + aligned

    def _notify_source_checkpoint_complete(self, barrier_id: int) -> tuple[bool, Any]:
        try:
            if barrier_id not in self._inflight_source_states:
                logger.warning("Ignoring unknown checkpoint barrier %s", barrier_id)
                return False, None
            state = self._inflight_source_states.pop(barrier_id)
            self._remember_resolved_checkpoint(barrier_id)
            return True, state
        except Exception:
            logger.exception("Checkpoint completion notification failed for barrier %s", barrier_id)
            return False, None

    def _on_records_emitted(self, record_emitted: bool, record_count: int = 1) -> Barrier | None:
        if super()._check_end_of_stream():
            return None
        self._emit_pending_rescale_barrier()
        requested_checkpoint_id = self._pop_requested_checkpoint()
        if requested_checkpoint_id is not None:
            barrier = self._generate_barrier(checkpoint_id=requested_checkpoint_id)
            if barrier:
                self._forced_checkpoint_requested.clear()
                reset_trigger = getattr(self._state.checkpoint_strategy, "reset_trigger", None)
                if callable(reset_trigger):
                    reset_trigger()
                self._state.metrics.barriers_out.inc()
                if barrier.coordinated:
                    self._emit_checkpoint_barrier(barrier)
                    return None
            return barrier

        force_checkpoint = self._forced_checkpoint_requested.is_set()
        if force_checkpoint or self._state.checkpoint_strategy.should_trigger(record_emitted, record_count):
            barrier = self._generate_barrier(force=force_checkpoint)
            if barrier:
                if force_checkpoint:
                    self._forced_checkpoint_requested.clear()
                self._state.metrics.barriers_out.inc()
                if barrier.coordinated:
                    self._emit_checkpoint_barrier(barrier)
                    return None
            return barrier
        return None

    def _emit_pending_rescale_barrier(self) -> bool:
        """Emit and park at a requested rescale cut from the source thread."""

        if self._source_rescale_requested.is_set():
            barrier = self._source_rescale_barrier
            output = self._state.output
            if barrier is None or output is None:
                raise RuntimeError("source rescale fence was requested without an output")
            output.flush(force=True)
            output.collect_to_edges(barrier, self._rescale_edge_indices)
            output.flush(force=True)
            self._source_rescale_requested.clear()
            loop = self._source_rescale_loop
            if loop is None:
                raise RuntimeError("source rescale event loop is unavailable")
            loop.call_soon_threadsafe(self._rescale_ready.set)
            self._source_rescale_resume.wait()
            return True
        return False

    def _emit_checkpoint_barrier(self, barrier: Barrier) -> None:
        output = self._state.output
        if output is None:
            raise RuntimeError("source checkpoint barrier requires an output")
        if barrier.coordinated:
            self._coordinated_checkpoint_barrier_id = barrier.id
        output.collect(barrier)

    def _resume_coordinated_checkpoint(self, barrier_id: int) -> None:
        if getattr(self, "_coordinated_checkpoint_barrier_id", None) != barrier_id:
            return
        self._coordinated_checkpoint_barrier_id = None

    def _await_terminal_barrier(self, checkpoint_id: int | None) -> Barrier | None:
        """Retry a canceled EOF epoch without starving an active rescale."""

        while not self._checkpoint_wait_stop.is_set():
            self._emit_pending_rescale_barrier()
            barrier = self._generate_barrier(
                is_eof=True,
                force=True,
                checkpoint_id=checkpoint_id,
            )
            if barrier is not None:
                return barrier
            # The assigned epoch may have been canceled by the rescale gate.
            # Retry queued coordinator work first, then allocate a new terminal
            # epoch once admission reopens.
            checkpoint_id = self._pop_requested_checkpoint()
            time.sleep(0.01)
        return None

    def _generate_barrier(
        self,
        is_eof: bool = False,
        *,
        force: bool = False,
        checkpoint_id: int | None = None,
    ) -> Barrier | None:
        if checkpoint_id is None:
            barrier = self._state.checkpoint_strategy.generate_next_barrier(is_eof, force=force)
        else:
            barrier = self._state.checkpoint_strategy.generate_next_barrier(
                is_eof,
                force=force,
                checkpoint_id=checkpoint_id,
            )
            self._discard_requested_checkpoint(checkpoint_id)
        if barrier is not None:
            # A source can be operator-chained directly with a sink. In that
            # layout the barrier never enters InboxPump, so the source task must
            # perform the same aligned sink lifecycle that the pump performs
            # for ordinary downstream tasks.
            self._state.operator.flush()
            self.prepare_sink_commit(barrier.id)
            if is_eof or isinstance(barrier, EndOfData):
                self._state.operator.finish()
            state = self._source_operator.snapshot_state(barrier.id)
            self._inflight_source_states[barrier.id] = state
            logger.debug("Triggering checkpoint barrier %s with source-owned state", barrier.id)
            try:
                state_size_bytes = len(cloudpickle.dumps(state))
            except Exception:
                state_size_bytes = 0
            self._state.checkpoint_strategy.on_barrier_received(
                barrier,
                lambda: self.register_checkpoint_metrics(barrier, state_size_bytes),
            )
            return barrier
        return None

    def notify_source_checkpoint_persisted(self, checkpoint_id: int) -> bool:
        """Notify the source only after its state is durable checkpoint metadata."""
        callback_succeeded = True
        try:
            for barrier_id in tuple(self._inflight_source_states):
                if barrier_id <= checkpoint_id:
                    self._inflight_source_states.pop(barrier_id, None)
            self._source_operator.notify_checkpoint_complete(checkpoint_id)
        except Exception:
            callback_succeeded = False
            logger.exception("Source checkpoint callback failed for durable checkpoint %s", checkpoint_id)
        finally:
            self._remember_resolved_checkpoint(checkpoint_id)
            self._resume_coordinated_checkpoint(checkpoint_id)
        return callback_succeeded
