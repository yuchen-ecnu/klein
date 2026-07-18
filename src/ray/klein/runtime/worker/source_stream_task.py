# SPDX-License-Identifier: Apache-2.0
import asyncio
from typing import TYPE_CHECKING, Any

import ray.cloudpickle as cloudpickle

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.message import MAX_WATERMARK, Barrier, Watermark
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
    generation and downstream emission happen on that
    same executor thread (sync ``klein.get``), consistent with the collector
    emit path.
    """

    def __init__(self, descriptor: "TaskDeploymentDescriptor") -> None:
        super().__init__(descriptor)
        self._inflight_source_states: dict[int, Any] = {}

    @property
    def _source_operator(self) -> SourceOperator:
        operator = self._state.operator
        if not isinstance(operator, SourceOperator):
            raise TypeError("SourceStreamTask requires a SourceOperator")
        return operator

    async def _on_setup_done(self, runtime_context: RuntimeContext) -> None:
        # Source emits inline: collect() is driven from inside the blocking
        # operator.run() loop on the executor thread, so there's no pump to drain
        # a pipelined buffer.
        self._state.pipelined = False
        if self._state.collector is not None:
            self._state.collector.configure_pipelining(False)
        source_operator = self._source_operator
        source_operator.bind_record_emitter(self._on_records_emitted)
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
        await super().stop(timeout)

    def _run_source(self) -> None:
        """Executor-thread body: drive the source operator to completion.

        ``operator.run()`` returns when the source is exhausted, or when a drain
        request set the fn's interrupt flag (graceful take(n) / shutdown). Either
        way we emit a single EndOfData so the downstream alignment chain can
        commit a final checkpoint and reach FINISHED.
        """
        self._source_operator.run()
        if self._state.collector is not None:
            self._state.collector.collect(Watermark(MAX_WATERMARK))
            self._state.collector.collect(self._generate_barrier(is_eof=True))
            self._state.collector.close()
        klein.get(self._job_manager.update_stream_task_status(self._vertex_id, ExecutionVertexStatus.FINISHED))
        self._eof_reached = True

    def request_drain(self) -> None:
        """Cooperatively stop producing (graceful take(n) / drain).

        Sets the source fn's interrupt flag so run() returns; _run_source then
        emits a normal EndOfData and the standard alignment chain drives the job
        to FINISHED. Idempotent and cheap — safe to call from the JobManager RPC.
        """
        self._source_operator.interrupt()

    def notify_source_checkpoint_complete(self, barrier_id: int) -> tuple[bool, Any]:
        return self._notify_source_checkpoint_complete(barrier_id)

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
        if existed:
            logger.debug("Discarded source state for checkpoint barrier %s", barrier_id)
        return existed

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
        if stale:
            logger.info(
                "Reclaimed %d orphan in-flight barriers through checkpoint %d after coordinator recovery",
                len(stale),
                cutoff_barrier_id,
            )
        return len(stale)

    def _notify_source_checkpoint_complete(self, barrier_id: int) -> tuple[bool, Any]:
        try:
            if barrier_id not in self._inflight_source_states:
                logger.warning("Ignoring unknown checkpoint barrier %s", barrier_id)
                return False, None
            state = self._inflight_source_states.pop(barrier_id)
            return True, state
        except Exception:
            logger.exception("Checkpoint completion notification failed for barrier %s", barrier_id)
            return False, None

    def _on_records_emitted(self, record_emitted: bool) -> Barrier | None:
        if super()._check_end_of_stream():
            return None
        if self._state.checkpoint_strategy.should_trigger(record_emitted):
            barrier = self._generate_barrier()
            if barrier:
                self._state.metrics.barriers_out.inc()
            return barrier
        return None

    def _generate_barrier(self, is_eof: bool = False) -> Barrier | None:
        barrier = self._state.checkpoint_strategy.generate_next_barrier(is_eof)
        if is_eof and barrier is None:
            raise RuntimeError("failed to generate the end-of-data barrier")
        if barrier is not None:
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

    def notify_source_checkpoint_persisted(self, checkpoint_id: int) -> None:
        """Notify the source only after its state is durable checkpoint metadata."""

        self._source_operator.notify_checkpoint_complete(checkpoint_id)
