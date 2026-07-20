# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
import time
from collections import deque

import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.logging import get_logger, log_event
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.observability.metrics.metrics import Counter, Gauge, Histogram
from ray.klein.runtime.actor import KleinActorHandle, create_remote_actor
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.runtime.coordinator.barrier_id_generator import BarrierIdGenerator
from ray.klein.runtime.coordinator.checkpoint import Checkpoint
from ray.klein.runtime.coordinator.checkpoint_registration import CheckpointRegistration
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.worker.async_worker import AsyncWorker
from ray.klein.state.operator_state_checkpoint_entry import (
    OperatorStateCheckpointEntry,
)
from ray.klein.state.sink_committable_checkpoint_entry import (
    SinkCommittableCheckpointEntry,
)
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry
from ray.klein.state.state_snapshot_reference import StateSnapshotReference

logger = get_logger(__name__)


class _CheckpointCommitError(RuntimeError):
    """A checkpoint could not become durable and externally visible."""


def _validate_integer_option(name: str, value: int, minimum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


class CheckpointCoordinator(AsyncWorker):
    """Async Ray actor that coordinates aligned checkpoints.

    One asyncio task drives periodic metadata persistence. Blocking checkpoint
    storage calls are dispatched outside the actor event loop.

    Blocking checkpoint-storage calls are dispatched via ``asyncio.to_thread``
    so filesystem or object-store I/O does not pin the actor event loop.

    For a remote async actor, ``KleinActorMethod`` returns an ObjectRef that
    ``ray.get`` can consume regardless of sync/async on the actor side.
    """

    @staticmethod
    def open_or_create(
        config: Configuration,
        namespace: str,
        job_name: str | None = None,
    ) -> KleinActorHandle:
        # Scope BOTH the lookup and the eventual create to the per-job
        # namespace so two coexisting Klein jobs each get their own progress
        # coordinator. Without this the second job's get_or_create would find
        # the first job's "CheckpointCoordinator" named actor (Ray's named-
        # actor registry was cluster-global) and silently start writing this
        # job's barriers / progress into the sibling job's state.
        coordinator = klein.get_actor_by_name(ComponentName.KLEIN_CHECKPOINT_COORDINATOR, namespace=namespace)
        if coordinator is not None:
            return coordinator
        ray_remote_args = {
            "name": ComponentName.KLEIN_CHECKPOINT_COORDINATOR,
            "num_cpus": 0,
            # Async actor: max_concurrency lets fast notify/register RPCs
            # interleave with snapshot-persist work parked in to_thread.
            "max_concurrency": 8,
            # max_restarts=-1: the coordinator is a single point of truth for
            # progress; let Ray auto-rebuild it on crash. The rebuilt actor
            # comes back with empty in-memory state — the JobManager health
            # loop detects this (needs_recovery) and re-opens it from the
            # last DFS checkpoint.
            "max_restarts": -1,
            "max_task_retries": -1,
        }
        ray_remote_args["namespace"] = namespace
        return create_remote_actor(
            CheckpointCoordinator,
            construct_args={
                "config": config,
                "job_id": namespace,
                "job_name": job_name or namespace,
            },
            ray_remote_args=ray_remote_args,
        )

    @staticmethod
    def find(namespace: str) -> KleinActorHandle | None:
        return klein.get_actor_by_name(ComponentName.KLEIN_CHECKPOINT_COORDINATOR, namespace=namespace)

    @staticmethod
    def coordinator_healthy(namespace: str, timeout: float = 30.0) -> bool:
        coordinator = CheckpointCoordinator.find(namespace=namespace)
        if coordinator:
            try:
                # ``needs_recovery`` is the coordinator's cheap, public health
                # probe: an actor rebuilt by Ray is alive but not healthy until
                # JobManager re-opens its persisted state.
                return not klein.get(coordinator.needs_recovery(), timeout=timeout)
            except Exception:
                logger.warning("The checkpoint coordinator is not ready", exc_info=True)
                return False

        logger.warning("The checkpoint coordinator actor was not found")
        return False

    def __init__(self, config: Configuration, job_id: str = "default", job_name: str | None = None) -> None:
        super().__init__()
        self._job_id = job_id
        metric_group = JobMetricGroup(job_name or job_id, job_id)
        self._checkpoints_triggered: Counter = metric_group.builtin_counter(KleinMetrics.CHECKPOINTS_TRIGGERED)
        self._checkpoints_completed: Counter = metric_group.builtin_counter(KleinMetrics.CHECKPOINTS_COMPLETED)
        self._checkpoints_failed: Counter = metric_group.builtin_counter(KleinMetrics.CHECKPOINTS_FAILED)
        self._checkpoint_persist_failures: Counter = metric_group.builtin_counter(
            KleinMetrics.CHECKPOINT_PERSIST_FAILURES
        )
        self._checkpoints_in_progress: Gauge = metric_group.builtin_gauge(KleinMetrics.CHECKPOINTS_IN_PROGRESS)
        self._last_completed_checkpoint_id: Gauge = metric_group.builtin_gauge(
            KleinMetrics.LAST_COMPLETED_CHECKPOINT_ID
        )
        self._last_persisted_checkpoint_revision: Gauge = metric_group.builtin_gauge(
            KleinMetrics.LAST_PERSISTED_CHECKPOINT_REVISION
        )
        self._checkpoint_duration_ms: Histogram = metric_group.builtin_histogram(KleinMetrics.CHECKPOINT_DURATION_MS)
        self._checkpoint_persist_duration_ms: Histogram = metric_group.builtin_histogram(
            KleinMetrics.CHECKPOINT_PERSIST_DURATION_MS
        )
        self._checkpoint_state_size_bytes: Gauge = metric_group.builtin_gauge(KleinMetrics.CHECKPOINT_STATE_SIZE_BYTES)
        self._sink_transactions_pending: Gauge = metric_group.builtin_gauge(KleinMetrics.SINK_TRANSACTIONS_PENDING)
        self._sink_transactions_committed: Counter = metric_group.builtin_counter(
            KleinMetrics.SINK_TRANSACTIONS_COMMITTED
        )
        self._sink_transaction_commit_failures: Counter = metric_group.builtin_counter(
            KleinMetrics.SINK_TRANSACTION_COMMIT_FAILURES
        )
        self._sink_transaction_commit_duration_ms: Histogram = metric_group.builtin_histogram(
            KleinMetrics.SINK_TRANSACTION_COMMIT_DURATION_MS
        )
        self._checkpoints_in_progress.set(0)
        self._sink_transactions_pending.set(0)
        self._checkpoint_persistence_interval = config.get(CheckpointOptions.PERSISTENCE_INTERVAL)
        _validate_integer_option(
            "execution.checkpointing.persistence-interval",
            self._checkpoint_persistence_interval,
            0,
        )
        self._checkpoint_dir = config.get(CheckpointOptions.DIRECTORY)
        self._checkpoint_storage_options = config.get(CheckpointOptions.STORAGE_OPTIONS)
        self._checkpoint_retained_count = config.get(CheckpointOptions.RETAINED_COUNT)
        _validate_integer_option("execution.checkpointing.num-retained", self._checkpoint_retained_count, 1)
        self._max_concurrent_checkpoints = config.get(CheckpointOptions.MAX_CONCURRENT)
        _validate_integer_option(
            "execution.checkpointing.max-concurrent-checkpoints",
            self._max_concurrent_checkpoints,
            1,
        )
        # Max seconds a checkpoint may stay in flight before being discarded —
        # frees the concurrency slot a never-aligning barrier (lost ack, crashed
        # subtask) would otherwise hold forever.
        self._checkpoint_timeout = config.get(CheckpointOptions.TIMEOUT)
        _validate_integer_option("execution.checkpointing.timeout", self._checkpoint_timeout, 0)
        checkpoint_history_size = config.get(CheckpointOptions.HISTORY_SIZE)
        _validate_integer_option(
            "execution.checkpointing.max-history-size",
            checkpoint_history_size,
            1,
        )
        self._checkpoint_history = deque(maxlen=checkpoint_history_size)
        self._metadata_revision: int = 0
        self._state_revision: int = 0
        self._persisted_state_revision: int = 0
        # The barrier high-water mark currently on disk. Tracked separately from
        # the live generator so a periodic tick with NO new progress can still
        # tell whether the high-water advanced and must be re-persisted. Without
        # this, the high-water only ever rode along with a non-empty progress
        # snapshot — so a coordinator that crashed before its first checkpoint
        # ever completed (progress still empty, but barriers already allocated
        # and being tracked by downstream aligners) persisted high-water 0. The
        # rebuilt coordinator then reseeded from 0 and re-issued ids that collide
        # with the orphans still pinned downstream, corrupting alignment.
        self._persisted_high_water: int = 0
        self._checkpoint_path: str | None = None
        self._latest_source_states: dict[str, SourceCheckpointEntry] = {}
        self._durable_source_states: dict[str, SourceCheckpointEntry] = {}
        self._notified_source_checkpoint_ids: dict[str, int] = {}
        self._latest_operator_states: dict[str, StateSnapshotReference] = {}
        self._restored_operator_states: dict[str, OperatorStateCheckpointEntry] = {}
        self._inflight_operator_states: dict[int, dict[str, StateSnapshotReference]] = {}
        self._pending_sink_committables: dict[tuple[str, int, str], SinkCommittableCheckpointEntry] = {}
        self._durable_sink_committables: dict[tuple[str, int, str], SinkCommittableCheckpointEntry] = {}
        self._inflight_sink_committables: dict[int, dict[str, SinkCommittableCheckpointEntry]] = {}
        self._execution_graph: ExecutionGraph | None = None
        self._required_acknowledgements: dict[ExecutionVertexId, int] = {}
        self._inflight_checkpoints: dict[int, Checkpoint] = {}
        # An aligned checkpoint leaves ``_inflight_checkpoints`` before its
        # source notification/state merge has completed. Keep that second phase
        # visible so a topology change cannot race an old-epoch state commit.
        self._completing_checkpoints: set[int] = set()
        # Barrier-id generator is per-coordinator-instance, NOT a process global.
        # A process-global counter resets to 0 when Ray rebuilds this actor
        # (max_restarts=-1), and on a Tier-1 coordinator-only restart the
        # downstream aligners keep partially aligned barrier ids from the previous epoch; a
        # fresh id starting from 1 could collide and corrupt alignment. open()
        # re-seeds this above the persisted high-water mark so reused ids can't
        # happen across a restart.
        self._barrier_id_gen: BarrierIdGenerator = BarrierIdGenerator()
        # The barrier-id floor of THIS coordinator epoch: the largest id that
        # belongs to a *previous* epoch. Captured in open() right after reseed
        # (= generator value before any new id is issued), so every barrier this
        # instance allocates is strictly greater. On a Tier-1 coordinator-only
        # rebuild the sources keep running with old-epoch barrier ids still
        # pinned in their _inflight_barriers; those ids are all <= this floor
        # (RESEED_STRIDE guarantees the gap), so broadcasting it lets sources drop
        # exactly the orphans without touching new-epoch in-flight barriers. 0 on
        # a cold start (no previous epoch -> nothing to reclaim).
        self._barrier_epoch_floor: int = 0
        # False until open() runs. After a Ray-driven restart the rebuilt actor
        # re-runs __init__ only, so this is False again — the JobManager health
        # loop uses it to detect a restarted-but-unrecovered coordinator and
        # re-open it from the last DFS checkpoint.
        self._opened: bool = False
        self._progress_lock = None
        self._persistence_lock = None
        self._rescale_operation_id: str | None = None
        # A committed local topology is not independently recoverable until one
        # full checkpoint has captured source progress and every stateful task
        # under that topology.  While this marker is set, task-level recovery
        # must escalate to a consistent global restore instead of rebuilding one
        # actor from pre-rescale checkpoint state.
        self._rescale_recovery_fence: str | None = None
        self._rescale_recovery_pending_sources: set[ExecutionVertexId] = set()
        self._rescale_recovery_required_state_revision: int | None = None
        self._transient_rescale_states: dict[tuple[str, int], tuple[StateSnapshotReference, ...]] = {}
        # Deliberately process-local. If the coordinator restarts before a
        # new-topology checkpoint supersedes a transient cut, the safe outcome
        # is a global checkpoint restore (including source rewind), never a
        # silent single-operator restore from old state.
        self._superseded_rescale_operations: set[tuple[str, int]] = set()

    def _ensure_locks(self) -> None:
        if self._progress_lock is None:
            self._progress_lock = asyncio.Lock()
        if self._persistence_lock is None:
            self._persistence_lock = asyncio.Lock()

    async def open(self, execution_graph: ExecutionGraph, restore_path: str | None) -> None:
        self._ensure_locks()
        # ``open`` is also used by a global restart on an existing coordinator.
        # Clear any abandoned in-memory transaction.  A coordinator-only Ray
        # rebuild receives an ExecutionGraph whose replacement vertices still
        # carry the local-cut identity; retain a conservative recovery fence in
        # that case until a fresh checkpoint completes.
        self._rescale_operation_id = None
        self._transient_rescale_states = {}
        self._superseded_rescale_operations = set()
        restore_vertices = tuple(
            vertex for vertex in execution_graph.execution_vertices if vertex.restore_operation_id is not None
        )
        restore_operations = {vertex.restore_operation_id for vertex in restore_vertices}
        self._rescale_recovery_fence = min(restore_operations) if restore_operations else None
        self._rescale_recovery_pending_sources = (
            {source.id for source in execution_graph.source_execution_vertices} if restore_operations else set()
        )
        self._rescale_recovery_required_state_revision = None
        if not restore_path:
            restore_path = await asyncio.to_thread(
                checkpoint_io.latest_checkpoint,
                self._checkpoint_dir,
                self._job_id,
                self._checkpoint_storage_options,
            )
        self._checkpoint_path = restore_path
        self._execution_graph = execution_graph
        self._required_acknowledgements = checkpoint_io.coordinator_ack_counts(execution_graph)
        # Checkpoint restore reads from disk; off-load it so we don't pin
        # the actor event loop during checkpoint recovery.
        (
            self._metadata_revision,
            restored_source_states,
            barrier_high_water,
        ) = await asyncio.to_thread(
            checkpoint_io.restore_checkpoint,
            restore_path,
            self._checkpoint_storage_options,
        )
        self._latest_source_states = {entry.task_key: entry for entry in restored_source_states}
        self._durable_source_states = dict(self._latest_source_states)
        # Completion callbacks are at-least-once across coordinator recovery:
        # a crash can happen after the metadata write but before the callback.
        # Source implementations must therefore treat the checkpoint id as an
        # idempotency key.
        self._notified_source_checkpoint_ids = {}
        self._latest_operator_states = {}
        self._restored_operator_states = await asyncio.to_thread(
            checkpoint_io.restore_operator_state_entries,
            restore_path,
            self._checkpoint_storage_options,
        )
        restored_sink_committables = await asyncio.to_thread(
            checkpoint_io.restore_sink_committable_entries,
            restore_path,
            self._checkpoint_storage_options,
        )
        self._pending_sink_committables = {
            self._sink_committable_identity(entry): entry for entry in restored_sink_committables
        }
        self._durable_sink_committables = dict(self._pending_sink_committables)
        self._inflight_sink_committables = {}
        self._update_pending_sink_transaction_metric()
        self._inflight_operator_states = {}
        self._inflight_checkpoints = {}
        self._completing_checkpoints = set()
        self._state_revision = 0
        self._persisted_state_revision = 0
        # Re-seed the barrier generator above the persisted high-water mark so a
        # rebuilt coordinator never re-issues a barrier id that a downstream
        # aligner might still be holding from before the restart. The stride
        # past the high-water also guards barriers that were allocated after the
        # last successful snapshot (and so weren't persisted) but may linger in
        # flight downstream.
        self._barrier_id_gen.reseed(barrier_high_water)
        self._persisted_high_water = barrier_high_water
        # Snapshot the epoch floor BEFORE any new barrier is issued: every id this
        # epoch allocates will be > this value, so it is the exact cutoff sources
        # use to drop orphan barriers from the previous epoch (see
        # get_barrier_epoch_floor / JobMaster Tier-1 reclaim).
        self._barrier_epoch_floor = self._barrier_id_gen.current
        self._opened = True
        self._checkpoints_in_progress.set(0)
        self._last_persisted_checkpoint_revision.set(self._metadata_revision)
        await self._commit_durable_sink_committables(strict=True)

    def barrier_epoch_floor(self) -> int:
        """Largest barrier id belonging to a previous coordinator epoch.

        Sync, in-memory. After a Tier-1 rebuild the scheduler broadcasts this to
        every source so they can discard orphan in-flight barriers (all <= floor)
        that this rebuilt coordinator has no record of and will never ack.
        """
        return self._barrier_epoch_floor

    def needs_recovery(self) -> bool:
        """True if this is a restarted actor that hasn't been re-opened yet.

        Cheap sync probe for the JobManager health loop. After a Ray restart the
        actor is alive but __init__-only (state empty); re-opening it from the
        last checkpoint path restores source state and the metadata revision.
        """
        return not self._opened

    async def start(self) -> None:
        # Persistence may be disabled without starting an idle supervisor task.
        if self._checkpoint_persistence_interval <= 0:
            logger.warning(
                "Checkpoint persistence is disabled because its interval is %s",
                self._checkpoint_persistence_interval,
            )
            return
        await super().start()

    async def _run(self) -> None:
        # One iteration of the persistence loop. Sleeping with asyncio.sleep is
        # interruptible: stop() cancels the task and CancelledError unwinds the
        # await immediately (no need for a separate wake event).
        await asyncio.sleep(self._checkpoint_persistence_interval)
        await self._expire_stale_checkpoints()
        await self._persist_checkpoint_metadata()

    async def _expire_stale_checkpoints(self) -> None:
        """Discard in-flight checkpoints older than the configured timeout.

        A barrier that never fully aligns (lost sink ack, crashed subtask) would
        otherwise sit in _inflight_checkpoints forever, permanently consuming a
        concurrency slot and eventually throttling all new checkpoints once the
        cap is hit. The matching source-owned state pinned when the barrier was
        generated leaks the same way, so we also tell the source to release it.
        In-memory bookkeeping is sync;
        the source-release RPC is off-loaded (sources are still sync actors).
        """
        if self._checkpoint_timeout <= 0:
            return
        now_ms = int(time.time() * 1000)
        deadline_ms = self._checkpoint_timeout * 1000
        stale = [
            barrier_id
            for barrier_id, checkpoint in self._inflight_checkpoints.items()
            if checkpoint.triggered_at_ms is not None and now_ms - checkpoint.triggered_at_ms > deadline_ms
        ]
        for barrier_id in stale:
            checkpoint = self._inflight_checkpoints.pop(barrier_id)
            self._inflight_operator_states.pop(barrier_id, None)
            await self._abort_inflight_sink_committables(barrier_id)
            checkpoint.mark_failed(f"checkpoint timed out after {self._checkpoint_timeout}s")
            self._checkpoints_failed.inc()
            self._checkpoints_in_progress.set(len(self._inflight_checkpoints))
            log_event(
                logger,
                logging.WARNING,
                "checkpoint.timed_out",
                "Checkpoint %s timed out: %s",
                barrier_id,
                checkpoint.reason,
                job_id=self._job_id,
                checkpoint_id=barrier_id,
            )
            await self._release_source_inflight(barrier_id, checkpoint.trigger_sources[0])

    async def _release_source_inflight(self, barrier_id: int, source_vertex_id: ExecutionVertexId) -> None:
        """Best-effort: ask a source to drop state held for a dead barrier."""
        source_vertex = self._execution_graph.execution_vertex(source_vertex_id)
        stream_task = source_vertex.stream_task
        if stream_task is None:
            return
        try:
            await klein.aget(stream_task.discard_source_checkpoint(barrier_id))
        except Exception:
            logger.debug(
                "Failed to release source state for expired checkpoint barrier %s",
                barrier_id,
                exc_info=True,
            )

    def register_checkpoint(self, vertex_id: ExecutionVertexId, *, force: bool = False) -> CheckpointRegistration:
        # Intentionally sync: pure in-memory bookkeeping with no I/O. Ray async
        # actors happily expose sync methods alongside async ones.
        if self._rescale_operation_id is not None:
            return CheckpointRegistration.skip(
                f"Checkpoint registration is paused for operator rescale {self._rescale_operation_id}"
            )
        # No sink path downstream -> nothing to align/ack -> checkpointing this
        # source is meaningless without a required sink acknowledgement count.
        required_acknowledgements = self._required_acknowledgements.get(vertex_id, 0)
        if required_acknowledgements <= 0:
            reason = f"Source {vertex_id} has no checkpointable sink path; skipping checkpoint"
            logger.debug(reason)
            return CheckpointRegistration.skip(reason)

        pending_checkpoints = len(self._inflight_checkpoints)
        # Enforce the cap before allocating another checkpoint.
        if not force and pending_checkpoints >= self._max_concurrent_checkpoints:
            reason = (
                f"Pending checkpoints {pending_checkpoints} reached the concurrent "
                f"checkpoint limit {self._max_concurrent_checkpoints}; skipping checkpoint"
            )
            logger.debug(reason)
            return CheckpointRegistration.skip(reason)

        barrier_id = next(self._barrier_id_gen)
        log_event(
            logger,
            logging.DEBUG,
            "checkpoint.triggered",
            "Triggered checkpoint %s from source %s",
            barrier_id,
            self._execution_graph.execution_vertex(vertex_id),
            job_id=self._job_id,
            checkpoint_id=barrier_id,
        )
        checkpoint = Checkpoint(barrier_id, required_acknowledgements, (vertex_id,))
        self._checkpoint_history.append(checkpoint)
        self._inflight_checkpoints[barrier_id] = checkpoint
        checkpoint.mark_in_progress()
        self._checkpoints_triggered.inc()
        self._checkpoints_in_progress.set(len(self._inflight_checkpoints))

        return CheckpointRegistration.success(barrier_id)

    async def begin_operator_rescale(self, operation_id: str, timeout: float) -> bool:
        """Stop admitting checkpoints and wait for the old topology to drain."""

        if not isinstance(operation_id, str) or not operation_id.strip():
            raise ValueError("rescale operation_id cannot be empty")
        if timeout <= 0:
            raise ValueError("rescale timeout must be greater than zero")
        deadline = time.monotonic() + timeout
        while self._rescale_recovery_fence is not None:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "timed out waiting for the committed topology to complete its stabilization checkpoint"
                )
            await asyncio.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        if self._rescale_operation_id not in {None, operation_id}:
            raise RuntimeError(f"checkpoint coordinator is busy with rescale {self._rescale_operation_id}")
        self._rescale_operation_id = operation_id
        while self._inflight_checkpoints or self._completing_checkpoints:
            if time.monotonic() >= deadline:
                self._rescale_operation_id = None
                raise TimeoutError("timed out waiting for in-flight checkpoints before rescale")
            await asyncio.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        return True

    def finish_operator_rescale(self, operation_id: str, committed: bool = False) -> bool:
        """Release checkpoint admission and optionally fence local recovery.

        Both forms are idempotent so a caller can safely retry after a lost RPC
        response.  An aborted operation that never acquired the gate is also a
        successful no-op while the coordinator is otherwise idle; this matters
        when replacement-actor prewarming fails before ``begin`` is attempted.
        """

        if self._rescale_operation_id == operation_id:
            self._rescale_operation_id = None
            if committed:
                self._rescale_recovery_fence = operation_id
                self._rescale_recovery_pending_sources = {
                    vertex.id for vertex in self._execution_graph.source_execution_vertices
                }
                self._rescale_recovery_required_state_revision = None
            return True
        if not committed and self._rescale_operation_id is None:
            return True
        return committed and self._rescale_recovery_fence == operation_id

    def operator_rescale_recovery_fenced(self) -> bool:
        """Whether a task failure must use global checkpoint recovery."""

        return self._rescale_recovery_fence is not None

    def register_operator_state(
        self,
        barrier_id: int,
        vertex_id: ExecutionVertexId,
        reference: StateSnapshotReference,
    ) -> bool:
        """Pin one immutable task snapshot until its source barrier completes."""

        if not isinstance(reference, StateSnapshotReference):
            raise TypeError("reference must be a StateSnapshotReference")
        if barrier_id not in self._inflight_checkpoints:
            logger.warning(
                "Ignoring operator state for unknown checkpoint %s from execution vertex %s",
                barrier_id,
                vertex_id,
            )
            return False
        task_key = self._operator_state_key(vertex_id)
        states = self._inflight_operator_states.setdefault(barrier_id, {})
        existing = states.get(task_key)
        if existing is not None and existing.checksum != reference.checksum:
            raise ValueError(f"operator {task_key} registered different state for checkpoint {barrier_id}")
        states[task_key] = reference
        return True

    def register_operator_checkpoint_metrics(
        self,
        barrier_id: int,
        vertex_id: ExecutionVertexId,
        metrics: dict[str, int | float],
    ) -> bool:
        """Attach per-subtask alignment/state diagnostics to a checkpoint."""

        checkpoint = self._inflight_checkpoints.get(barrier_id)
        if checkpoint is None or checkpoint.status.is_terminal:
            return False
        allowed = {
            "alignment_duration_ms",
            "barrier_latency_ms",
            "state_size_bytes",
            "rows_in",
            "rows_out",
            "backpressure_events",
            "backpressure_duration_ms",
        }
        normalized: dict[str, int | float] = {}
        for key, value in metrics.items():
            if key not in allowed or isinstance(value, bool) or not isinstance(value, int | float):
                continue
            normalized[key] = max(0, value)
        checkpoint.record_operator_metrics(vertex_id, normalized)
        return True

    def register_sink_committable(
        self,
        barrier_id: int,
        vertex_id: ExecutionVertexId,
        committable: SinkCommittable,
    ) -> bool:
        """Register one prepared sink transaction before acknowledging a barrier."""

        if not isinstance(committable, SinkCommittable):
            raise TypeError("committable must be a SinkCommittable")
        checkpoint = self._inflight_checkpoints.get(barrier_id)
        if checkpoint is None or checkpoint.status.is_terminal:
            logger.warning(
                "Ignoring sink transaction %s for unavailable checkpoint %s",
                committable.transaction_id,
                barrier_id,
            )
            return False
        task_key = self._operator_state_key(vertex_id)
        entry = SinkCommittableCheckpointEntry(task_key, barrier_id, committable)
        entries = self._inflight_sink_committables.setdefault(barrier_id, {})
        existing = entries.get(task_key)
        if existing is not None and existing.transaction_id != entry.transaction_id:
            raise ValueError(f"sink {task_key} registered different transactions for checkpoint {barrier_id}")
        entries[task_key] = entry
        self._update_pending_sink_transaction_metric()
        return True

    async def notify_checkpoint_aligned(self, barrier_id: int, vertex_id: ExecutionVertexId) -> bool:
        """Acks barrier alignment from a sink; once aligned, snapshots the source."""
        vertex = self._execution_graph.execution_vertex(vertex_id)
        checkpoint = self._inflight_checkpoints.get(barrier_id)
        if checkpoint is None:
            logger.warning(
                "Ignoring unknown completed checkpoint barrier %s from execution vertex %s",
                barrier_id,
                vertex,
            )
            return False
        if checkpoint.status.is_terminal:
            logger.warning(
                "Ignoring checkpoint barrier %s from execution vertex %s because it is already %s",
                barrier_id,
                vertex,
                checkpoint.status,
            )
            return False
        if not checkpoint.acknowledge(committer=vertex_id):
            return True
        checkpoint = self._inflight_checkpoints.pop(barrier_id, None)
        if checkpoint is None:
            return False
        self._completing_checkpoints.add(barrier_id)
        self._checkpoints_in_progress.set(len(self._inflight_checkpoints) + len(self._completing_checkpoints))
        logger.debug(
            "Checkpoint barrier %s is aligned; notifying the source",
            barrier_id,
        )
        try:
            source_vertex = self._execution_graph.execution_vertex(checkpoint.trigger_sources[0])
            stream_task = source_vertex.stream_task
            if stream_task is None:
                await self._fail_checkpoint(
                    checkpoint,
                    barrier_id,
                    "Source task is unavailable during checkpoint completion.",
                )
                logger.warning(
                    "Source task %s disappeared while completing checkpoint %s",
                    source_vertex.id,
                    barrier_id,
                )
                return False
            return await self._complete_aligned_checkpoint(
                checkpoint,
                barrier_id,
                source_vertex.id,
                stream_task,
            )
        finally:
            self._completing_checkpoints.discard(barrier_id)
            self._checkpoints_in_progress.set(len(self._inflight_checkpoints) + len(self._completing_checkpoints))

    async def _complete_aligned_checkpoint(
        self,
        checkpoint: Checkpoint,
        barrier_id: int,
        source_vertex_id: ExecutionVertexId,
        stream_task: KleinActorHandle,
    ) -> bool:
        """Make one fully aligned checkpoint durable and externally visible."""

        try:
            success, source_state = await klein.aget(stream_task.notify_source_checkpoint_complete(barrier_id))
            if not success:
                await self._fail_checkpoint(
                    checkpoint,
                    barrier_id,
                    "Notifying source checkpoint completion failed.",
                )
                log_event(
                    logger,
                    logging.WARNING,
                    "checkpoint.failed",
                    "Checkpoint %s failed while notifying its source",
                    barrier_id,
                    job_id=self._job_id,
                    checkpoint_id=barrier_id,
                )
                return False
            async with self._progress_lock:
                task_key = self._source_state_key(source_vertex_id)
                self._update_latest_source_state(
                    SourceCheckpointEntry(
                        task_key=task_key,
                        checkpoint_id=barrier_id,
                        state=source_state,
                    )
                )
                completed_states = self._inflight_operator_states.pop(barrier_id, {})
                self._replace_logical_operator_states(completed_states)
                completed_committables = self._inflight_sink_committables.pop(barrier_id, {})
                for entry in completed_committables.values():
                    self._pending_sink_committables[self._sink_committable_identity(entry)] = entry
                self._update_pending_sink_transaction_metric()
                if completed_committables:
                    self._state_revision += 1
                stabilization_ready = self._record_rescale_stabilization_progress(source_vertex_id)
            if completed_committables or stabilization_ready:
                await self._persist_checkpoint_metadata(strict=True)
            checkpoint.mark_complete()
            duration_ms = max(
                0,
                (checkpoint.completed_at_ms or 0) - (checkpoint.triggered_at_ms or 0),
            )
            self._checkpoints_completed.inc()
            self._last_completed_checkpoint_id.set(barrier_id)
            self._checkpoint_duration_ms.observe(duration_ms)
            self._checkpoint_state_size_bytes.set(
                sum(reference.size_bytes for reference in self._latest_operator_states.values())
            )
            log_event(
                logger,
                logging.INFO,
                "checkpoint.completed",
                "Checkpoint %s completed in %d ms",
                barrier_id,
                duration_ms,
                job_id=self._job_id,
                checkpoint_id=barrier_id,
                duration_ms=duration_ms,
            )
            return True
        except _CheckpointCommitError:
            checkpoint.mark_failed("Checkpoint could not become durable.")
            self._checkpoints_failed.inc()
            logger.exception("Checkpoint %s could not become durable", barrier_id)
            raise
        except Exception:
            self._inflight_operator_states.pop(barrier_id, None)
            await self._abort_inflight_sink_committables(barrier_id)
            logger.exception(
                "Checkpoint completion notification failed for barrier %s",
                barrier_id,
            )
            checkpoint.mark_failed("Notifying source checkpoint completion failed.")
            self._checkpoints_failed.inc()
            log_event(
                logger,
                logging.WARNING,
                "checkpoint.failed",
                "Checkpoint %s failed during completion notification",
                barrier_id,
                job_id=self._job_id,
                checkpoint_id=barrier_id,
            )
            return False

    async def _fail_checkpoint(self, checkpoint: Checkpoint, barrier_id: int, reason: str) -> None:
        """Release non-durable task state and mark a checkpoint failed."""

        self._inflight_operator_states.pop(barrier_id, None)
        await self._abort_inflight_sink_committables(barrier_id)
        checkpoint.mark_failed(reason)
        self._checkpoints_failed.inc()

    async def _persist_checkpoint_metadata(
        self,
        *,
        notify_sources: bool = True,
        strict: bool = False,
    ) -> bool:
        # The whole persist sequence (pick target revision -> write -> clean old
        # -> bump the revision) is serialized: the periodic loop and an external
        # persist_now() must not interleave (else both write the same checkpoint-N and
        # clean each other's path).
        async with self._persistence_lock:
            # Finish durable sink transactions before source offsets are
            # committed. This preserves end-to-end exactly-once ordering.
            sinks_committed = await self._commit_durable_sink_committables(strict=strict)
            if not sinks_committed:
                return False
            if notify_sources:
                await self._notify_durable_source_checkpoints()
            # Take a consistent snapshot of source-owned state under the progress
            # lock, then do the blocking disk write outside it — never
            # hold a lock across the to_thread await for the merge.
            async with self._progress_lock:
                barrier_high_water = self._barrier_id_gen.current
                state_revision = self._state_revision
                # Avoid emitting a new checkpoint directory for identical
                # metadata. The barrier high-water remains an independent dirty
                # signal because allocated-but-uncommitted barriers must still be
                # made durable across coordinator recovery.
                if (
                    state_revision <= self._persisted_state_revision
                    and barrier_high_water <= self._persisted_high_water
                ):
                    self._clear_rescale_recovery_fence_if_durable()
                    return True
                source_states = list(self._latest_source_states.values())
                live_operator_states = dict(self._latest_operator_states)
                restored_operator_states = {
                    key: value
                    for key, value in self._restored_operator_states.items()
                    if key not in live_operator_states
                }
                restore_checkpoint_path = self._checkpoint_path
                sink_committables = tuple(self._pending_sink_committables.values())
            target_revision = self._metadata_revision + 1
            persist_started_at = time.monotonic()
            try:
                operator_states = await self._materialize_operator_states(
                    live_operator_states,
                    restored_operator_states,
                    restore_checkpoint_path,
                )
                # Disk write: off-load to a thread.
                latest_checkpoint_path = await asyncio.to_thread(
                    checkpoint_io.write_checkpoint,
                    source_states,
                    target_revision,
                    self._checkpoint_dir,
                    barrier_high_water,
                    self._job_id,
                    self._checkpoint_storage_options,
                    operator_states,
                    sink_committables,
                )
            except Exception as error:
                self._checkpoint_persist_failures.inc()
                log_event(
                    logger,
                    logging.ERROR,
                    "checkpoint.persistence.failed",
                    "Failed to persist checkpoint revision %s",
                    target_revision,
                    exc_info=True,
                    job_id=self._job_id,
                    checkpoint_revision=target_revision,
                )
                if strict:
                    raise _CheckpointCommitError("Failed to persist checkpoint metadata") from error
                return False

            self._checkpoint_path = latest_checkpoint_path
            self._checkpoint_persist_duration_ms.observe_elapsed(persist_started_at)
            self._metadata_revision = target_revision
            self._last_persisted_checkpoint_revision.set(target_revision)
            self._persisted_state_revision = state_revision
            self._persisted_high_water = barrier_high_water
            self._durable_source_states = {entry.task_key: entry for entry in source_states}
            self._durable_sink_committables = {
                self._sink_committable_identity(entry): entry for entry in sink_committables
            }
            self._restored_operator_states = await asyncio.to_thread(
                checkpoint_io.restore_operator_state_entries,
                latest_checkpoint_path,
                self._checkpoint_storage_options,
            )
            sinks_committed = await self._commit_durable_sink_committables(strict=strict)
            if not sinks_committed:
                return False
            if notify_sources:
                await self._notify_durable_source_checkpoints()
            self._clear_rescale_recovery_fence_if_durable()

        try:
            await asyncio.to_thread(
                checkpoint_io.cleanup_checkpoints,
                self._checkpoint_dir,
                self._job_id,
                self._checkpoint_retained_count,
                self._checkpoint_storage_options,
            )
        except Exception:
            # Retention is best-effort after the new _metadata is durable. A
            # transient delete/list error must not invalidate that checkpoint.
            log_event(
                logger,
                logging.WARNING,
                "checkpoint.retention.failed",
                "Checkpoint retention cleanup failed",
                exc_info=True,
                job_id=self._job_id,
            )

        log_event(
            logger,
            logging.INFO,
            "checkpoint.persisted",
            "Persisted checkpoint revision %s",
            self._metadata_revision,
            job_id=self._job_id,
            checkpoint_revision=self._metadata_revision,
            checkpoint_path=self._checkpoint_path,
        )
        return True

    async def _notify_durable_source_checkpoints(self) -> None:
        """Best-effort, at-least-once source callbacks after metadata commit."""

        if self._execution_graph is None or not self._durable_source_states:
            return
        for source_vertex in self._execution_graph.source_execution_vertices:
            task_key = self._source_state_key(source_vertex.id)
            entry = self._durable_source_states.get(task_key)
            if entry is None or self._notified_source_checkpoint_ids.get(task_key, -1) >= entry.checkpoint_id:
                continue
            if source_vertex.stream_task is None:
                continue
            try:
                await klein.aget(source_vertex.stream_task.notify_source_checkpoint_persisted(entry.checkpoint_id))
            except Exception:
                logger.warning(
                    "Failed to notify source %s that checkpoint %s is durable",
                    task_key,
                    entry.checkpoint_id,
                    exc_info=True,
                )
                continue
            self._notified_source_checkpoint_ids[task_key] = entry.checkpoint_id

    async def _commit_durable_sink_committables(self, *, strict: bool) -> bool:
        """Idempotently publish every transaction present in durable metadata."""

        all_committed = True
        for identity, entry in list(self._durable_sink_committables.items()):
            commit_started_at = time.monotonic()
            try:
                await asyncio.to_thread(entry.committable.commit)
            except Exception as error:
                all_committed = False
                self._sink_transaction_commit_failures.inc()
                log_event(
                    logger,
                    logging.ERROR,
                    "sink.transaction.commit_failed",
                    "Failed to commit sink transaction %s for checkpoint %s",
                    entry.transaction_id,
                    entry.checkpoint_id,
                    exc_info=True,
                    job_id=self._job_id,
                    checkpoint_id=entry.checkpoint_id,
                    transaction_id=entry.transaction_id,
                )
                if strict:
                    raise _CheckpointCommitError(
                        f"Failed to commit sink transaction {entry.transaction_id!r}"
                    ) from error
                continue
            self._sink_transaction_commit_duration_ms.observe_elapsed(commit_started_at)
            self._sink_transactions_committed.inc()
            self._durable_sink_committables.pop(identity, None)
            if self._pending_sink_committables.pop(identity, None) is not None:
                self._state_revision += 1
            self._update_pending_sink_transaction_metric()
            log_event(
                logger,
                logging.INFO,
                "sink.transaction.committed",
                "Committed sink transaction %s for checkpoint %s",
                entry.transaction_id,
                entry.checkpoint_id,
                job_id=self._job_id,
                checkpoint_id=entry.checkpoint_id,
                transaction_id=entry.transaction_id,
            )
        return all_committed

    async def _abort_inflight_sink_committables(self, barrier_id: int) -> None:
        entries = tuple(self._inflight_sink_committables.pop(barrier_id, {}).values())
        for entry in entries:
            try:
                await asyncio.to_thread(entry.committable.abort)
            except Exception:
                logger.warning(
                    "Failed to abort sink transaction %s for checkpoint %s",
                    entry.transaction_id,
                    barrier_id,
                    exc_info=True,
                )
        self._update_pending_sink_transaction_metric()

    def _update_pending_sink_transaction_metric(self) -> None:
        inflight_count = sum(len(entries) for entries in self._inflight_sink_committables.values())
        self._sink_transactions_pending.set(len(self._pending_sink_committables) + inflight_count)

    async def persist_now(
        self,
        *,
        notify_sources: bool = True,
        abort_inflight_sinks: bool = False,
    ) -> str | None:
        """Persist the latest progress immediately (terminal flush).

        Called by the scheduler right before stopping the coordinator on job
        FINISHED/CANCELLED, so the progress accumulated since the last periodic
        flush is durable rather than lost between the last loop tick and stop.
        The scheduler disables source callbacks because workers have already
        stopped; explicit calls keep callbacks enabled for normal checkpoints.
        """
        if abort_inflight_sinks:
            for barrier_id in tuple(self._inflight_sink_committables):
                await self._abort_inflight_sink_committables(barrier_id)
        await self._persist_checkpoint_metadata(notify_sources=notify_sources)
        return self._checkpoint_path

    def latest_checkpoint_path(self) -> str | None:
        """Return the latest durable checkpoint path."""
        return self._checkpoint_path

    def dashboard_snapshot(self) -> dict:
        """Return checkpoint history and state-size aggregates for the UI."""

        history = [self._checkpoint_dashboard_row(checkpoint) for checkpoint in reversed(self._checkpoint_history)]
        statuses = [checkpoint["status"] for checkpoint in history]
        state_size = sum(reference.size_bytes for reference in self._latest_operator_states.values()) + sum(
            entry.size_bytes for entry in self._restored_operator_states.values()
        )
        latest_completed = next(
            (checkpoint for checkpoint in history if checkpoint["status"] == "COMPLETED"),
            None,
        )
        if latest_completed is not None:
            state_size = max(state_size, int(latest_completed.get("state_size_bytes", 0)))
        return {
            "summary": {
                "total": len(history),
                "completed": statuses.count("COMPLETED"),
                "failed": statuses.count("FAILED"),
                "in_progress": sum(status in {"CREATED", "IN_PROGRESS", "NOTIFYING"} for status in statuses),
                "state_size_bytes": state_size,
                "last_persisted_revision": self._metadata_revision,
                "pending_sink_transactions": len(self._pending_sink_committables)
                + sum(len(entries) for entries in self._inflight_sink_committables.values()),
            },
            "history": history,
            "latest_path": self._checkpoint_path,
            "rescale_recovery_fenced": self.operator_rescale_recovery_fenced(),
        }

    def _checkpoint_dashboard_row(self, checkpoint: Checkpoint) -> dict:
        duration_ms = None
        if checkpoint.triggered_at_ms is not None:
            end_ms = checkpoint.completed_at_ms or checkpoint.last_acknowledged_at_ms or int(time.time() * 1000)
            duration_ms = max(0, end_ms - checkpoint.triggered_at_ms)
        operators: dict[int, dict] = {}
        for vertex_id, metrics in sorted(
            checkpoint.operator_metrics.items(),
            key=lambda item: (item[0].job_vertex_id, item[0].index),
        ):
            job_vertex = (
                None
                if self._execution_graph is None
                else self._execution_graph.find_job_vertex(vertex_id.job_vertex_id)
            )
            operator = operators.setdefault(
                vertex_id.job_vertex_id,
                {
                    "op_id": vertex_id.job_vertex_id,
                    "name": job_vertex.name if job_vertex is not None else f"Operator {vertex_id.job_vertex_id}",
                    "state_size_bytes": 0,
                    "alignment_duration_ms": 0.0,
                    "barrier_latency_ms": 0.0,
                    "subtasks": [],
                },
            )
            subtask = {"subtask_index": vertex_id.index, **metrics}
            operator["subtasks"].append(subtask)
            operator["state_size_bytes"] += int(metrics.get("state_size_bytes", 0))
            operator["alignment_duration_ms"] = max(
                operator["alignment_duration_ms"],
                float(metrics.get("alignment_duration_ms", 0.0)),
            )
            operator["barrier_latency_ms"] = max(
                operator["barrier_latency_ms"],
                float(metrics.get("barrier_latency_ms", 0.0)),
            )
        operator_rows = list(operators.values())
        return {
            "id": checkpoint.barrier_id,
            "status": checkpoint.status.name,
            "triggered_at_ms": checkpoint.triggered_at_ms,
            "completed_at_ms": checkpoint.completed_at_ms,
            "duration_ms": duration_ms,
            "acknowledged": checkpoint.acknowledgements,
            "required_acknowledgements": checkpoint.required_acknowledgements,
            "reason": checkpoint.reason or None,
            "state_size_bytes": sum(operator["state_size_bytes"] for operator in operator_rows),
            "alignment_duration_ms": max(
                (operator["alignment_duration_ms"] for operator in operator_rows),
                default=0.0,
            ),
            "barrier_latency_ms": max(
                (operator["barrier_latency_ms"] for operator in operator_rows),
                default=0.0,
            ),
            "operators": operator_rows,
        }

    async def source_state(self, vertex_id: ExecutionVertexId) -> SourceCheckpointEntry | None:
        """Return the latest completed state owned by one source subtask."""

        task_key = self._source_state_key(vertex_id)
        async with self._progress_lock:
            return self._latest_source_states.get(task_key)

    async def stage_operator_rescale_state(
        self,
        operation_id: str,
        job_vertex_id: int,
        snapshots: tuple[StateSnapshotReference, ...],
    ) -> None:
        """Stage a local-cut snapshot without polluting global checkpoints."""

        if self._rescale_operation_id != operation_id:
            raise RuntimeError(f"operator rescale {operation_id} is not active")
        if isinstance(job_vertex_id, bool) or not isinstance(job_vertex_id, int):
            raise TypeError("job_vertex_id must be an integer")
        references = tuple(snapshots)
        if any(not isinstance(reference, StateSnapshotReference) for reference in references):
            raise TypeError("rescale snapshots must be StateSnapshotReference values")
        self._transient_rescale_states[operation_id, job_vertex_id] = references

    def operator_rescale_states(
        self,
        operation_id: str,
        vertex_id: ExecutionVertexId,
    ) -> tuple[StateSnapshotReference, ...]:
        return self._transient_rescale_states.get((operation_id, vertex_id.job_vertex_id), ())

    async def restore_operator_rescale_states(
        self,
        operation_id: str,
        vertex_id: ExecutionVertexId,
    ) -> tuple[StateSnapshotReference, ...]:
        """Restore the local cut initially, then normal checkpoint state.

        A replacement actor's Ray restart recipe permanently contains its
        rescale operation id. During the active swap the transient cut is
        mandatory. Once committed, a later checkpoint supersedes that cut and
        actor recovery must follow the normal latest-state path instead.
        """

        key = (operation_id, vertex_id.job_vertex_id)
        transient = self._transient_rescale_states.get(key)
        if transient is not None:
            return transient
        if self._rescale_operation_id == operation_id:
            raise RuntimeError(f"managed state for active operator rescale {operation_id} is unavailable")
        if key not in self._superseded_rescale_operations:
            raise RuntimeError(
                f"managed state for operator rescale {operation_id} is no longer recoverable; "
                "a consistent global checkpoint restore is required"
            )
        return await self.latest_operator_states(vertex_id)

    def discard_operator_rescale_state(self, operation_id: str, job_vertex_id: int) -> bool:
        key = (operation_id, job_vertex_id)
        self._superseded_rescale_operations.discard(key)
        return self._transient_rescale_states.pop(key, None) is not None

    def reconfigure_execution_graph(self, execution_graph: ExecutionGraph) -> None:
        """Install new checkpoint acknowledgement topology without reopening."""

        if execution_graph.namespace != self._job_id:
            raise ValueError("execution graph belongs to a different job")
        if self._rescale_operation_id is None:
            raise RuntimeError("execution graph can only be reconfigured inside an operator rescale")
        if self._inflight_checkpoints or self._completing_checkpoints:
            raise RuntimeError("cannot reconfigure checkpoint topology with checkpoints in flight")
        self._execution_graph = execution_graph
        self._required_acknowledgements = checkpoint_io.coordinator_ack_counts(execution_graph)

    async def latest_operator_states(
        self,
        vertex_id: ExecutionVertexId,
    ) -> tuple[StateSnapshotReference, ...]:
        """Return all fragments of one logical operator, hot where available."""

        prefix = self._operator_state_prefix(vertex_id)
        async with self._progress_lock:
            live = {key: value for key, value in self._latest_operator_states.items() if key.startswith(prefix)}
            durable_keys = sorted(
                key for key in self._restored_operator_states if key.startswith(prefix) and key not in live
            )
        durable = await self._read_durable_operator_states(durable_keys)
        return tuple(live[key] for key in sorted(live)) + durable

    async def durable_operator_states(
        self,
        vertex_id: ExecutionVertexId,
    ) -> tuple[StateSnapshotReference, ...]:
        """Return every durable fragment for a logical operator."""

        prefix = self._operator_state_prefix(vertex_id)
        async with self._progress_lock:
            keys = sorted(key for key in self._restored_operator_states if key.startswith(prefix))
        return await self._read_durable_operator_states(keys)

    async def _read_durable_operator_states(
        self,
        task_keys: list[str],
    ) -> tuple[StateSnapshotReference, ...]:
        async with self._progress_lock:
            entries = [self._restored_operator_states[key] for key in task_keys]
            checkpoint_path = self._checkpoint_path
        if checkpoint_path is None:
            return ()
        references = []
        for entry in entries:
            payload = await asyncio.to_thread(
                checkpoint_io.read_operator_state,
                checkpoint_path,
                entry,
                self._checkpoint_storage_options,
            )
            references.append(
                StateSnapshotReference(
                    size_bytes=entry.size_bytes,
                    checksum=entry.checksum,
                    inline_payload=payload,
                )
            )
        return tuple(references)

    async def _materialize_operator_states(
        self,
        live: dict[str, StateSnapshotReference],
        durable: dict[str, OperatorStateCheckpointEntry],
        checkpoint_path: str | None,
    ) -> dict[str, bytes]:
        states: dict[str, bytes] = {}
        for task_key, reference in live.items():
            states[task_key] = await asyncio.to_thread(reference.materialize, klein.get)
        if checkpoint_path is not None:
            for task_key, entry in durable.items():
                states[task_key] = await asyncio.to_thread(
                    checkpoint_io.read_operator_state,
                    checkpoint_path,
                    entry,
                    self._checkpoint_storage_options,
                )
        return states

    @staticmethod
    def _operator_state_key(vertex_id: ExecutionVertexId) -> str:
        return f"{vertex_id.job_vertex_id}:{vertex_id.index}"

    @staticmethod
    def _sink_committable_identity(entry: SinkCommittableCheckpointEntry) -> tuple[str, int, str]:
        return entry.task_key, entry.checkpoint_id, entry.transaction_id

    @staticmethod
    def _source_state_key(vertex_id: ExecutionVertexId) -> str:
        return f"{vertex_id.job_vertex_id}:{vertex_id.index}"

    @staticmethod
    def _operator_state_prefix(vertex_id: ExecutionVertexId) -> str:
        return f"{vertex_id.job_vertex_id}:"

    def _replace_logical_operator_states(
        self,
        completed_states: dict[str, StateSnapshotReference],
    ) -> None:
        """Atomically supersede every old-parallelism fragment for each operator."""

        prefixes = {f"{task_key.split(':', 1)[0]}:" for task_key in completed_states}
        if prefixes:
            self._latest_operator_states = {
                key: value
                for key, value in self._latest_operator_states.items()
                if not any(key.startswith(prefix) for prefix in prefixes)
            }
            self._restored_operator_states = {
                key: value
                for key, value in self._restored_operator_states.items()
                if not any(key.startswith(prefix) for prefix in prefixes)
            }
        self._latest_operator_states.update(completed_states)
        if prefixes:
            logical_ids = {int(prefix[:-1]) for prefix in prefixes}
            self._superseded_rescale_operations.update(
                key for key in self._transient_rescale_states if key[1] in logical_ids
            )
            self._transient_rescale_states = {
                key: value for key, value in self._transient_rescale_states.items() if key[1] not in logical_ids
            }
        if completed_states:
            self._state_revision += 1

    def _record_rescale_stabilization_progress(self, source_vertex_id: ExecutionVertexId) -> bool:
        """Record one source cut and report whether strict persistence is due.

        Stateless rescaled operators have no transient fragments, so any fully
        aligned checkpoint is sufficient.  For a stateful operator the state
        replacement above removes the transient cut first. The fence itself is
        only cleared later, after metadata at or beyond the captured revision is
        durable.
        """

        operation_id = self._rescale_recovery_fence
        if operation_id is None:
            return False
        self._rescale_recovery_pending_sources.discard(source_vertex_id)
        if self._rescale_recovery_pending_sources:
            return False
        if any(key[0] == operation_id for key in self._transient_rescale_states):
            logger.warning(
                "Checkpoint completed without superseding transient state for rescale %s; "
                "retaining the global-recovery fence",
                operation_id,
            )
            return False
        self._rescale_recovery_required_state_revision = self._state_revision
        return True

    def _clear_rescale_recovery_fence_if_durable(self) -> None:
        """Clear the fence only when its new-topology state is on storage."""

        operation_id = self._rescale_recovery_fence
        required_revision = self._rescale_recovery_required_state_revision
        if operation_id is None or required_revision is None:
            return
        if self._persisted_state_revision < required_revision:
            return
        self._rescale_recovery_fence = None
        self._rescale_recovery_pending_sources.clear()
        self._rescale_recovery_required_state_revision = None

    def _update_latest_source_state(self, entry: SourceCheckpointEntry) -> None:
        current = self._latest_source_states.get(entry.task_key)
        if current is None or entry.checkpoint_id > current.checkpoint_id:
            self._latest_source_states[entry.task_key] = entry
            self._state_revision += 1

    def _get_name(self) -> str:
        return "CheckpointCoordinator"
