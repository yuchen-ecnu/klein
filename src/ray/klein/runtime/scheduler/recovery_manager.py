# SPDX-License-Identifier: Apache-2.0
"""Failover recovery flows for the JobMaster (single-writer scheduler thread).

Tier-0 single-point task recovery + regional coordinator re-open. Mutates vertex
status only via ``task_deployer.bootstrap_vertex`` (which does not transition
status), so the single-writer invariant holds: transitions stay with the caller.
"""

import logging
from collections.abc import Callable
from enum import Enum, auto

from ray.exceptions import ActorUnavailableError

import ray
import ray.klein as klein
from ray.klein._internal.logging import get_logger, log_event
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.scheduler import task_deployer

logger = get_logger(__name__)


class _RecoveryOutcome(Enum):
    RUNNING = auto()
    REBUILDING = auto()
    REBUILT = auto()
    TERMINAL = auto()
    UNRECOVERABLE = auto()


class RecoveryManager:
    """Single-point and regional failover recovery for one job's graph.

    ``coordinator_provider`` is read each call so we always see the live handle
    (Ray may have rebuilt the coordinator since construction).
    """

    def __init__(
        self,
        execution_graph: ExecutionGraph,
        coordinator_provider: Callable[[], KleinActorHandle | None],
        rpc_timeout: float,
        start_timeout: float,
    ) -> None:
        self._execution_graph = execution_graph
        self._namespace = execution_graph.namespace
        self._coordinator_provider = coordinator_provider
        self._rpc_timeout = rpc_timeout
        self._start_timeout = start_timeout

    def _coordinator_alive(self) -> bool:
        from ray.klein._internal.constants import ComponentName

        return (
            klein.get_actor_status(ComponentName.KLEIN_CHECKPOINT_COORDINATOR, namespace=self._namespace)
            == StreamTaskStatus.ALIVE
        )

    def recover_coordinator_if_needed(self) -> bool:
        """REGIONAL failover: re-open a Ray-rebuilt coordinator from its last
        checkpoint, then reclaim orphan barriers. Returns True if recovery ran.

        The coordinator has max_restarts=-1; Ray rebuilds it with empty state, so
        needs_recovery() detects the rebuild and open() restores from checkpoint.
        """
        coordinator = self._coordinator_provider()
        if coordinator is None or not self._coordinator_alive():
            return False
        try:
            if not klein.get(
                coordinator.needs_recovery(),
                timeout=self._rpc_timeout,
            ):
                return False
            restore_path = klein.get(
                coordinator.latest_checkpoint_path(),
                timeout=self._rpc_timeout,
            )
            if restore_path is None:
                log_event(
                    logger,
                    logging.WARNING,
                    "failover.coordinator.checkpoint_missing",
                    "The rebuilt checkpoint coordinator has no persisted checkpoint; reopening from scratch",
                )
            log_event(
                logger,
                logging.WARNING,
                "failover.coordinator.recovery.started",
                "Reopening the rebuilt checkpoint coordinator from %s",
                restore_path,
                checkpoint_path=restore_path,
            )
            klein.get(
                coordinator.open(self._execution_graph, restore_path),
                timeout=self._start_timeout,
            )
            klein.get(coordinator.start(), timeout=self._start_timeout)
            self._reclaim_orphan_barriers(coordinator)
            log_event(
                logger,
                logging.INFO,
                "failover.coordinator.recovery.completed",
                "Checkpoint coordinator recovery completed from %s",
                restore_path,
                checkpoint_path=restore_path,
            )
            return True
        except Exception as error:
            log_event(
                logger,
                logging.WARNING,
                "failover.coordinator.recovery.failed",
                "Checkpoint coordinator recovery failed: %s",
                error,
                exc_info=True,
                checkpoint_path=locals().get("restore_path"),
            )
            return False

    def _reclaim_orphan_barriers(self, coordinator: KleinActorHandle) -> None:
        """Tell every task to drop in-flight barriers from the previous epoch.

        The rebuilt coordinator never registered the still-running tasks' old-epoch
        barriers (and will never ack them). It re-seeds ids above an epoch floor, so
        all orphans have id <= that floor; broadcasting it lets each task reclaim
        them without touching new-epoch barriers. Best-effort; ``ray.wait`` drains
        progressively so one failed RPC doesn't block the rest.
        """
        barrier_id_floor = self._read_barrier_epoch_floor(coordinator)
        if barrier_id_floor is None or barrier_id_floor <= 0:
            return
        references = self._orphan_reclaim_references(barrier_id_floor)
        self._drain_orphan_reclaims(references)

    def _read_barrier_epoch_floor(self, coordinator: KleinActorHandle) -> int | None:
        try:
            return klein.get(
                coordinator.barrier_epoch_floor(),
                timeout=self._rpc_timeout,
            )
        except Exception as error:
            log_event(
                logger,
                logging.WARNING,
                "failover.barrier.epoch_read_failed",
                "Could not read the barrier epoch floor; orphan-barrier reclamation was skipped: %s",
                error,
                exc_info=True,
            )
            return None

    def _orphan_reclaim_references(self, barrier_id_floor: int) -> dict[ray.ObjectRef, str]:
        references: dict[ray.ObjectRef, str] = {}
        for vertex in self._execution_graph.execution_vertices:
            if vertex.stream_task is not None:
                reference = vertex.stream_task.reset_inflight_before(barrier_id_floor)
                if isinstance(reference, ray.ObjectRef):
                    references[reference] = vertex.name
        return references

    def _drain_orphan_reclaims(self, references: dict[ray.ObjectRef, str]) -> None:
        pending = list(references)
        while pending:
            ready, pending = ray.wait(
                pending,
                num_returns=min(len(pending), 100),
                timeout=self._rpc_timeout,
            )
            for reference in ready:
                try:
                    ray.get(reference)
                except Exception as error:
                    log_event(
                        logger,
                        logging.WARNING,
                        "failover.barrier.reclaim_failed",
                        "Orphan-barrier reclamation failed for task %s: %s",
                        references[reference],
                        error,
                        exc_info=True,
                        task_name=references[reference],
                    )
            if not ready and pending:
                log_event(
                    logger,
                    logging.WARNING,
                    "failover.barrier.reclaim_timed_out",
                    "Orphan-barrier reclamation timed out for %d tasks",
                    len(pending),
                    remaining_tasks=len(pending),
                )
                break

    def try_recover_tasks(self) -> bool:
        """Tier-0 single-task recovery (classify-then-act in one pass).

        Returns True if every non-terminal vertex is running or being rebuilt by
        Ray; False (escalate to global restart) only for genuinely unrecoverable
        vertices: FAILED, NOT_EXIST, missing handle, or a setup_and_run() error.
        Classifying before acting stops one mid-rebuild vertex from blocking the
        re-bootstrap of others.
        """
        rebuilt: list[ExecutionVertex] = []
        rebuilding: list[ExecutionVertex] = []
        for vertex in self._execution_graph.execution_vertices:
            outcome = self._recover_vertex(vertex)
            if outcome is _RecoveryOutcome.UNRECOVERABLE:
                return False
            if outcome is _RecoveryOutcome.REBUILDING:
                rebuilding.append(vertex)
            elif outcome is _RecoveryOutcome.REBUILT:
                rebuilt.append(vertex)
        for vertex in rebuilt:
            self._replay_upstreams_to(vertex)
        if rebuilt:
            task_names = [vertex.name for vertex in rebuilt]
            log_event(
                logger,
                logging.INFO,
                "failover.task.recovery.completed",
                "Recovered %d tasks and replayed their upstream buffers: %s",
                len(rebuilt),
                ", ".join(task_names),
                task_names=task_names,
            )
        if rebuilding:
            task_names = [vertex.name for vertex in rebuilding]
            log_event(
                logger,
                logging.INFO,
                "failover.task.rebuild_pending",
                "Ray is rebuilding %d tasks; recovery will continue on the next health check: %s",
                len(rebuilding),
                ", ".join(task_names),
                task_names=task_names,
            )
        return True

    def _recover_vertex(self, vertex: ExecutionVertex) -> _RecoveryOutcome:
        status = vertex.status
        if status == ExecutionVertexStatus.FAILED:
            log_event(
                logger,
                logging.INFO,
                "failover.task.unrecoverable",
                "Task %s reported a logical failure; escalating to a global restart",
                vertex.name,
                task_name=vertex.name,
                task_status=status.name,
            )
            return _RecoveryOutcome.UNRECOVERABLE
        if status == ExecutionVertexStatus.CREATED:
            return _RecoveryOutcome.UNRECOVERABLE
        if status.is_terminal:
            return _RecoveryOutcome.TERMINAL
        if vertex.stream_task is None:
            return _RecoveryOutcome.UNRECOVERABLE

        actor_status = klein.get_actor_status(vertex.name, namespace=self._namespace)
        if actor_status == StreamTaskStatus.NOT_EXIST:
            return _RecoveryOutcome.UNRECOVERABLE
        if actor_status != StreamTaskStatus.ALIVE:
            return _RecoveryOutcome.REBUILDING
        return self._recover_alive_vertex(vertex)

    def _recover_alive_vertex(self, vertex: ExecutionVertex) -> _RecoveryOutcome:
        try:
            running = klein.get(
                vertex.stream_task.is_running(),
                timeout=self._rpc_timeout,
            )
        except ActorUnavailableError:
            return _RecoveryOutcome.REBUILDING
        except Exception:
            return _RecoveryOutcome.UNRECOVERABLE
        if running:
            return _RecoveryOutcome.RUNNING
        return _RecoveryOutcome.REBUILT if self._bootstrap_vertex(vertex) else _RecoveryOutcome.UNRECOVERABLE

    def _bootstrap_vertex(self, vertex: ExecutionVertex) -> bool:
        log_event(
            logger,
            logging.WARNING,
            "failover.task.recovery.started",
            "Rebootstrapping task %s after Ray rebuilt its actor",
            vertex.name,
            task_name=vertex.name,
        )
        try:
            task_deployer.bootstrap_vertex(vertex, timeout=self._start_timeout)
            return True
        except Exception as error:
            log_event(
                logger,
                logging.ERROR,
                "failover.task.recovery.failed",
                "Task recovery failed for %s; escalating to a global restart: %s",
                vertex.name,
                error,
                exc_info=True,
                task_name=vertex.name,
            )
            return False

    def _replay_upstreams_to(self, vertex: ExecutionVertex) -> None:
        """Tell every live upstream to replay its buffer to ``vertex``."""
        for input_edge in self._execution_graph.input_job_edges(vertex.id.job_vertex_id):
            for execution_edge in input_edge.execution_edges:
                if execution_edge.target.id != vertex.id:
                    continue
                upstream = execution_edge.source
                if upstream.stream_task is None:
                    continue
                if klein.get_actor_status(upstream.name, namespace=self._namespace) != StreamTaskStatus.ALIVE:
                    continue
                try:
                    klein.get(
                        upstream.stream_task.replay_buffered_to(vertex.name),
                        timeout=self._rpc_timeout,
                    )
                except Exception as error:
                    log_event(
                        logger,
                        logging.WARNING,
                        "failover.buffer.replay_failed",
                        "Buffer replay from %s to %s failed and will be retried: %s",
                        upstream.name,
                        vertex.name,
                        error,
                        exc_info=True,
                        source_task=upstream.name,
                        target_task=vertex.name,
                    )
