# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Sequence
from typing import Any

import ray.klein as klein
from ray.klein._internal.deadline import Deadline
from ray.klein._internal.logging import get_logger, log_event
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)

logger = get_logger(__name__)


class JobHealthReport:
    """One health snapshot of a running job: all StreamTasks + the coordinator.

    Built once per supervisor tick, off the actor event loop. It captures enough
    for the supervisor to pick a recovery tier in one pass:

      * ``healthy`` — everything alive; sleep until the next tick.
      * ``tasks_not_running`` — names of tasks that aren't healthy (Ray is
        rebuilding them, they crashed, or they reported FAILED); drives Tier-0
        single-task recovery / escalation to a full restart.
      * ``coordinator_healthy`` — the checkpoint coordinator's actor health.
    """

    def __init__(self, execution_graph: ExecutionGraph, rpc_timeout: float = 30.0) -> None:
        deadline = Deadline(rpc_timeout)
        self._task_status: dict[str, bool] = self._check_task_health(execution_graph, deadline)
        # Use the per-job Ray namespace pinned on the ExecutionGraph by
        # JobManager.submit so the health probe targets *this* job's
        # coordinator. Without it, two coexisting Klein jobs would both
        # probe the same cluster-global "CheckpointCoordinator" actor and
        # one job's restart would swing the other's health verdict.
        self._coordinator_healthy = CheckpointCoordinator.coordinator_healthy(
            namespace=execution_graph.namespace,
            timeout=deadline.remaining(),
        )

    @staticmethod
    def _check_task_health(execution_graph: ExecutionGraph, deadline: Deadline) -> dict[str, bool]:
        """Per-task liveness. Terminal vertices are settled from local state;
        the rest are probed with one batched health RPC.

        A FAILED vertex is unhealthy; a FINISHED one is healthy-and-done. For the
        remainder we ask the actor itself (``health_info``). A vertex with no
        actor handle (mid-teardown, or never (re)deployed) counts as unhealthy so
        the supervisor doesn't mistake a half-built graph for a running one.
        """
        task_status: dict[str, bool] = {}
        probe_names: list[str] = []
        probe_requests = []
        for vertex in execution_graph.execution_vertices:
            status = vertex.status
            if status == ExecutionVertexStatus.FAILED:
                task_status[vertex.name] = False
            elif status == ExecutionVertexStatus.FINISHED:
                task_status[vertex.name] = True
            elif vertex.stream_task is None:
                task_status[vertex.name] = False
            else:
                probe_names.append(vertex.name)
                probe_requests.append(vertex.stream_task.health_info())

        for name, (is_healthy, reason) in zip(
            probe_names,
            JobHealthReport._gather_health(probe_requests, deadline),
            strict=True,
        ):
            task_status[name] = is_healthy
            if not is_healthy:
                log_event(
                    logger,
                    logging.WARNING,
                    "task.health.unhealthy",
                    "Task %s is unhealthy: %s",
                    name,
                    reason,
                    task_name=name,
                    reason=reason,
                )
        return task_status

    @staticmethod
    def _gather_health(requests: Sequence[Any], deadline: Deadline) -> list[tuple[bool, str]]:
        """Resolve the batched health RPCs, degrading to per-request resolution
        if the batch fails (one dead actor must not blind us to the others)."""
        if not requests:
            return []
        try:
            return klein.get(requests, timeout=deadline.remaining())
        except Exception:
            results = []
            for request in requests:
                if deadline.expired():
                    results.append((False, "Task health probe deadline exceeded"))
                    continue
                try:
                    results.append(klein.get(request, timeout=deadline.remaining()))
                except Exception:
                    results.append((False, "Error when querying health info from actor"))
            return results

    @property
    def all_tasks_running(self) -> bool:
        return all(self._task_status.values())

    @property
    def tasks_not_running(self) -> list[str]:
        return [name for name, is_running in self._task_status.items() if not is_running]

    @property
    def coordinator_healthy(self) -> bool:
        return self._coordinator_healthy

    @property
    def healthy(self) -> bool:
        return self.all_tasks_running and self._coordinator_healthy

    def summary(self) -> str:
        not_running = self.tasks_not_running
        return (
            f"healthy={self.healthy} tasks_healthy={self.all_tasks_running} "
            f"coordinator_healthy={self.coordinator_healthy} unhealthy_tasks={not_running}"
        )
