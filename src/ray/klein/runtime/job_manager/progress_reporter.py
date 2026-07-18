# SPDX-License-Identifier: Apache-2.0
"""Builds the CLI live-view progress snapshot from the execution graph.

Pure read side: it reads vertex statuses and gathers each subtask's row counts,
producing a ``ProgressSnapshot`` for the JobClient's live view. It never writes
the execution graph, so it does not participate in the single-writer invariant —
extracted from JobManager so the supervisor class stays focused on lifecycle.
"""

from collections.abc import Callable
from dataclasses import fields

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.job_manager.progress import (
    InstanceCounts,
    OperatorProgress,
    ProgressSnapshot,
    SubtaskCounts,
    SubtaskProgress,
)

logger = get_logger(__name__)


class ProgressReporter:
    """Read-only producer of operator progress snapshots for the CLI view."""

    def __init__(
        self,
        execution_graph: ExecutionGraph,
        is_job_running: Callable[[], bool],
        restart_window: Callable[[], tuple[int, int, int]],
    ) -> None:
        self._execution_graph = execution_graph
        self._is_job_running = is_job_running
        self._restart_window = restart_window
        # Last good per-subtask counts, keyed by object identity. A subtask whose progress
        # RPC fails mid-restart falls back to these so its row holds its numbers
        # instead of dropping to zero.
        self._last_counts: dict[int, SubtaskCounts] = {}

    async def snapshot(self) -> ProgressSnapshot:
        """Per-operator progress + failover state. Best-effort and read-only."""
        job_running = self._is_job_running()
        job_vertices = list(self._execution_graph.job_vertices.values())
        vertices_by_job_vertex = [list(job_vertex.execution_vertices.values()) for job_vertex in job_vertices]
        counts_by_vertex, failed_vertices = await self._probe_counts(vertices_by_job_vertex)
        operators = [
            self._operator_progress(
                job_vertex,
                vertices,
                counts_by_vertex,
                failed_vertices,
                job_running,
            )
            for job_vertex, vertices in zip(
                job_vertices,
                vertices_by_job_vertex,
                strict=True,
            )
        ]
        operators.sort(key=lambda progress: progress.op_id)
        restarts, max_restarts, window_seconds = self._restart_counts()
        return ProgressSnapshot(
            operators=tuple(operators),
            restarts=restarts,
            max_restarts=max_restarts,
            window_seconds=window_seconds,
        )

    async def _probe_counts(self, vertices_by_job_vertex) -> tuple[dict, set[int]]:
        probed_vertices = [
            vertex for vertices in vertices_by_job_vertex for vertex in vertices if vertex.stream_task is not None
        ]
        requests = [vertex.stream_task.progress_counts() for vertex in probed_vertices]
        results = await klein.aget(requests, return_exceptions=True) if requests else []

        counts_by_vertex = {}
        failed_vertices: set[int] = set()
        for vertex, result in zip(probed_vertices, results, strict=True):
            vertex_key = id(vertex)
            if isinstance(result, Exception):
                failed_vertices.add(vertex_key)
                counts_by_vertex[vertex_key] = self._last_counts.get(vertex_key, SubtaskCounts())
            else:
                counts_by_vertex[vertex_key] = result
                self._last_counts[vertex_key] = result
        return counts_by_vertex, failed_vertices

    def _operator_progress(
        self,
        job_vertex,
        vertices,
        counts_by_vertex,
        failed_vertices,
        job_running: bool,
    ) -> OperatorProgress:
        statuses = [vertex.status for vertex in vertices]
        counts = [counts_by_vertex.get(id(vertex), SubtaskCounts()) for vertex in vertices]
        totals = self._sum_counts(counts)
        failed_progress_requests = sum(1 for vertex in vertices if id(vertex) in failed_vertices)
        resources = job_vertex.resources
        subtasks = tuple(
            self._subtask_progress(vertex, count, id(vertex) in failed_vertices, job_running)
            for vertex, count in zip(vertices, counts, strict=True)
        )
        return OperatorProgress(
            name=job_vertex.name,
            op_id=job_vertex.id,
            parallelism=job_vertex.concurrency,
            status=self._aggregate_status(statuses, job_running, failed_progress_requests),
            rows_in=totals.rows_in,
            rows_out=totals.rows_out,
            queued=totals.queued,
            capacity=totals.capacity,
            busy_ns=totals.busy_ns,
            backpressure_ns=totals.backpressure_ns,
            instances=self._instance_counts(statuses, job_running, failed_progress_requests),
            cpus=resources.cpus,
            gpus=resources.gpus,
            downstream=tuple(self._execution_graph.downstream_job_vertices(job_vertex.id)),
            backpressure_events=totals.backpressure_events,
            barriers_in=totals.barriers_in,
            barriers_out=totals.barriers_out,
            checkpoint_alignment_ms=max((count.checkpoint_alignment_ms for count in counts), default=0.0),
            checkpoint_barrier_latency_ms=max(
                (count.checkpoint_barrier_latency_ms for count in counts),
                default=0.0,
            ),
            checkpoint_state_size_bytes=totals.checkpoint_state_size_bytes,
            last_checkpoint_id=max(
                (count.last_checkpoint_id for count in counts if count.last_checkpoint_id is not None),
                default=None,
            ),
            subtasks=subtasks,
        )

    @staticmethod
    def _sum_counts(counts: list[SubtaskCounts]) -> SubtaskCounts:
        totals = {
            field.name: (
                max(
                    (getattr(count, field.name) for count in counts if getattr(count, field.name) is not None),
                    default=None,
                )
                if field.name == "last_checkpoint_id"
                else sum(getattr(count, field.name) for count in counts)
            )
            for field in fields(SubtaskCounts)
        }
        return SubtaskCounts(**totals)

    @classmethod
    def _subtask_progress(
        cls,
        vertex,
        counts: SubtaskCounts,
        progress_failed: bool,
        job_running: bool,
    ) -> SubtaskProgress:
        status = cls._aggregate_status([vertex.status], job_running, int(progress_failed))
        return SubtaskProgress(
            subtask_index=vertex.index,
            status=status,
            **{field.name: getattr(counts, field.name) for field in fields(SubtaskCounts)},
        )

    def _restart_counts(self) -> tuple[int, int, int]:
        try:
            return self._restart_window()
        except Exception as error:
            logger.debug("restart window read failed: %s", error)
            return 0, 0, 0

    @staticmethod
    def _instance_counts(statuses, job_running: bool, failed_progress_requests: int = 0) -> InstanceCounts:
        """Break an operator's subtask statuses into per-state counts.

        A FAILED subtask while the job is still RUNNING is Ray rebuilding it
        (restarting), not a terminal failure. ``failed_progress_requests`` subtasks
        failed their progress RPC (actor unavailable, mid-restart) even though
        their cached status may still read RUNNING — reclassify those to
        restarting so recovery is visible immediately.
        """
        running = pending = restarting = finished = failed = 0
        for status in statuses:
            if status == ExecutionVertexStatus.RUNNING:
                running += 1
            elif status == ExecutionVertexStatus.FINISHED:
                finished += 1
            elif status == ExecutionVertexStatus.FAILED:
                if job_running:
                    restarting += 1
                else:
                    failed += 1
            else:  # CREATED / DEPLOYED / CANCELLING — not yet processing
                pending += 1
        reclassify = min(failed_progress_requests, running)
        running -= reclassify
        restarting += reclassify
        return InstanceCounts(
            running=running,
            pending=pending,
            restarting=restarting,
            finished=finished,
            failed=failed,
        )

    @staticmethod
    def _aggregate_status(statuses, job_running: bool, failed_progress_requests: int = 0) -> str:
        # A subtask FAILED while the job is still RUNNING means Ray is rebuilding
        # it / Tier-0 recovery is in flight — surface as "recovering". A FAILED
        # subtask once the job has left RUNNING is a genuine failure.
        status_set = set(statuses)
        if job_running and (failed_progress_requests > 0 or ExecutionVertexStatus.FAILED in status_set):
            return "recovering"
        if ExecutionVertexStatus.FAILED in status_set:
            return "failed"
        if statuses and status_set == {ExecutionVertexStatus.FINISHED}:
            return "finished"
        if ExecutionVertexStatus.RUNNING in status_set:
            return "running"
        return "pending"
