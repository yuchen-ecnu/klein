# SPDX-License-Identifier: Apache-2.0
"""Builds the CLI live-view progress snapshot from the execution graph.

Pure read side: it reads vertex statuses and gathers each subtask's row counts,
producing a ``ProgressSnapshot`` for the JobClient's live view. It never writes
the execution graph, so it does not participate in the single-writer invariant —
extracted from JobManager so the supervisor class stays focused on lifecycle.
"""

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import fields

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
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

_CUMULATIVE_COUNT_FIELDS = frozenset(
    {
        "rows_in",
        "rows_out",
        "bytes_in",
        "bytes_out",
        "busy_ns",
        "backpressure_ns",
        "backpressure_events",
        "barriers_in",
        "barriers_out",
    }
)
_PROGRESS_RPC_TIMEOUT_SECONDS = 2.0


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
        # Last good per-subtask counts, keyed by stable physical identity. A subtask whose progress
        # RPC fails mid-restart falls back to these so its row holds its numbers
        # instead of dropping to zero.
        self._last_counts: dict[ExecutionVertexId, SubtaskCounts] = {}
        # Scale-in removes live subtasks, but their cumulative work remains part
        # of the operator's lifetime totals.  Gauges (queue depth/capacity,
        # checkpoint latency and state size) are deliberately not retained.
        self._retired_counts: dict[int, SubtaskCounts] = {}
        # Dashboard polling and a rescale RPC may interleave on a Ray async
        # actor.  Topology replacement must not race an in-flight old-graph
        # probe or it could retire a stale sample.
        self._lock = asyncio.Lock()

    async def snapshot(self) -> ProgressSnapshot:
        """Per-operator progress + failover state. Best-effort and read-only."""
        async with self._lock:
            return await self._snapshot()

    async def _snapshot(self) -> ProgressSnapshot:
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

    async def replace_execution_graph(
        self,
        execution_graph: ExecutionGraph,
        retired_counts: Mapping[ExecutionVertexId, SubtaskCounts] | None = None,
    ) -> None:
        """Adopt a resized graph and retain cumulative counts of removed tasks."""

        async with self._lock:
            final_counts = {} if retired_counts is None else retired_counts
            old_vertex_ids = {vertex.id for vertex in self._execution_graph.execution_vertices}
            new_vertex_ids = {vertex.id for vertex in execution_graph.execution_vertices}
            for vertex_id in old_vertex_ids - new_vertex_ids:
                counts = final_counts.get(vertex_id)
                cached_counts = self._last_counts.pop(vertex_id, None)
                if counts is None:
                    counts = cached_counts
                if counts is None:
                    continue
                retired = self._cumulative_counts(counts)
                previous = self._retired_counts.get(vertex_id.job_vertex_id, SubtaskCounts())
                self._retired_counts[vertex_id.job_vertex_id] = self._sum_counts([previous, retired])
            live_job_vertex_ids = set(execution_graph.job_vertices)
            self._retired_counts = {
                job_vertex_id: counts
                for job_vertex_id, counts in self._retired_counts.items()
                if job_vertex_id in live_job_vertex_ids
            }
            self._execution_graph = execution_graph

    async def _probe_counts(
        self, vertices_by_job_vertex
    ) -> tuple[dict[ExecutionVertexId, SubtaskCounts], set[ExecutionVertexId]]:
        probed_vertices = [
            vertex for vertices in vertices_by_job_vertex for vertex in vertices if vertex.stream_task is not None
        ]
        requests = [vertex.stream_task.progress_counts() for vertex in probed_vertices]
        results = (
            await asyncio.gather(
                *(klein.aget(request, timeout=_PROGRESS_RPC_TIMEOUT_SECONDS) for request in requests),
                return_exceptions=True,
            )
            if requests
            else []
        )

        counts_by_vertex = {}
        failed_vertices: set[ExecutionVertexId] = set()
        for vertex, result in zip(probed_vertices, results, strict=True):
            vertex_key = vertex.id
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
        counts = [counts_by_vertex.get(vertex.id, SubtaskCounts()) for vertex in vertices]
        live_totals = self._sum_counts(counts)
        retired = self._retired_counts.get(job_vertex.id, SubtaskCounts())
        totals = self._sum_counts([live_totals, retired])
        failed_progress_requests = sum(1 for vertex in vertices if vertex.id in failed_vertices)
        resources = job_vertex.resources
        subtasks = tuple(
            self._subtask_progress(vertex, count, vertex.id in failed_vertices, job_running)
            for vertex, count in zip(vertices, counts, strict=True)
        )
        return OperatorProgress(
            name=job_vertex.name,
            op_id=job_vertex.id,
            parallelism=job_vertex.concurrency,
            status=self._aggregate_status(statuses, job_running, failed_progress_requests),
            rows_in=totals.rows_in,
            rows_out=totals.rows_out,
            bytes_in=totals.bytes_in,
            bytes_out=totals.bytes_out,
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

    @staticmethod
    def _cumulative_counts(counts: SubtaskCounts) -> SubtaskCounts:
        return SubtaskCounts(
            **{
                field.name: getattr(counts, field.name)
                for field in fields(SubtaskCounts)
                if field.name in _CUMULATIVE_COUNT_FIELDS
            }
        )

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
            actor_id=getattr(vertex.stream_task, "actor_id", None),
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
