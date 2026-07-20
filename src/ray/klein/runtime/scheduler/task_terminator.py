# SPDX-License-Identifier: Apache-2.0
"""Worker teardown mechanics for the JobMaster.

Stateless module-level functions. Tears StreamTask actors down in two phases:
request a graceful stop, then force-kill any survivor — so no orphan actor
outlives the stop. The JobMaster decides *when* to stop; this is the *how*.
"""

import time
from collections import deque
from collections.abc import Iterable

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)

_KILL_ACTOR_MAX_RETRIES = 3
_KILL_ACTOR_RETRY_DELAY = 1.0


logger = get_logger(__name__)


def stop_workers(execution_graph: ExecutionGraph, timeout: float, force: bool) -> None:
    """Tear every worker down: request a stop, then force-kill any survivor.

    Phase 1 (``_request_graceful_stop``) asks each task to stop (or kills it when
    ``force``) and waits up to ``timeout`` for acks — best-effort, a timeout does
    NOT skip phase 2. Phase 2 (``_force_kill_survivors``) kills any still-ALIVE
    actor and reconciles vertex status.
    """
    _request_graceful_stop(execution_graph, timeout, force)
    _force_kill_survivors(execution_graph)


def stop_job_vertex(
    job_vertex: ExecutionJobVertex,
    namespace: str,
    timeout: float,
    force: bool = False,
    *,
    vertices: Iterable[ExecutionVertex] | None = None,
) -> None:
    """Stop one operator's actors, or only an explicit rescale delta."""

    selected = _select_vertices(job_vertex, vertices)
    references = _stop_worker(job_vertex, force, selected)
    try:
        klein.get(references, timeout=timeout)
    except Exception as error:
        logger.warning("Operator stop did not complete within %.1fs; force-killing survivors: %s", timeout, error)
    survivors = []
    for vertex in selected:
        if klein.get_actor_status(
            vertex.name, namespace=namespace
        ) != StreamTaskStatus.NOT_EXIST and not _kill_actor_with_retry(vertex.name, namespace):
            survivors.append(vertex.name)
            continue
        if vertex.status != ExecutionVertexStatus.CREATED and not vertex.status.is_terminal:
            vertex.transition_to(ExecutionVertexStatus.CANCELLED)
        vertex.stream_task = None
    if survivors:
        raise RuntimeError(f"failed to stop operator actor(s): {survivors}")


def _request_graceful_stop(execution_graph: ExecutionGraph, timeout: float, force: bool) -> None:
    """Phase 1: ask every task (source-first BFS) to stop and wait for acks.

    Best-effort: if tasks don't ack within ``timeout`` the caller still runs the
    force-kill sweep, so a stuck task can't leave orphan actors consuming
    resources.
    """
    references = []
    pending_queue: deque = deque()
    for job_vertex_id in execution_graph.source_job_vertices:
        pending_queue.append(job_vertex_id)

    while pending_queue:
        job_vertex_id = pending_queue.popleft()
        job_vertex = execution_graph.job_vertex(job_vertex_id)
        references.extend(_stop_worker(job_vertex, force))
        for downstream_job_vertex_id in execution_graph.downstream_job_vertices(job_vertex.id):
            pending_queue.append(downstream_job_vertex_id)

    try:
        klein.get(references, timeout=timeout)
    except Exception as error:
        logger.warning("Graceful stop did not complete within %.1fs; force-killing survivors: %s", timeout, error)


def _force_kill_survivors(execution_graph: ExecutionGraph) -> None:
    """Phase 2: kill any actor still ALIVE and reconcile vertex status."""
    namespace = execution_graph.namespace
    for vertex in execution_graph.execution_vertices:
        if klein.get_actor_status(vertex.name, namespace=namespace) != StreamTaskStatus.NOT_EXIST:
            logger.debug("Force-killing stream task %s", vertex.name)
            if not _kill_actor_with_retry(vertex.name, namespace):
                continue
        # Only cancel vertices not already globally-terminal: a FAILED (logical
        # failure) or FINISHED vertex must keep that status — forcing CANCELLED
        # would lose the real outcome and trip the state machine (no
        # FAILED→CANCELLED edge). RUNNING/CANCELLING/DEPLOYED ones go to CANCELLED.
        if vertex.status != ExecutionVertexStatus.CREATED and not vertex.status.is_terminal:
            vertex.transition_to(ExecutionVertexStatus.CANCELLED)
        vertex.stream_task = None


def _kill_actor_with_retry(name: str, namespace: str) -> bool:
    last_error: Exception | None = None
    for attempt in range(_KILL_ACTOR_MAX_RETRIES):
        try:
            klein.kill_actor_by_name(name, namespace=namespace)
        except Exception as error:
            last_error = error
        try:
            if klein.get_actor_status(name, namespace=namespace) == StreamTaskStatus.NOT_EXIST:
                return True
        except Exception as error:
            last_error = error
        if attempt < _KILL_ACTOR_MAX_RETRIES - 1:
            logger.warning(
                "Actor %s is still alive after kill attempt %d of %d; retrying",
                name,
                attempt + 1,
                _KILL_ACTOR_MAX_RETRIES,
            )
            time.sleep(_KILL_ACTOR_RETRY_DELAY)
    logger.error(
        "Failed to kill actor %s after %d attempts; actor may still be running and consuming resources.%s",
        name,
        _KILL_ACTOR_MAX_RETRIES,
        "" if last_error is None else f" Last error: {last_error}",
    )
    return False


def _stop_worker(
    job_vertex: ExecutionJobVertex,
    force: bool,
    vertices: Iterable[ExecutionVertex] | None = None,
) -> list[KleinActorHandle]:
    references = []
    for vertex in _select_vertices(job_vertex, vertices):
        if force:
            if vertex.stream_task is not None:
                klein.kill(vertex.stream_task)
            continue
        if vertex.status == ExecutionVertexStatus.CREATED or vertex.status.is_terminal:
            logger.debug("Skipping inactive execution vertex %s during task shutdown", vertex)
            continue
        references.append(vertex.stream_task.stop())
        vertex.transition_to(ExecutionVertexStatus.CANCELLING)
    return references


def _select_vertices(
    job_vertex: ExecutionJobVertex,
    vertices: Iterable[ExecutionVertex] | None,
) -> tuple[ExecutionVertex, ...]:
    if vertices is None:
        # Preserve the historical whole-job-vertex path, including lightweight
        # test doubles used by teardown callers.
        return tuple(job_vertex.execution_vertices.values())
    selected = tuple(vertices)
    seen: set[int] = set()
    for vertex in selected:
        if not isinstance(vertex, ExecutionVertex):
            raise TypeError("vertex subsets must contain ExecutionVertex values")
        if job_vertex.execution_vertices.get(vertex.index) is not vertex:
            raise ValueError(f"ExecutionVertex '{vertex}' does not belong to operator {job_vertex.name}")
        if vertex.index in seen:
            raise ValueError(f"duplicate ExecutionVertex index {vertex.index}")
        seen.add(vertex.index)
    return selected
