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
from ray.klein._internal.deadline import Deadline
from ray.klein._internal.logging import get_logger
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.scheduler.errors import TeardownError
from ray.klein.runtime.scheduler.vertex_selection import select_vertices

_KILL_ACTOR_MAX_RETRIES = 3
_KILL_ACTOR_RETRY_DELAY = 1.0


logger = get_logger(__name__)


def stop_workers(execution_graph: ExecutionGraph, timeout: float, force: bool) -> None:
    """Tear every worker down: request a stop, then force-kill any survivor.

    Phase 1 (``_request_graceful_stop``) asks each task to stop (or kills it when
    ``force``) and waits up to ``timeout`` for acks — best-effort, a timeout does
    NOT skip phase 2. Phase 2 (``force_kill_survivors``) kills any still-ALIVE
    actor and reconciles vertex status.
    """
    deadline = Deadline(timeout)
    try:
        _request_graceful_stop(execution_graph, deadline.step(timeout), force)
    except Exception:
        # Teardown is deliberately two-phase. An unexpected phase-one failure
        # must never prevent the named-actor survivor sweep.
        logger.exception("Graceful stop phase failed; force-killing survivors")
    _force_kill_survivors(execution_graph)


def stop_job_vertex(
    job_vertex: ExecutionJobVertex,
    namespace: str,
    timeout: float,
    force: bool = False,
    *,
    vertices: Iterable[ExecutionVertex] | None = None,
    rescale_operation_id: str | None = None,
) -> None:
    """Stop one operator's actors, or only an explicit rescale delta."""

    selected = select_vertices(job_vertex, vertices)
    deadline = Deadline(timeout)
    try:
        references = _stop_worker(
            job_vertex,
            force,
            selected,
            rescale_operation_id=rescale_operation_id,
            timeout=deadline.remaining(),
        )
        klein.get(references, timeout=timeout)
    except Exception as error:
        logger.warning("Operator stop did not complete within %.1fs; force-killing survivors: %s", timeout, error)
    if len(selected) == 1:
        name = selected[0].name
        survivor_names = {
            name
            for name in (name,)
            if _actor_may_exist(name, namespace) and not _kill_actor_with_retry(name, namespace)
        }
    else:
        survivor_names = _kill_actor_names(
            (vertex.name for vertex in selected),
            namespace,
            deadline.step(timeout),
        )
    for vertex in selected:
        if vertex.name in survivor_names:
            continue
        if vertex.status != ExecutionVertexStatus.CREATED and not vertex.status.is_terminal:
            vertex.transition_to(ExecutionVertexStatus.CANCELLED)
        vertex.stream_task = None
    if survivor_names:
        raise TeardownError(f"failed to stop operator actor(s): {sorted(survivor_names)}")


def _request_graceful_stop(execution_graph: ExecutionGraph, timeout: float, force: bool) -> None:
    """Phase 1: ask every task (source-first BFS) to stop and wait for acks.

    Best-effort: if tasks don't ack within ``timeout`` the caller still runs the
    force-kill sweep, so a stuck task can't leave orphan actors consuming
    resources.
    """
    references = []
    pending_queue: deque = deque()
    visited = set()
    for job_vertex_id in execution_graph.source_job_vertices:
        pending_queue.append(job_vertex_id)

    while pending_queue:
        job_vertex_id = pending_queue.popleft()
        if job_vertex_id in visited:
            continue
        visited.add(job_vertex_id)
        job_vertex = execution_graph.job_vertex(job_vertex_id)
        references.extend(_stop_worker(job_vertex, force))
        for downstream_job_vertex_id in execution_graph.downstream_job_vertices(job_vertex.id):
            pending_queue.append(downstream_job_vertex_id)

    try:
        klein.get(references, timeout=timeout)
    except Exception as error:
        logger.warning("Graceful stop did not complete within %.1fs; force-killing survivors: %s", timeout, error)


def force_kill_survivors(execution_graph: ExecutionGraph, timeout: float = 2.0) -> None:
    """Phase 2: kill any actor still ALIVE and reconcile vertex status."""
    namespace = execution_graph.namespace
    vertices = tuple(execution_graph.execution_vertices)
    survivor_names = _kill_actor_names(
        (vertex.name for vertex in vertices),
        namespace,
        timeout,
    )
    for vertex in vertices:
        if vertex.name in survivor_names:
            continue
        # Only cancel vertices not already globally-terminal: a FAILED (logical
        # failure) or FINISHED vertex must keep that status — forcing CANCELLED
        # would lose the real outcome and trip the state machine (no
        # FAILED→CANCELLED edge). RUNNING/CANCELLING/DEPLOYED ones go to CANCELLED.
        if vertex.status != ExecutionVertexStatus.CREATED and not vertex.status.is_terminal:
            vertex.transition_to(ExecutionVertexStatus.CANCELLED)
        vertex.stream_task = None
    if survivor_names:
        raise TeardownError(f"failed to stop worker actor(s): {sorted(survivor_names)}")


def _force_kill_survivors(execution_graph: ExecutionGraph) -> None:
    """Compatibility sweep using the former per-actor retry surface."""

    survivors = []
    for vertex in execution_graph.execution_vertices:
        if _actor_may_exist(vertex.name, execution_graph.namespace) and not _kill_actor_with_retry(
            vertex.name, execution_graph.namespace
        ):
            survivors.append(vertex.name)
            continue
        if vertex.status != ExecutionVertexStatus.CREATED and not vertex.status.is_terminal:
            vertex.transition_to(ExecutionVertexStatus.CANCELLED)
        vertex.stream_task = None
    if survivors:
        raise TeardownError(f"failed to stop worker actor(s): {survivors}")


def _actor_may_exist(name: str, namespace: str) -> bool:
    """Treat an unavailable actor-status service conservatively as a survivor."""
    try:
        return klein.get_actor_status(name, namespace=namespace) != StreamTaskStatus.NOT_EXIST
    except Exception as error:
        logger.warning("Could not query actor %s before teardown; attempting kill: %s", name, error)
        return True


def _kill_actor_names(
    names: Iterable[str],
    namespace: str,
    timeout: float,
) -> set[str]:
    """Kill many named actors in shared retry rounds under one deadline."""

    pending = _existing_actor_names(set(names), namespace)
    if not pending:
        return set()
    deadline = Deadline(timeout)
    for attempt in range(_KILL_ACTOR_MAX_RETRIES):
        pending = _kill_actor_round(pending, namespace)
        if not pending:
            return set()
        if attempt < _KILL_ACTOR_MAX_RETRIES - 1:
            logger.warning(
                "%d actor(s) are still alive after kill attempt %d of %d; retrying",
                len(pending),
                attempt + 1,
                _KILL_ACTOR_MAX_RETRIES,
            )
            remaining = deadline.remaining()
            if remaining <= 0:
                break
            time.sleep(min(_KILL_ACTOR_RETRY_DELAY, remaining))
    logger.error(
        "Failed to kill actor(s) %s; they may still be running and consuming resources",
        sorted(pending),
    )
    return pending


def _existing_actor_names(names: set[str], namespace: str) -> set[str]:
    return {name for name in names if _actor_may_exist(name, namespace)}


def _kill_actor_round(names: set[str], namespace: str) -> set[str]:
    for name in names:
        try:
            logger.debug("Force-killing stream task %s", name)
            klein.kill_actor_by_name(name, namespace=namespace)
        except Exception:
            logger.debug("Named actor kill failed for %s", name, exc_info=True)
    survivors = set()
    for name in names:
        try:
            if klein.get_actor_status(name, namespace=namespace) != StreamTaskStatus.NOT_EXIST:
                survivors.add(name)
        except Exception:
            survivors.add(name)
    return survivors


def _kill_actor_with_retry(name: str, namespace: str, timeout: float = 2.0) -> bool:
    """Compatibility wrapper for one named actor."""
    deadline = Deadline(timeout)
    pending = {name}
    for attempt in range(_KILL_ACTOR_MAX_RETRIES):
        pending = _kill_actor_round(pending, namespace)
        if not pending:
            return True
        if attempt < _KILL_ACTOR_MAX_RETRIES - 1:
            remaining = deadline.remaining()
            if remaining <= 0:
                break
            time.sleep(min(_KILL_ACTOR_RETRY_DELAY, remaining))
    return False


def _stop_worker(
    job_vertex: ExecutionJobVertex,
    force: bool,
    vertices: Iterable[ExecutionVertex] | None = None,
    *,
    rescale_operation_id: str | None = None,
    timeout: float = 30.0,
) -> list[KleinActorHandle]:
    references = []
    for vertex in select_vertices(job_vertex, vertices):
        if force:
            if vertex.stream_task is not None:
                try:
                    klein.kill(vertex.stream_task)
                except Exception as error:
                    logger.warning("Could not force-kill execution vertex %s by handle: %s", vertex, error)
            continue
        if vertex.status == ExecutionVertexStatus.CREATED or vertex.status.is_terminal:
            logger.debug("Skipping inactive execution vertex %s during task shutdown", vertex)
            continue
        if vertex.stream_task is None:
            logger.warning("Execution vertex %s has no actor handle; deferring to named-actor cleanup", vertex)
            continue
        try:
            if rescale_operation_id is None:
                reference = vertex.stream_task.stop()
            else:
                reference = vertex.stream_task.retire_rescale(rescale_operation_id, timeout)
        except Exception as error:
            logger.warning("Execution vertex %s rejected graceful stop: %s", vertex, error)
            continue
        references.append(reference)
        vertex.transition_to(ExecutionVertexStatus.CANCELLING)
    return references


def _select_vertices(
    job_vertex: ExecutionJobVertex,
    vertices: Iterable[ExecutionVertex] | None,
) -> tuple[ExecutionVertex, ...]:
    """Compatibility validator that also supports lightweight test doubles."""

    if vertices is None:
        return tuple(job_vertex.execution_vertices.values())
    selected = tuple(vertices)
    seen: set[int] = set()
    for vertex in selected:
        if not isinstance(vertex, ExecutionVertex):
            raise TypeError("vertex subsets must contain ExecutionVertex values")
        if job_vertex.execution_vertices.get(vertex.index) is not vertex:
            raise ValueError(f"ExecutionVertex index {vertex.index} does not belong to operator {job_vertex.name}")
        if vertex.index in seen:
            raise ValueError(f"duplicate ExecutionVertex index {vertex.index}")
        seen.add(vertex.index)
    return selected
