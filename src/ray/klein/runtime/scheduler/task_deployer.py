# SPDX-License-Identifier: Apache-2.0
"""Worker creation / deploy / start mechanics for the JobMaster.

Stateless module-level functions (no per-call state). The JobMaster owns policy
and state; this is the mechanism layer that turns a placement decision into live
StreamTask actors and drives them to RUNNING. Teardown lives in
``task_terminator``. All failures raise ``DeploymentError`` / ``PlacementError``
— there is no (bool, err) return.
"""

from ray.util.queue import Queue

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.runtime.actor import create_remote_actor
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.scheduler.errors import DeploymentError
from ray.klein.runtime.scheduler.placement import PlacementPlan, PlacementStrategy
from ray.klein.runtime.scheduler.task_deployment_descriptor import (
    OutputEdgeDescriptor,
    TaskDeploymentDescriptor,
)
from ray.klein.runtime.worker.source_stream_task import SourceStreamTask
from ray.klein.runtime.worker.stream_task import StreamTask

logger = get_logger(__name__)


def has_schedulable_worker_nodes() -> bool:
    """Whether the cluster has any non-head ALIVE node for affinity/PG placement.

    Round-Robin / PlacementGroup place tasks only on non-head worker nodes; with
    none available the caller falls back to Ray-native placement.
    """
    from ray.klein.runtime.scheduler.assignment import cluster_worker_nodes

    workers, _ = cluster_worker_nodes()
    return bool(workers)


def validate_vertex_statuses(execution_graph: ExecutionGraph) -> None:
    """Assert every execution vertex is globally-terminal before (re)creation."""
    for vertex in execution_graph.execution_vertices:
        current_status = vertex.status
        if current_status != ExecutionVertexStatus.CREATED and not current_status.is_terminal:
            raise DeploymentError(
                "create workers",
                f"ExecutionVertex '{vertex}' is in {current_status}, which can not be recreated. Please stop first.",
            )


def place_workers(
    execution_graph: ExecutionGraph,
    strategy: PlacementStrategy,
) -> PlacementPlan:
    """Compute a placement with ``strategy`` and instantiate every actor into it.

    The single create path shared by all strategies: ``strategy.plan`` raises
    ``PlacementError`` if infeasible (the caller falls through to the next
    strategy); on a mid-instantiate failure we cancel partial tasks, roll back
    the plan's reserved resource (e.g. the PG), and re-raise. Returns the plan so
    the caller can hold any resource it created (e.g. the PlacementGroup).
    """
    plan = strategy.plan(execution_graph)
    try:
        for job_vertex in execution_graph.job_vertices.values():
            _instantiate_job_vertex(execution_graph, job_vertex, plan)
    except Exception as error:
        for job_vertex in execution_graph.job_vertices.values():
            _cancel_created_tasks(job_vertex)
        plan.rollback()
        raise DeploymentError("create workers", error) from error
    return plan


def _instantiate_job_vertex(
    graph: ExecutionGraph,
    job_vertex: ExecutionJobVertex,
    plan: PlacementPlan,
) -> None:
    """Create every actor for one physical job vertex.

    Actor creation is deployment mechanism, not graph data, so it lives in the
    deployer rather than on :class:`ExecutionJobVertex`.
    """

    job_vertex.output_queue = Queue() if job_vertex.operator_spec.collecting else None
    for vertex in job_vertex.execution_vertices.values():
        vertex.reset()
        descriptor = _build_descriptor(graph, job_vertex, vertex)
        remote_args = {
            "name": vertex.name,
            "num_cpus": vertex.resources.cpus,
            "num_gpus": vertex.resources.gpus,
            "max_restarts": -1,
            "namespace": graph.namespace,
        }
        placement_group = None
        bundle_index = -1
        schedule_node_id = None
        if plan.uses_placement_group:
            placement_group = plan.placement_group
            bundle_index = plan.bundle_for(vertex.id)
        elif plan.node_by_vertex:
            schedule_node_id = plan.node_for(vertex.id)
        vertex.stream_task = create_remote_actor(
            _task_class_for(vertex),
            construct_args={"descriptor": descriptor},
            ray_remote_args=remote_args,
            schedule_node_id=schedule_node_id,
            placement_group=placement_group,
            placement_group_bundle_index=bundle_index,
        )
        placement = f"pg[bundle={bundle_index}]" if placement_group is not None else f"node={schedule_node_id}"
        logger.debug("Created execution vertex %s with placement %s", vertex.name, placement)


def _build_descriptor(
    graph: ExecutionGraph,
    job_vertex: ExecutionJobVertex,
    vertex: ExecutionVertex,
) -> TaskDeploymentDescriptor:
    config = job_vertex.config
    output_buffer_max_rows = config.get(PipelineOptions.OUTPUT_BUFFER_MAX_ROWS)
    put_timeout = config.get(PipelineOptions.INPUT_BUFFER_PUT_TIMEOUT).total_seconds()
    out_edges = tuple(
        OutputEdgeDescriptor(
            target_task_names=tuple(
                target.name for target in graph.job_vertex(output_edge.target).execution_vertices.values()
            ),
            partitioner=output_edge.partitioner,
            control_target_indices=output_edge.partitioner.target_indices(
                job_vertex.concurrency,
                graph.job_vertex(output_edge.target).concurrency,
                vertex.index,
            ),
            output_buffer_max_rows=output_buffer_max_rows,
            put_timeout=put_timeout,
        )
        for output_edge in graph.output_job_edges(job_vertex.id)
    )
    input_vertex_ids = tuple(
        edge.source.id
        for input_edge in graph.input_job_edges(job_vertex.id)
        for edge in input_edge.execution_edges
        if edge.target.id == vertex.id
    )
    return TaskDeploymentDescriptor(
        operator=job_vertex.operator_spec,
        vertex_id=vertex.id,
        task_name=vertex.name,
        task_index=vertex.index,
        parallelism=vertex.concurrency,
        config=config,
        metric_group=vertex.task_metric_group,
        barrier_split=graph.barrier_splits[vertex.id],
        is_committer=job_vertex.id in graph.sink_job_vertices,
        out_edges=out_edges,
        input_buffer_size=config.get(PipelineOptions.INPUT_BUFFER_SIZE),
        output_queue=job_vertex.output_queue,
        namespace=graph.namespace,
        input_vertex_ids=input_vertex_ids,
    )


def _task_class_for(vertex: ExecutionVertex) -> type[StreamTask]:
    return SourceStreamTask if vertex.operator_spec.source else StreamTask


def _cancel_created_tasks(job_vertex: ExecutionJobVertex) -> None:
    for vertex in job_vertex.execution_vertices.values():
        if vertex.stream_task is not None:
            klein.kill(vertex.stream_task)
            vertex.stream_task = None


def deploy_workers(execution_graph: ExecutionGraph) -> None:
    """Move every created vertex to DEPLOYED.

    With the self-contained TaskDeploymentDescriptor there is nothing to push into
    the actors at deploy time — the actor builds its own collector / snapshot
    strategy / context in setup_and_run(). DEPLOYED is retained (not skipped to
    RUNNING) because a fast source can report FINISHED before start_workers marks
    it RUNNING, and FINISHED is only permitted from RUNNING or DEPLOYED.
    """
    for vertex in execution_graph.execution_vertices:
        if vertex.stream_task is None:
            vertex.transition_to(ExecutionVertexStatus.FAILED)
            raise DeploymentError("deploy workers", f"ExecutionVertex '{vertex}' has not been created yet.")
        vertex.transition_to(ExecutionVertexStatus.DEPLOYED)


def start_workers(execution_graph: ExecutionGraph, timeout: float) -> None:
    """Start the DEPLOYED workers sink-first so downstreams are ready before their
    upstreams produce. Raises ``DeploymentError`` if any vertex fails.

    The ordering invariant only requires each job vertex to start after its
    downstreams, not full serialization. Vertices are grouped by longest
    distance to a sink (an edge always crosses levels, so same-level vertices
    are independent) and each level
    is started in a single batched RPC — a deep chain costs one round-trip per
    level instead of one per job vertex.
    """
    levels = _sink_first_levels(execution_graph)
    for level in levels:
        job_vertices = [execution_graph.job_vertex(vertex_id) for vertex_id in level]
        try:
            _start_wave(job_vertices, timeout=timeout)
        except Exception as error:
            logger.exception("Failed to start a deployment wave containing %d operators", len(job_vertices))
            raise DeploymentError("start workers", error) from error


def _sink_first_levels(execution_graph: ExecutionGraph) -> list[list[int]]:
    """Job vertex IDs bucketed by distance to a sink, sinks first."""
    depth: dict[int, int] = {}

    def sink_distance(job_vertex_id: int) -> int:
        if job_vertex_id not in depth:
            downstream = execution_graph.downstream_job_vertices(job_vertex_id)
            depth[job_vertex_id] = (
                0 if not downstream else 1 + max(sink_distance(vertex_id) for vertex_id in downstream)
            )
        return depth[job_vertex_id]

    for job_vertex_id in execution_graph.job_vertices:
        sink_distance(job_vertex_id)
    max_level = max(depth.values(), default=-1)
    return [[vertex_id for vertex_id, distance in depth.items() if distance == level] for level in range(max_level + 1)]


def _start_wave(job_vertices: list[ExecutionJobVertex], timeout: float) -> None:
    vertices = [vertex for job_vertex in job_vertices for vertex in job_vertex.execution_vertices.values()]
    klein.get([vertex.stream_task.setup_and_run() for vertex in vertices], timeout=timeout)
    for vertex in vertices:
        if vertex.status == ExecutionVertexStatus.DEPLOYED:
            vertex.transition_to(ExecutionVertexStatus.RUNNING)


def bootstrap_vertex(vertex, timeout: float) -> None:
    """(Re)bootstrap one vertex's actor and wait for it — the shared mechanism
    behind both initial deploy and single-point recovery.

    ``setup_and_run`` is idempotent (a no-op on a live actor), which is what makes
    it safe to reuse for recovering a Ray-rebuilt actor. Does NOT transition
    status — the caller owns that: initial deploy moves DEPLOYED→RUNNING; recovery
    must NOT re-transition an already-RUNNING vertex (it may have reached a
    terminal state during the crash window).
    """
    klein.get(vertex.stream_task.setup_and_run(), timeout=timeout)
