# SPDX-License-Identifier: Apache-2.0
"""Node-assignment helpers for round-robin placement.

Plain module-level functions over a small ``WorkerNode`` value object — these
are stateless algorithms, so they are functions, not methods on namespace-only
classes. Consumed by the placement strategies in ``scheduler/placement.py``.
"""

from dataclasses import dataclass

import ray.util.state

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex

logger = get_logger(__name__)


@dataclass(slots=True)
class WorkerNode:
    """A Ray node's index + remaining CPU/GPU + assigned task count.

    Mutable state (resources decrement, assigned_tasks increments as vertices are
    assigned), so this is a real object rather than a free function's tuple.
    """

    index: int
    cpu: float
    gpu: float
    assigned_tasks: int = 0

    def can_host(self, job_vertex: ExecutionJobVertex) -> bool:
        """Whether this node has enough remaining resources for one subtask."""
        return self.cpu >= job_vertex.resources.cpus and self.gpu >= job_vertex.resources.gpus

    def assign(self, job_vertex: ExecutionJobVertex) -> None:
        """Consume the resources required by one subtask."""
        if not self.can_host(job_vertex):
            raise ValueError(f"Node {self.index} has insufficient resources for {job_vertex.name}")
        self.cpu -= job_vertex.resources.cpus
        self.gpu -= job_vertex.resources.gpus
        self.assigned_tasks += 1


def cluster_worker_nodes() -> tuple[list[WorkerNode], list[str]]:
    """Return resource snapshots and IDs for live, non-head Ray nodes."""
    ray_nodes = ray.util.state.list_nodes(
        address=ray.get_runtime_context().gcs_address,
        limit=10000,
        filters=[("state", "=", "ALIVE"), ("is_head_node", "=", False)],
    )
    node_ids = [node.node_id for node in ray_nodes]
    workers = [
        WorkerNode(i, float(node.resources_total.get("CPU", 0)), float(node.resources_total.get("GPU", 0)))
        for i, node in enumerate(ray_nodes)
    ]
    return workers, node_ids


def round_robin_allocate(
    job_vertices: list[ExecutionJobVertex],
    nodes: list[WorkerNode],
) -> tuple[dict[int, list[int]], bool]:
    """Round-robin assign each job vertex's subtasks across nodes.

    Returns a mapping from job vertex ID to node index per subtask plus a
    success flag. A failed allocation leaves the input snapshots untouched.
    """
    assignments = {job_vertex.id: [-1] * job_vertex.concurrency for job_vertex in job_vertices}
    ordered_vertices = sorted(
        job_vertices,
        key=lambda job_vertex: (job_vertex.resources.gpus, job_vertex.resources.cpus, job_vertex.concurrency),
        reverse=True,
    )
    node_count = len(nodes)
    available_nodes = [WorkerNode(node.index, node.cpu, node.gpu, node.assigned_tasks) for node in nodes]
    if not available_nodes and any(job_vertex.concurrency for job_vertex in ordered_vertices):
        return {}, False
    for job_vertex in ordered_vertices:
        available_nodes.sort(
            key=lambda node: (node.gpu, node.cpu, -node.assigned_tasks, -node.index),
            reverse=True,
        )
        node_cursor = 0
        for subtask_index in range(job_vertex.concurrency):
            assigned = False
            for _ in range(node_count):
                worker = available_nodes[node_cursor]
                if worker.can_host(job_vertex):
                    assignments[job_vertex.id][subtask_index] = worker.index
                    worker.assign(job_vertex)
                    assigned = True
                node_cursor = (node_cursor + 1) % node_count
                if assigned:
                    break
            if not assigned:
                return {}, False
    logger.debug("Computed round-robin node assignment: %s", assignments)
    return assignments, True
