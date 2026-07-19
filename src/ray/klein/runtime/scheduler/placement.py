# SPDX-License-Identifier: Apache-2.0
"""Placement strategies + plans for scheduling StreamTask actors.

A ``PlacementStrategy`` decides *where* a job's subtasks run; a ``PlacementPlan``
is its resolved output, consumed by ``ExecutionJobVertex.instantiate`` (which
doesn't know how the plan was computed). The cascade in JobMaster tries
strategies in order, each raising ``PlacementError`` when infeasible so the next
is tried:

* **PlacementGroupStrategy** (default): gang-schedules the whole job into one Ray
  PlacementGroup, bundles grouped by FORWARD-affinity so a chain's same-index
  subtasks co-locate (local, serialization-free handoffs) while shuffle edges
  spread across nodes. Reservation is all-or-nothing.
* **RoundRobinStrategy** (fallback): a per-vertex node pin
  applied via ``NodeAffinitySchedulingStrategy``.
* **NativeStrategy** (final fallback): an empty plan; Ray schedules freely.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.scheduler.errors import PlacementError

logger = get_logger(__name__)

if TYPE_CHECKING:
    from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph


@dataclass(slots=True)
class PlacementPlan:
    """Where each execution vertex's actor should be scheduled.

    Empty (all fields default) means "Ray native placement" — no constraint. At
    most one of the node-pin / placement-group shapes is populated. ``on_rollback``
    releases any reserved resource (e.g. the PG) if instantiation fails midway.
    """

    node_by_vertex: dict[ExecutionVertexId, str] = field(default_factory=dict)
    placement_group: object | None = None
    bundle_by_vertex: dict[ExecutionVertexId, int] = field(default_factory=dict)
    on_rollback: Callable[[], None] | None = None

    def node_for(self, vertex_id: ExecutionVertexId) -> str | None:
        return self.node_by_vertex.get(vertex_id)

    def bundle_for(self, vertex_id: ExecutionVertexId) -> int:
        return self.bundle_by_vertex.get(vertex_id, -1)

    @property
    def uses_placement_group(self) -> bool:
        return self.placement_group is not None

    def rollback(self) -> None:
        if self.on_rollback is not None:
            self.on_rollback()


class PlacementStrategy(ABC):
    """Computes a PlacementPlan for an ExecutionGraph (raises PlacementError)."""

    name: str = "placement"

    @abstractmethod
    def plan(self, execution_graph: "ExecutionGraph") -> PlacementPlan:
        """Return a PlacementPlan, or raise ``PlacementError`` if infeasible."""


class NativeStrategy(PlacementStrategy):
    """Ray-native placement: no constraints, always feasible."""

    name = "native"

    def plan(self, _execution_graph: "ExecutionGraph") -> PlacementPlan:
        return PlacementPlan()


class RoundRobinStrategy(PlacementStrategy):
    """Round-robin node pinning across non-head worker nodes."""

    name = "round-robin"

    def plan(self, execution_graph: "ExecutionGraph") -> PlacementPlan:
        from ray.klein.runtime.scheduler.assignment import (
            cluster_worker_nodes,
            round_robin_allocate,
        )

        job_vertices = list(execution_graph.job_vertices.values())
        nodes, node_ids = cluster_worker_nodes()
        assignments, allocated = round_robin_allocate(job_vertices, nodes)
        if not allocated:
            raise PlacementError(self.name, "nodes have no enough resources to assign")
        return _node_pin_plan(execution_graph, assignments, node_ids)


class PlacementGroupStrategy(PlacementStrategy):
    """Gang-scheduled, FORWARD-affinity-grouped Ray PlacementGroup (default)."""

    name = "placement-group"

    def __init__(self, strategy: str, ready_timeout: float) -> None:
        self._placement_group_strategy = strategy
        self._ready_timeout = ready_timeout

    def plan(self, execution_graph: "ExecutionGraph") -> PlacementPlan:
        from ray.util.placement_group import placement_group, remove_placement_group

        import ray.klein as klein

        bundles = []
        bundle_by_vertex: dict[ExecutionVertexId, int] = {}
        vertex_by_id = {vertex.id: vertex for vertex in execution_graph.execution_vertices}
        for group in execution_graph.affinity_groups:
            for vertex_id in group:
                vertex = vertex_by_id[vertex_id]
                bundle = {"CPU": vertex.resources.cpus}
                if vertex.resources.gpus:
                    bundle["GPU"] = vertex.resources.gpus
                bundle_by_vertex[vertex_id] = len(bundles)
                bundles.append(bundle)
        if not bundles:
            raise PlacementError(self.name, "no bundles to place")

        group = placement_group(bundles, strategy=self._placement_group_strategy)
        try:
            klein.get(group.ready(), timeout=self._ready_timeout)
        except Exception as error:
            remove_placement_group(group)
            raise PlacementError(self.name, f"group not ready in {self._ready_timeout}s: {error}") from error
        logger.info("Reserved a placement group with strategy %s", self._placement_group_strategy)
        return PlacementPlan(
            placement_group=group,
            bundle_by_vertex=bundle_by_vertex,
            on_rollback=lambda: remove_placement_group(group),
        )


def _node_pin_plan(
    execution_graph: "ExecutionGraph",
    assignments: dict[int, list[int]],
    node_ids: list[str],
) -> PlacementPlan:
    """Resolve per-job-vertex node indices into physical vertex node IDs."""
    node_by_vertex: dict[ExecutionVertexId, str] = {}
    for job_vertex_id, subtask_nodes in assignments.items():
        job_vertex = execution_graph.find_job_vertex(job_vertex_id)
        if job_vertex is None:
            raise ValueError(f"Unknown job vertex {job_vertex_id}")
        for subtask_index, vertex in job_vertex.execution_vertices.items():
            node_by_vertex[vertex.id] = node_ids[subtask_nodes[subtask_index]]
    return PlacementPlan(node_by_vertex=node_by_vertex)
