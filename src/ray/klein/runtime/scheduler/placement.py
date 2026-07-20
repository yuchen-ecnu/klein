# SPDX-License-Identifier: Apache-2.0
"""Placement strategies + plans for scheduling StreamTask actors.

A ``PlacementStrategy`` decides *where* a job's subtasks run; a ``PlacementPlan``
is its resolved output, consumed by ``ExecutionJobVertex.instantiate`` (which
doesn't know how the plan was computed). The cascade in JobMaster tries
strategies in order, each raising ``PlacementError`` when infeasible so the next
is tried:

* **PlacementGroupStrategy** (default): reserves one independently releasable
  single-bundle PlacementGroup per physical actor. This lets local scale-out
  reserve only added actors and scale-in return only retired reservations.
* **RoundRobinStrategy** (fallback): a per-vertex node pin
  applied via ``NodeAffinitySchedulingStrategy``.
* **NativeStrategy** (final fallback): an empty plan; Ray schedules freely.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.scheduler.errors import PlacementCleanupError, PlacementError

logger = get_logger(__name__)

if TYPE_CHECKING:
    from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
    from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
    from ray.klein.runtime.scheduler.assignment import WorkerNode


@dataclass(slots=True)
class PlacementPlan:
    """Where each execution vertex's actor should be scheduled.

    Empty (all fields default) means "Ray native placement" — no constraint. At
    most one of the node-pin / placement-group shapes is populated. ``on_rollback``
    releases any reserved resource (e.g. the PG) if instantiation fails midway.
    """

    node_by_vertex: dict[ExecutionVertexId, str] = field(default_factory=dict)
    placement_group: object | None = None
    placement_group_by_vertex: dict[ExecutionVertexId, object] = field(default_factory=dict)
    bundle_by_vertex: dict[ExecutionVertexId, int] = field(default_factory=dict)
    on_rollback: Callable[[], None] | None = None
    strategy: "PlacementStrategy | None" = field(default=None, repr=False)
    _remove_group: Callable[[object], None] | None = field(default=None, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def node_for(self, vertex_id: ExecutionVertexId) -> str | None:
        return self.node_by_vertex.get(vertex_id)

    def bundle_for(self, vertex_id: ExecutionVertexId) -> int:
        return self.bundle_by_vertex.get(vertex_id, -1)

    def placement_group_for(self, vertex_id: ExecutionVertexId) -> object | None:
        """Return the exact reservation owned by ``vertex_id``.

        ``placement_group`` is retained as a compatibility fallback for plans
        created by third-party strategies. Klein's built-in placement-group
        strategy uses one independently removable group per actor so a local
        scale-in can release resources without killing retained actors.
        """

        return self.placement_group_by_vertex.get(vertex_id, self.placement_group)

    @property
    def uses_placement_group(self) -> bool:
        return self.placement_group is not None or bool(self.placement_group_by_vertex)

    def rollback(self) -> None:
        self.close()

    def reconcile(self) -> None:
        self.close()

    def close(self) -> None:
        """Release every reservation owned by this plan.

        Ownership is removed only after the corresponding external release
        succeeds. A failed Ray removal therefore leaves an exact handle for a
        later reconciliation attempt instead of silently leaking resources.
        """

        if self._closed:
            return
        groups = _unique_objects(
            (*self.placement_group_by_vertex.values(), self.placement_group)
            if self.placement_group is not None
            else tuple(self.placement_group_by_vertex.values())
        )
        cleanup_errors: list[Exception] = []
        if self._remove_group is not None:
            for group in groups:
                try:
                    self._remove_group(group)
                except Exception as error:
                    # Ray removals are independent. Continue so one unavailable
                    # group cannot hide/leak every reservation created after it;
                    # failed handles deliberately remain owned for reconciliation.
                    cleanup_errors.append(error)
                    continue
                released_ids = tuple(
                    vertex_id
                    for vertex_id, owned_group in self.placement_group_by_vertex.items()
                    if owned_group is group
                )
                for vertex_id in released_ids:
                    self.placement_group_by_vertex.pop(vertex_id, None)
                    self.bundle_by_vertex.pop(vertex_id, None)
                if self.placement_group is group:
                    self.placement_group = None
        elif self.on_rollback is not None:
            # Compatibility for externally constructed plans that still own
            # their lifecycle through one aggregate callback.
            self.on_rollback()
            self.on_rollback = None
        if cleanup_errors:
            summary = "; ".join(f"{type(error).__name__}: {error}" for error in cleanup_errors)
            raise RuntimeError(f"placement reservation cleanup was incomplete: {summary}") from cleanup_errors[0]
        self.node_by_vertex.clear()
        self.placement_group_by_vertex.clear()
        self.bundle_by_vertex.clear()
        self.placement_group = None
        self.on_rollback = None
        self._closed = True

    def begin_rescale(
        self,
        execution_graph: "ExecutionGraph",
        *,
        added: Iterable["ExecutionVertex"],
        removed: Iterable["ExecutionVertex"],
        timeout: float | None = None,
    ) -> "PlacementTransition":
        """Reserve only a physical rescale delta before its data-plane cut.

        The returned candidate plan is used to construct added actors. Rollback
        releases only those new reservations; commit adopts them into this
        job-owned plan. Retired reservations are deliberately released by a
        separate call, after the corresponding actors have stopped.
        """

        if self._closed:
            raise RuntimeError("cannot rescale a closed placement plan")
        if self.strategy is None:
            raise PlacementError("placement", "the active placement strategy cannot be rescaled")
        added_vertices = tuple(added)
        removed_ids = tuple(vertex.id for vertex in removed)
        candidate = self.strategy.plan_rescale(
            execution_graph,
            added_vertices,
            owner=self,
            timeout=timeout,
        )
        return PlacementTransition(self, candidate, removed_ids)

    def _adopt(self, candidate: "PlacementPlan") -> None:
        if candidate._closed:
            raise RuntimeError("cannot adopt a closed placement candidate")
        self.node_by_vertex.update(candidate.node_by_vertex)
        self.placement_group_by_vertex.update(candidate.placement_group_by_vertex)
        self.bundle_by_vertex.update(candidate.bundle_by_vertex)
        candidate.node_by_vertex.clear()
        candidate.placement_group_by_vertex.clear()
        candidate.bundle_by_vertex.clear()
        candidate.placement_group = None
        candidate.on_rollback = None
        candidate._closed = True

    def _release_vertices(self, vertex_ids: Iterable[ExecutionVertexId]) -> None:
        selected = tuple(vertex_ids)
        if self.placement_group is not None and selected:
            raise PlacementError(
                "placement",
                "an aggregate placement group cannot release only retired actors",
            )
        groups = _unique_objects(
            tuple(
                group
                for vertex_id in selected
                if (group := self.placement_group_by_vertex.get(vertex_id)) is not None
            )
        )
        if (groups or (self.placement_group is not None and selected)) and self._remove_group is None:
            raise PlacementError(
                "placement",
                "the active placement group is not independently releasable",
            )
        cleanup_errors: list[Exception] = []
        for group in groups:
            owners = {
                vertex_id
                for vertex_id, owned_group in self.placement_group_by_vertex.items()
                if owned_group is group
            }
            if owners - set(selected):
                raise PlacementError(
                    "placement",
                    "one placement group is shared by retained and retired actors",
                )
            try:
                self._remove_group(group)
            except Exception as error:
                cleanup_errors.append(error)
                continue
            for vertex_id in owners:
                self.placement_group_by_vertex.pop(vertex_id, None)
                self.bundle_by_vertex.pop(vertex_id, None)
        if cleanup_errors:
            summary = "; ".join(f"{type(error).__name__}: {error}" for error in cleanup_errors)
            raise RuntimeError(f"retired placement cleanup was incomplete: {summary}") from cleanup_errors[0]
        for vertex_id in selected:
            self.node_by_vertex.pop(vertex_id, None)
            self.bundle_by_vertex.pop(vertex_id, None)


@dataclass(slots=True)
class PlacementTransition:
    """Two-phase reservation update for one operator rescale."""

    owner: PlacementPlan
    candidate_plan: PlacementPlan
    removed_vertex_ids: tuple[ExecutionVertexId, ...]
    _committed: bool = field(default=False, init=False, repr=False)
    _retired_released: bool = field(default=False, init=False, repr=False)

    def commit(self) -> None:
        """Adopt scale-out reservations after the topology commit."""

        if self._committed:
            return
        self.owner._adopt(self.candidate_plan)
        self._committed = True

    def release_retired(self) -> None:
        """Release scale-in reservations after retired actors have stopped."""

        if not self._committed:
            raise RuntimeError("placement transition must be committed before releasing retired resources")
        if self._retired_released:
            return
        self.owner._release_vertices(self.removed_vertex_ids)
        self._retired_released = True

    def rollback(self) -> None:
        """Release an uncommitted scale-out reservation."""

        if self._committed:
            raise RuntimeError("a committed placement transition cannot be rolled back")
        self.candidate_plan.rollback()

    def reconcile(self) -> None:
        """Retry whichever reservation cleanup remains for this transition."""

        if self._committed:
            self.release_retired()
        else:
            self.rollback()


class PlacementStrategy(ABC):
    """Computes a PlacementPlan for an ExecutionGraph (raises PlacementError)."""

    name: str = "placement"

    @abstractmethod
    def plan(self, execution_graph: "ExecutionGraph") -> PlacementPlan:
        """Return a PlacementPlan, or raise ``PlacementError`` if infeasible."""

    def plan_vertices(
        self,
        execution_graph: "ExecutionGraph",
        vertices: Iterable["ExecutionVertex"],
        *,
        timeout: float | None = None,
    ) -> PlacementPlan:
        """Return placement for an added actor subset during local rescale."""

        del timeout
        requested = {vertex.id for vertex in vertices}
        plan = self.plan(execution_graph)
        if plan.uses_placement_group:
            plan.close()
            raise PlacementError(
                self.name,
                "placement-group strategies must implement delta reservation explicitly",
            )
        plan.node_by_vertex = {
            vertex_id: node_id for vertex_id, node_id in plan.node_by_vertex.items() if vertex_id in requested
        }
        plan.placement_group_by_vertex = {
            vertex_id: group
            for vertex_id, group in plan.placement_group_by_vertex.items()
            if vertex_id in requested
        }
        plan.bundle_by_vertex = {
            vertex_id: bundle for vertex_id, bundle in plan.bundle_by_vertex.items() if vertex_id in requested
        }
        return plan

    def plan_rescale(
        self,
        execution_graph: "ExecutionGraph",
        vertices: Iterable["ExecutionVertex"],
        *,
        owner: PlacementPlan,
        timeout: float | None = None,
    ) -> PlacementPlan:
        """Plan a physical delta while retaining the active plan's actors."""

        del owner
        return self.plan_vertices(execution_graph, vertices, timeout=timeout)


class NativeStrategy(PlacementStrategy):
    """Ray-native placement: no constraints, always feasible."""

    name = "native"

    def plan(self, _execution_graph: "ExecutionGraph") -> PlacementPlan:
        return PlacementPlan(strategy=self)


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
        plan = _node_pin_plan(execution_graph, assignments, node_ids)
        plan.strategy = self
        return plan

    def plan_rescale(
        self,
        execution_graph: "ExecutionGraph",
        vertices: Iterable["ExecutionVertex"],
        *,
        owner: PlacementPlan,
        timeout: float | None = None,
    ) -> PlacementPlan:
        """Pin additions after subtracting retained actors at their live pins."""

        del timeout
        from ray.klein.runtime.scheduler.assignment import cluster_worker_nodes

        selected = tuple(vertices)
        if not selected:
            return PlacementPlan(strategy=self)
        nodes, node_ids = cluster_worker_nodes()
        requested_ids = {vertex.id for vertex in selected}
        _reserve_retained_round_robin_nodes(
            execution_graph,
            requested_ids,
            owner,
            nodes,
            node_ids,
        )
        node_by_vertex = _allocate_round_robin_delta(selected, nodes, node_ids)
        return PlacementPlan(node_by_vertex=node_by_vertex, strategy=self)


class PlacementGroupStrategy(PlacementStrategy):
    """Elastic placement-group reservation, isolated per physical actor.

    Ray placement-group bundles are immutable and removing a group terminates
    every actor scheduled in it. A single job-wide group therefore cannot both
    preserve overlapping actors and release scale-in reservations. Klein uses
    one single-bundle group per actor instead: added groups can be reserved
    before the rescale barrier and removed groups can be released after their
    actors stop, without moving any retained actor.

    This intentionally trades job-wide gang scheduling and FORWARD-affinity
    co-location for safe local elasticity. All groups are still reserved before
    actor construction, and a partial reservation is rolled back as one unit.
    """

    name = "placement-group"

    def __init__(self, strategy: str, ready_timeout: float) -> None:
        if strategy not in {"PACK", "SPREAD"}:
            raise ValueError(
                "elastic actor-scoped placement groups support only PACK or SPREAD; "
                "STRICT_* cross-actor semantics cannot be preserved"
            )
        self._placement_group_strategy = strategy
        self._ready_timeout = ready_timeout

    def plan(self, execution_graph: "ExecutionGraph") -> PlacementPlan:
        return self.plan_vertices(execution_graph, execution_graph.execution_vertices)

    def plan_vertices(
        self,
        _execution_graph: "ExecutionGraph",
        vertices: Iterable["ExecutionVertex"],
        *,
        timeout: float | None = None,
    ) -> PlacementPlan:
        from ray.util.placement_group import placement_group, remove_placement_group

        import ray.klein as klein

        selected = tuple(vertices)
        if not selected:
            return PlacementPlan(strategy=self, _remove_group=remove_placement_group)

        placement_group_by_vertex: dict[ExecutionVertexId, object] = {}
        bundle_by_vertex: dict[ExecutionVertexId, int] = {}
        groups: list[object] = []
        try:
            for vertex in selected:
                bundle = {"CPU": vertex.resources.cpus}
                if vertex.resources.gpus:
                    bundle["GPU"] = vertex.resources.gpus
                group = placement_group([bundle], strategy=self._placement_group_strategy)
                groups.append(group)
                placement_group_by_vertex[vertex.id] = group
                bundle_by_vertex[vertex.id] = 0
            # One shared timeout bounds the whole reservation, rather than one
            # timeout per physical actor.
            ready_timeout = self._ready_timeout if timeout is None else min(self._ready_timeout, timeout)
            if ready_timeout <= 0:
                raise TimeoutError("operator rescale placement deadline expired")
            klein.get([group.ready() for group in groups], timeout=ready_timeout)
        except Exception as error:
            partial_plan = PlacementPlan(
                placement_group_by_vertex=placement_group_by_vertex,
                bundle_by_vertex=bundle_by_vertex,
                strategy=self,
                _remove_group=remove_placement_group,
            )
            try:
                partial_plan.close()
            except Exception as cleanup_error:
                raise PlacementCleanupError(
                    self.name,
                    error,
                    cleanup_error,
                    partial_plan,
                ) from error
            raise PlacementError(
                self.name,
                f"elastic groups not ready within the placement deadline: {error}",
            ) from error
        logger.info(
            "Reserved %d elastic placement groups with strategy %s",
            len(groups),
            self._placement_group_strategy,
        )
        return PlacementPlan(
            placement_group_by_vertex=placement_group_by_vertex,
            bundle_by_vertex=bundle_by_vertex,
            strategy=self,
            _remove_group=remove_placement_group,
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


def _reserve_retained_round_robin_nodes(
    execution_graph: "ExecutionGraph",
    requested_ids: set[ExecutionVertexId],
    owner: PlacementPlan,
    nodes: list["WorkerNode"],
    node_ids: list[str],
) -> None:
    """Subtract hard-pinned retained actors from current node capacity."""

    node_by_id = dict(zip(node_ids, nodes, strict=True))
    # A RoundRobin plan uses hard node affinity, so the owner mapping is the
    # running actor's actual allowed node. Recomputing the whole graph could
    # otherwise pretend retained actors moved and overfill their real node.
    for vertex in execution_graph.execution_vertices:
        if vertex.id in requested_ids:
            continue
        node_id = owner.node_for(vertex.id)
        node = None if node_id is None else node_by_id.get(node_id)
        if node is None:
            raise PlacementError(
                "round-robin",
                f"retained actor {vertex.id} has no live worker-node assignment",
            )
        if node.cpu < vertex.resources.cpus or node.gpu < vertex.resources.gpus:
            raise PlacementError(
                "round-robin",
                f"retained actors exceed resources on node {node_id}",
            )
        node.cpu -= vertex.resources.cpus
        node.gpu -= vertex.resources.gpus
        node.assigned_tasks += 1


def _allocate_round_robin_delta(
    selected: tuple["ExecutionVertex", ...],
    nodes: list["WorkerNode"],
    node_ids: list[str],
) -> dict[ExecutionVertexId, str]:
    """Allocate only added actors against capacity left by retained actors."""

    available_nodes = sorted(
        nodes,
        key=lambda node: (node.gpu, node.cpu, -node.assigned_tasks, -node.index),
        reverse=True,
    )
    if not available_nodes:
        raise PlacementError("round-robin", "no live worker nodes are available")
    node_id_by_index = dict(zip((node.index for node in nodes), node_ids, strict=True))
    node_cursor = 0
    node_by_vertex: dict[ExecutionVertexId, str] = {}
    for vertex in sorted(
        selected,
        key=lambda item: (item.resources.gpus, item.resources.cpus, -item.index),
        reverse=True,
    ):
        assigned = False
        for _ in range(len(available_nodes)):
            node = available_nodes[node_cursor]
            node_cursor = (node_cursor + 1) % len(available_nodes)
            if node.cpu < vertex.resources.cpus or node.gpu < vertex.resources.gpus:
                continue
            node.cpu -= vertex.resources.cpus
            node.gpu -= vertex.resources.gpus
            node.assigned_tasks += 1
            node_by_vertex[vertex.id] = node_id_by_index[node.index]
            assigned = True
            break
        if not assigned:
            raise PlacementError(
                "round-robin",
                "nodes have no enough resources to assign the rescale delta",
            )
    return node_by_vertex


def _unique_objects(values: Iterable[object]) -> tuple[object, ...]:
    """Identity-based de-duplication for Ray handles (whose equality is opaque)."""

    unique: list[object] = []
    seen: set[int] = set()
    for value in values:
        identity = id(value)
        if identity not in seen:
            seen.add(identity)
            unique.append(value)
    return tuple(unique)
