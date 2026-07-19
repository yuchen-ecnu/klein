# SPDX-License-Identifier: Apache-2.0
from collections import deque
from collections.abc import Mapping, Sequence
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from ray.klein.runtime.execution_graph.execution_job_edge import ExecutionJobEdge
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import (
    ExecutionVertex,
    ExecutionVertexId,
)

if TYPE_CHECKING:
    from ray.klein.config.configuration import Configuration
    from ray.klein.observability.metrics.metric_group import JobMetricGroup
    from ray.klein.runtime.graph.logical_graph import LogicalGraph


class ExecutionGraph:
    """Immutable physical topology used to schedule Ray actors.

    Adjacency and degree views are computed lazily from the fixed topology.
    """

    def __init__(
        self,
        namespace: str,
        job_vertices: Mapping[int, ExecutionJobVertex] | None = None,
        job_edges: Sequence[ExecutionJobEdge] = (),
    ) -> None:
        if not namespace:
            raise ValueError("ExecutionGraph namespace must not be empty")
        self._namespace = namespace
        self._job_vertices = dict(job_vertices or {})
        self._job_edges = tuple(job_edges)
        self._validate_topology()
        _ = self._topological_job_vertices

    def _validate_topology(self) -> None:
        for vertex_id, vertex in self._job_vertices.items():
            if vertex.id != vertex_id:
                raise ValueError(f"execution vertex key {vertex_id} does not match vertex id {vertex.id}")
        pairs: set[tuple[int, int]] = set()
        for edge in self._job_edges:
            if edge.source not in self._job_vertices or edge.target not in self._job_vertices:
                raise ValueError(f"execution edge {edge.source} -> {edge.target} has a missing endpoint")
            pair = (edge.source, edge.target)
            if pair in pairs:
                raise ValueError(f"duplicate execution edge {edge.source} -> {edge.target}")
            pairs.add(pair)

        targets = {edge.target for edge in self._job_edges}
        roots = self._job_vertices.keys() - targets
        declared_sources = {
            vertex_id for vertex_id, vertex in self._job_vertices.items() if vertex.operator_spec.source
        }
        if self._job_vertices and roots != declared_sources:
            raise ValueError(
                f"ExecutionGraph roots must exactly match source operators; roots={roots}, sources={declared_sources}"
            )

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def job_vertices(self) -> Mapping[int, ExecutionJobVertex]:
        return MappingProxyType(self._job_vertices)

    @property
    def job_edges(self) -> tuple[ExecutionJobEdge, ...]:
        return self._job_edges

    # ------------------------------------------------------------------ #
    # Cached adjacency / degree indices (topology is fixed after expand). #
    # ------------------------------------------------------------------ #
    @cached_property
    def _out_edges_by_job_vertex(self) -> dict[int, list[ExecutionJobEdge]]:
        adjacency: dict[int, list[ExecutionJobEdge]] = {vertex_id: [] for vertex_id in self.job_vertices}
        for edge in self.job_edges:
            adjacency.setdefault(edge.source, []).append(edge)
        return adjacency

    @cached_property
    def _in_edges_by_job_vertex(self) -> dict[int, list[ExecutionJobEdge]]:
        adjacency: dict[int, list[ExecutionJobEdge]] = {vertex_id: [] for vertex_id in self.job_vertices}
        for edge in self.job_edges:
            adjacency.setdefault(edge.target, []).append(edge)
        return adjacency

    @cached_property
    def _job_vertex_degrees(self) -> tuple[dict[int, int], dict[int, int]]:
        in_degree = {vertex_id: len(self._in_edges_by_job_vertex[vertex_id]) for vertex_id in self.job_vertices}
        out_degree = {vertex_id: len(self._out_edges_by_job_vertex[vertex_id]) for vertex_id in self.job_vertices}
        return in_degree, out_degree

    @cached_property
    def barrier_splits(self) -> dict[ExecutionVertexId, dict[ExecutionVertexId, int]]:
        """Per-vertex barrier-alignment counts: ``{vertex_id: {source_vertex_id: count}}``.

        This is a pure function of the immutable post-expand topology and is
        shared by every deployment descriptor.
        """
        alignments: dict[ExecutionVertexId, dict[ExecutionVertexId, int]] = {}
        for source_job_vertex_id in self.source_job_vertices:
            self._accumulate_barrier_splits(source_job_vertex_id, alignments)
        return alignments

    def _accumulate_barrier_splits(
        self,
        source_job_vertex_id: int,
        alignments: dict[ExecutionVertexId, dict[ExecutionVertexId, int]],
    ) -> None:
        """BFS from one source, tallying per-vertex barrier alignment counts.

        Each source subtask seeds its own count of 1; downstream vertices sum the
        counts contributed along every execution edge from that source, so a
        vertex fed by a shuffle (RESCALE/HASH) aligns one barrier per upstream
        subtask, while a forward edge contributes exactly one.
        """
        source_job_vertex = self.job_vertices[source_job_vertex_id]
        for source_vertex in source_job_vertex.execution_vertices.values():
            alignments[source_vertex.id] = {source_vertex.id: 1}
        for current_id in self._topological_job_vertices:
            if current_id == source_job_vertex_id:
                continue
            self._propagate_source_alignment(source_job_vertex_id, current_id, alignments)

    def _propagate_source_alignment(
        self,
        source_job_vertex_id: int,
        current_job_vertex_id: int,
        alignments: dict[ExecutionVertexId, dict[ExecutionVertexId, int]],
    ) -> None:
        for input_edge in self.input_job_edges(current_job_vertex_id):
            for edge in input_edge.execution_edges:
                source_counts = alignments.get(edge.source.id, {})
                target_counts = alignments.setdefault(edge.target.id, {})
                for source_id in source_counts:
                    if source_id.job_vertex_id == source_job_vertex_id:
                        target_counts[source_id] = target_counts.get(source_id, 0) + 1

    @cached_property
    def _topological_job_vertices(self) -> tuple[int, ...]:
        in_degree, _ = self._job_vertex_degrees
        remaining = dict(in_degree)
        ready = deque(vertex_id for vertex_id, degree in remaining.items() if degree == 0)
        ordered: list[int] = []
        while ready:
            vertex_id = ready.popleft()
            ordered.append(vertex_id)
            for downstream in self.downstream_job_vertices(vertex_id):
                remaining[downstream] -= 1
                if remaining[downstream] == 0:
                    ready.append(downstream)
        if len(ordered) != len(self.job_vertices):
            raise ValueError("ExecutionGraph must be acyclic")
        return tuple(ordered)

    @cached_property
    def affinity_groups(self) -> tuple[tuple[ExecutionVertexId, ...], ...]:
        """Co-location groups: connected components over FORWARD edges only.

        A FORWARD edge is 1:1 same-parallelism (``ev_i -> ev_i``, no shuffle), so
        co-locating its endpoints makes the hop a local serialization-free handoff;
        shuffle edges are all-to-all and intentionally cross-node, so they are not
        co-location signals. Each component is one affinity group = one placement
        unit (a no-forward-neighbour subtask is its own singleton).
        """
        adjacency = self._forward_adjacency()
        return tuple(tuple(group) for group in self._connected_components(adjacency))

    def _forward_adjacency(self) -> dict[ExecutionVertexId, list[ExecutionVertexId]]:
        from ray.klein.runtime.partitioning.forward_partitioner import ForwardPartitioner

        adjacency: dict[ExecutionVertexId, list[ExecutionVertexId]] = {
            vertex.id: [] for vertex in self.execution_vertices
        }
        for edge in self.job_edges:
            if edge.partitioner.is_type(ForwardPartitioner):
                for exec_edge in edge.execution_edges:
                    src_id, dst_id = exec_edge.source.id, exec_edge.target.id
                    adjacency[src_id].append(dst_id)
                    adjacency[dst_id].append(src_id)
        return adjacency

    @staticmethod
    def _connected_components(
        adjacency: dict[ExecutionVertexId, list[ExecutionVertexId]],
    ) -> list[list[ExecutionVertexId]]:
        groups: list[list[ExecutionVertexId]] = []
        seen: set[ExecutionVertexId] = set()
        for vertex_id in adjacency:
            if vertex_id in seen:
                continue
            component: list[ExecutionVertexId] = []
            stack = [vertex_id]
            while stack:
                node = stack.pop()
                if node in seen:
                    continue
                seen.add(node)
                component.append(node)
                stack.extend(neighbor for neighbor in adjacency[node] if neighbor not in seen)
            groups.append(component)
        return groups

    @staticmethod
    def expand(
        logical_graph: "LogicalGraph",
        job_config: "Configuration",
        job_metric_group: "JobMetricGroup",
        namespace: str,
    ) -> "ExecutionGraph":
        """Expand a logical graph into parallel physical subtasks.

        ``VertexId.index`` is the stable integer key for each job vertex.
        """
        job_vertices = {}
        job_edges = []
        for vertex_id, spec in logical_graph.vertices.items():
            job_vertices[vertex_id.index] = ExecutionJobVertex(spec, job_config, job_metric_group)
        for edge in logical_graph.edges:
            source_job_vertex = job_vertices[edge.source.index]
            target_job_vertex = job_vertices[edge.target.index]
            job_edges.append(ExecutionJobEdge(source_job_vertex, target_job_vertex, edge.partitioner))

        return ExecutionGraph(namespace, job_vertices, job_edges)

    @property
    def execution_vertices(self) -> tuple[ExecutionVertex, ...]:
        return tuple(
            vertex for job_vertex in self.job_vertices.values() for vertex in job_vertex.execution_vertices.values()
        )

    def execution_vertex(self, vertex_id: ExecutionVertexId) -> ExecutionVertex:
        return self.job_vertex(vertex_id.job_vertex_id).execution_vertex(vertex_id.index)

    def find_execution_vertex(self, vertex_id: ExecutionVertexId) -> ExecutionVertex | None:
        job_vertex = self._job_vertices.get(vertex_id.job_vertex_id)
        return None if job_vertex is None else job_vertex.execution_vertices.get(vertex_id.index)

    @property
    def sink_execution_vertices(self) -> tuple[ExecutionVertex, ...]:
        # A sink job vertex (out-degree 0) contributes all its subtasks as sinks.
        return tuple(
            vertex
            for job_vertex_id in self.sink_job_vertices
            for vertex in self.job_vertices[job_vertex_id].execution_vertices.values()
        )

    @property
    def source_execution_vertices(self) -> tuple[ExecutionVertex, ...]:
        """Return every physical subtask belonging to a source vertex."""

        return tuple(
            vertex
            for job_vertex_id in self.source_job_vertices
            for vertex in self.job_vertices[job_vertex_id].execution_vertices.values()
        )

    @property
    def source_job_vertices(self) -> tuple[int, ...]:
        return tuple(vertex_id for vertex_id, vertex in self.job_vertices.items() if vertex.operator_spec.source)

    @property
    def sink_job_vertices(self) -> tuple[int, ...]:
        _, out_degree = self._job_vertex_degrees
        return tuple(vertex_id for vertex_id in self.job_vertices if out_degree[vertex_id] == 0)

    def downstream_job_vertices(self, exec_job_vertex_id: int) -> tuple[int, ...]:
        self.job_vertex(exec_job_vertex_id)
        return tuple(edge.target for edge in self._out_edges_by_job_vertex[exec_job_vertex_id])

    def job_vertex(self, job_vertex_id: int) -> ExecutionJobVertex:
        return self._job_vertices[job_vertex_id]

    def find_job_vertex(self, job_vertex_id: int) -> ExecutionJobVertex | None:
        return self._job_vertices.get(job_vertex_id)

    def output_job_edges(self, exec_job_vertex_id: int) -> tuple[ExecutionJobEdge, ...]:
        """Outbound ExecutionJobEdges of the given ExecutionJobVertex."""
        self.job_vertex(exec_job_vertex_id)
        return tuple(self._out_edges_by_job_vertex[exec_job_vertex_id])

    def input_job_edges(self, exec_job_vertex_id: int) -> tuple[ExecutionJobEdge, ...]:
        """Inbound ExecutionJobEdges of the given ExecutionJobVertex."""
        self.job_vertex(exec_job_vertex_id)
        return tuple(self._in_edges_by_job_vertex[exec_job_vertex_id])

    def __repr__(self) -> str:
        # Plain edge-list repr for debugging. The ExecutionGraph is the
        # per-subtask physical plan and is intentionally not pretty-printed in
        # the job logs (the LogicalGraph already shows the operator
        # plan); this keeps a readable repr() for ad-hoc inspection.
        lines = [f"ExecutionGraph({len(self.job_vertices)} vertices, {len(self.job_edges)} edges)"]
        for edge in self.job_edges:
            src = self.job_vertices[edge.source].name
            dst = self.job_vertices[edge.target].name
            lines.append(f"  {src} --{edge.partitioner}--> {dst}")
        return "\n".join(lines)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        # Drop memoized cached_property values; they rebuild lazily on access and
        # needn't ride the pickle (and the degree tuple isn't worth shipping).
        for key in (
            "_out_edges_by_job_vertex",
            "_in_edges_by_job_vertex",
            "_job_vertex_degrees",
            "_topological_job_vertices",
            "barrier_splits",
            "affinity_groups",
        ):
            state.pop(key, None)
        return state
