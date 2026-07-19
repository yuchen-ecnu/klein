# SPDX-License-Identifier: Apache-2.0
"""Immutable IR between the DataStream API and the physical ExecutionGraph.

Design invariants:

* Edges are stored once; inbound and outbound adjacency are cached views.
* One vertex attribute bag (:class:`VertexSpec`) carries everything (operator,
  parallelism, resource, batch/async config) so fields aren't re-declared and
  drifting across layers.
* Identity is first-class: every vertex is addressed by a :class:`VertexId`.
* Transformations are pure ``LogicalGraph -> LogicalGraph`` (build a new graph
  via :class:`LogicalGraphBuilder`); no in-place mutation, no hidden side
  effects.
* ``sources()`` is determined by operator type rather than graph degree.
"""

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import replace
from functools import cached_property
from heapq import heappop, heappush
from types import MappingProxyType
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger
from ray.klein._internal.partitioning import default_partitioner
from ray.klein.api.node_type import NodeType
from ray.klein.api.resource_edge import ResourceEdge
from ray.klein.api.resource_node import ResourceNode
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.api.stream import Stream
from ray.klein.api.stream_sink import StreamSink
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec
from ray.klein.runtime.resources import Resources

logger = get_logger(__name__)

if TYPE_CHECKING:
    from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder


class LogicalGraph:
    """Immutable DAG of :class:`VertexSpec` connected by :class:`EdgeSpec`.

    Construct via :class:`LogicalGraphBuilder`; transformations return new graphs.
    """

    def __init__(
        self,
        job_name: str,
        config: Configuration,
        vertices: Mapping[VertexId, VertexSpec],
        edges: Sequence[EdgeSpec],
    ) -> None:
        if not job_name:
            raise ValueError("LogicalGraph job_name must not be empty")
        if not isinstance(config, Configuration):
            raise TypeError("LogicalGraph config must be a Configuration")
        self.job_name = job_name
        self._config = deepcopy(config)
        self._vertices = dict(vertices)
        self._edges = tuple(edges)
        order = self._validate()
        self._vertices = {vertex_id: self._vertices[vertex_id] for vertex_id in order}
        rank = {vertex_id: index for index, vertex_id in enumerate(order)}
        self._edges = tuple(sorted(self._edges, key=lambda edge: (rank[edge.source], rank[edge.target])))

    @classmethod
    def from_sinks(
        cls,
        streams: Sequence[StreamSink],
        job_name: str,
        config: Configuration,
    ) -> "LogicalGraph":
        """Capture the DataStream API directly into the immutable logical IR."""

        from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder

        builder = LogicalGraphBuilder(job_name, config)
        visited: set[int] = set()

        def visit(stream: Stream) -> None:
            vertex_id = VertexId(job_name, stream.id)
            builder.add_vertex(
                VertexSpec(
                    id=vertex_id,
                    name=f"{stream.name}[{stream.id}]",
                    operator=stream.stream_operator.to_spec(),
                    node_type=stream.node_type,
                    resources=stream.resources,
                    ray_serve_enabled=stream.ray_serve_enabled,
                )
            )
            if stream.id in visited:
                return
            visited.add(stream.id)
            for parent in stream.input_streams:
                visit(parent)
                partitioner = parent.partitioner or default_partitioner(parent.concurrency, stream.concurrency)
                builder.add_edge(EdgeSpec(VertexId(job_name, parent.id), vertex_id, partitioner.to_spec()))

        for stream in streams:
            visit(stream)
        return builder.build()

    def _validate(self) -> tuple[VertexId, ...]:
        self._validate_vertices()
        in_degree, outgoing = self._validated_topology()
        roots = {vertex_id for vertex_id, degree in in_degree.items() if degree == 0}
        declared_sources = {vertex_id for vertex_id, vertex in self._vertices.items() if vertex.is_source}
        if self._vertices and roots != declared_sources:
            raise ValueError(
                f"logical graph roots must exactly match source operators; roots={roots}, sources={declared_sources}"
            )
        return self._topological_order(in_degree, outgoing)

    def _validate_vertices(self) -> None:
        for vertex_id, vertex in self._vertices.items():
            if vertex.id != vertex_id:
                raise ValueError(f"logical vertex key {vertex_id} does not match spec id {vertex.id}")
            if vertex_id.job != self.job_name:
                raise ValueError(f"logical vertex {vertex_id} belongs to a different job")
            if (vertex.node_type == NodeType.SOURCE) != vertex.operator.source:
                raise ValueError(f"logical vertex {vertex_id} has inconsistent node and operator source classification")

    def _validated_topology(self) -> tuple[dict[VertexId, int], dict[VertexId, list[VertexId]]]:
        pairs: set[tuple[VertexId, VertexId]] = set()
        in_degree = dict.fromkeys(self._vertices, 0)
        outgoing: dict[VertexId, list[VertexId]] = {vertex_id: [] for vertex_id in self._vertices}
        for edge in self._edges:
            if edge.source not in self._vertices or edge.target not in self._vertices:
                raise ValueError(f"logical edge {edge.source} -> {edge.target} has a missing endpoint")
            pair = (edge.source, edge.target)
            if pair in pairs:
                raise ValueError(f"duplicate logical edge {edge.source} -> {edge.target}")
            pairs.add(pair)
            in_degree[edge.target] += 1
            outgoing[edge.source].append(edge.target)
        return in_degree, outgoing

    @staticmethod
    def _topological_order(
        in_degree: Mapping[VertexId, int],
        outgoing: Mapping[VertexId, Sequence[VertexId]],
    ) -> tuple[VertexId, ...]:
        remaining = dict(in_degree)
        ready: list[tuple[int, VertexId]] = []
        for vertex_id in remaining:
            if remaining[vertex_id] == 0:
                heappush(ready, (vertex_id.index, vertex_id))
        ordered: list[VertexId] = []
        while ready:
            _, vertex_id = heappop(ready)
            ordered.append(vertex_id)
            for target in sorted(outgoing[vertex_id], key=lambda candidate: candidate.index):
                remaining[target] -= 1
                if remaining[target] == 0:
                    heappush(ready, (target.index, target))
        if len(ordered) != len(remaining):
            raise ValueError("LogicalGraph must be acyclic")
        return tuple(ordered)

    @property
    def vertices(self) -> Mapping[VertexId, VertexSpec]:
        return MappingProxyType(self._vertices)

    @property
    def config(self) -> Configuration:
        """Return a defensive configuration snapshot."""

        return deepcopy(self._config)

    @property
    def edges(self) -> tuple[EdgeSpec, ...]:
        return self._edges

    @cached_property
    def _out(self) -> dict[VertexId, list[EdgeSpec]]:
        adj: dict[VertexId, list[EdgeSpec]] = {vid: [] for vid in self._vertices}
        for edge in self._edges:
            adj[edge.source].append(edge)
        return adj

    @cached_property
    def _in(self) -> dict[VertexId, list[EdgeSpec]]:
        adj: dict[VertexId, list[EdgeSpec]] = {vid: [] for vid in self._vertices}
        for edge in self._edges:
            adj[edge.target].append(edge)
        return adj

    def get(self, vid: VertexId) -> VertexSpec:
        return self._vertices[vid]

    def out_edges(self, vertex_id: VertexId) -> tuple[EdgeSpec, ...]:
        if vertex_id not in self._vertices:
            raise KeyError(f"logical vertex {vertex_id} does not exist")
        return tuple(self._out[vertex_id])

    def in_edges(self, vertex_id: VertexId) -> tuple[EdgeSpec, ...]:
        if vertex_id not in self._vertices:
            raise KeyError(f"logical vertex {vertex_id} does not exist")
        return tuple(self._in[vertex_id])

    def downstream(self, vertex_id: VertexId) -> tuple[VertexId, ...]:
        return tuple(edge.target for edge in self.out_edges(vertex_id))

    def upstream(self, vertex_id: VertexId) -> tuple[VertexId, ...]:
        return tuple(edge.source for edge in self.in_edges(vertex_id))

    def edge(self, source: VertexId, target: VertexId) -> EdgeSpec | None:
        for edge in self.out_edges(source):
            if edge.target == target:
                return edge
        return None

    @property
    def sources(self) -> tuple[VertexId, ...]:
        """Vertices whose operator is a SOURCE.

        Deliberately by operator type, NOT in-degree: after the Union rule
        rewires the graph, a union-branch source still has out-edges only but is
        a genuine barrier-emitting source; in-degree=0 missed exactly these and
        produced the barrier_split KeyError.
        """
        return tuple(vertex_id for vertex_id, vertex in self._vertices.items() if vertex.is_source)

    @property
    def sinks(self) -> tuple[VertexId, ...]:
        return tuple(vertex_id for vertex_id in self._vertices if not self.out_edges(vertex_id))

    def partitioner_for(self, source: VertexId, target: VertexId) -> PartitionerSpec:
        edge = self.edge(source, target)
        if edge is None:
            raise KeyError(f"logical edge {source} -> {target} does not exist")
        return edge.partitioner

    @property
    def take_vertices(self) -> tuple[VertexId, ...]:
        """Vertices holding a CollectOperator (DataStream.take/show output)."""
        return tuple(vertex_id for vertex_id, vertex in self._vertices.items() if vertex.operator.collecting)

    def build_resource_plan(self) -> ResourcePlan:
        nodes = {}
        for vertex in self._vertices.values():
            resource_node = ResourceNode(
                vertex.id.index,
                vertex.operator.name,
                vertex.resources.num_cpus,
                vertex.resources.num_gpus,
                vertex.resources.concurrency,
                vertex.batch_size,
                vertex.async_buffer_size,
            )
            nodes[resource_node.key] = resource_node
        edges = [ResourceEdge(edge.source.index, edge.target.index, str(edge.partitioner)) for edge in self._edges]
        return ResourcePlan(nodes, edges)

    def with_resource_plan(self, plan: ResourcePlan) -> "LogicalGraph":
        if not self.build_resource_plan().is_compatible_with(plan):
            raise ValueError(f"Cannot apply resource plan {plan} to logical graph:\n{self}")
        builder = self.to_builder()
        for vertex in self._vertices.values():
            tuned = plan[f"{vertex.operator.name}[{vertex.id.index}]"]
            resources = Resources(tuned.num_cpus, tuned.num_gpus, tuned.concurrency)
            logical_function = vertex.operator.logical_function
            operator = vertex.operator
            if logical_function is not None:
                logical_function = logical_function.with_resources(resources).with_runtime_overrides(
                    batch_size=tuned.batch_size,
                    async_buffer_size=tuned.async_buffer_size,
                )
                operator = replace(operator, logical_function=logical_function)
            builder.replace_vertex(replace(vertex, resources=resources, operator=operator))
        return builder.build()

    @property
    def runtime_mode_requires_streaming(self) -> bool:
        for source_id in self.sources:
            source = self._vertices[source_id]
            if source.operator.operator_type is not OperatorType.SOURCE:
                raise TypeError(f"Logical source {source_id} does not contain a source operator")
            if not bool(source.operator.parameters.get("bounded", False)):
                return True
        sink_ids = set(self.sinks)
        return any(
            vertex.operator.logical_function is None or not vertex.operator.logical_function.batch_supported
            for vertex_id, vertex in self._vertices.items()
            if vertex_id in sink_ids
        )

    def to_builder(self) -> "LogicalGraphBuilder":
        from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder

        b = LogicalGraphBuilder(self.job_name, self.config)
        for vertex in self._vertices.values():
            b.add_vertex(vertex)
        for edge in self._edges:
            b.add_edge(edge)
        return b

    def __repr__(self) -> str:
        # Prefer the rich tree view; fall back to a plain edge list where rich is
        # unavailable (so logs / minimal environments still get readable output).
        try:
            from ray.klein.observability.progress_view import render_logical_graph

            rendered = render_logical_graph(self)
            if rendered:
                return "\n" + rendered.rstrip()
        except Exception:
            logger.debug("Rich LogicalGraph rendering failed", exc_info=True)
        lines = [f"LogicalGraph({self.job_name!r}, {len(self._vertices)} vertices, {len(self._edges)} edges)"]
        lines.extend(
            f"  {self._vertices[edge.source].name} --{edge.partitioner}--> {self._vertices[edge.target].name}"
            for edge in self._edges
        )
        return "\n".join(lines)
