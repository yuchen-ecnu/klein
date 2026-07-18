# SPDX-License-Identifier: Apache-2.0
"""Immutable IR between the DataStream API and the physical ExecutionGraph.

Design invariants:

* Edges are stored once; inbound and outbound adjacency are cached views.
* One vertex attribute bag (:class:`VertexSpec`) carries everything (operator,
  parallelism, resource, batch/async config) so fields aren't re-declared and
  drifting across layers.
* Identity is first-class (:class:`VertexId` / :class:`SubtaskId`), with actor
  names defined by ``SubtaskId.actor_name``.
* Transformations are pure ``LogicalGraph -> LogicalGraph`` (build a new graph
  via :class:`LogicalGraphBuilder`); no in-place mutation, no hidden side
  effects.
* ``sources()`` is determined by operator type rather than graph degree.
"""

from collections.abc import Mapping, Sequence
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec

logger = get_logger(__name__)

if TYPE_CHECKING:
    from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder


class LogicalGraph:
    """Immutable DAG of :class:`VertexSpec` connected by :class:`EdgeSpec`.

    Construct via :class:`LogicalGraphBuilder`; transformations return new graphs.
    """

    def __init__(
        self,
        vertices: Mapping[VertexId, VertexSpec],
        edges: Sequence[EdgeSpec],
    ) -> None:
        self._vertices = dict(vertices)
        self._edges = tuple(edges)

    @property
    def vertices(self) -> Mapping[VertexId, VertexSpec]:
        return MappingProxyType(self._vertices)

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
        return tuple(self._out.get(vertex_id, ()))

    def in_edges(self, vertex_id: VertexId) -> tuple[EdgeSpec, ...]:
        return tuple(self._in.get(vertex_id, ()))

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

    @property
    def take_vertices(self) -> tuple[VertexId, ...]:
        """Vertices holding a CollectOperator (DataStream.take/show output)."""
        return tuple(vertex_id for vertex_id, vertex in self._vertices.items() if vertex.operator.collecting)

    @staticmethod
    def from_stream_graph(stream_graph) -> "LogicalGraph":
        """Build a LogicalGraph from the API-level StreamGraph.

        StreamGraph still owns Stream collection + serve rewrite + batch
        (extracted in G5); here we just lift its topology into the immutable IR
        so optimization runs on LogicalGraph.
        """
        from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder

        b = LogicalGraphBuilder()
        for node in stream_graph.nodes.values():
            b.add_vertex(
                VertexSpec(
                    id=VertexId(stream_graph.job_name, node.id),
                    name=node.name,
                    operator=node.operator.to_spec(),
                    node_type=node.node_type,
                    resources=node.resources,
                    ray_serve_enabled=node.ray_serve_enabled,
                )
            )
        for (source, target), partitioner in stream_graph.edges.items():
            b.add_edge(
                EdgeSpec(
                    VertexId(stream_graph.job_name, source),
                    VertexId(stream_graph.job_name, target),
                    partitioner,
                )
            )
        return b.build()

    def to_builder(self) -> "LogicalGraphBuilder":
        from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder

        b = LogicalGraphBuilder()
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
        lines = [f"LogicalGraph({len(self._vertices)} vertices, {len(self._edges)} edges)"]
        lines.extend(
            f"  {self._vertices[edge.source].name} --{edge.partitioner}--> {self._vertices[edge.target].name}"
            for edge in self._edges
        )
        return "\n".join(lines)
