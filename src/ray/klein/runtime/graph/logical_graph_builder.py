# SPDX-License-Identifier: Apache-2.0

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec

logger = get_logger(__name__)


class LogicalGraphBuilder:
    """Mutable builder for assembling / transforming a :class:`LogicalGraph`.

    Rules take a graph, derive a builder (``graph.to_builder()``), mutate it, and
    ``build()`` a fresh immutable graph — keeping transformations pure at the
    graph level while remaining ergonomic.
    """

    def __init__(self) -> None:
        self._vertices: dict[VertexId, VertexSpec] = {}
        self._edges: list[EdgeSpec] = []

    def add_vertex(self, spec: VertexSpec) -> "LogicalGraphBuilder":
        self._vertices[spec.id] = spec
        return self

    def replace_vertex(self, spec: VertexSpec) -> "LogicalGraphBuilder":
        self._vertices[spec.id] = spec
        return self

    def vertex(self, vertex_id: VertexId) -> VertexSpec:
        return self._vertices[vertex_id]

    def add_edge(self, edge: EdgeSpec) -> "LogicalGraphBuilder":
        if edge.source not in self._vertices or edge.target not in self._vertices:
            logger.warning(
                "Skipping edge %s -> %s: one or both vertices do not exist",
                edge.source,
                edge.target,
            )
            return self
        for existing in self._edges:
            if existing.source == edge.source and existing.target == edge.target:
                logger.debug(
                    "Skipping duplicate edge %s -> %s",
                    edge.source,
                    edge.target,
                )
                return self  # dedupe
        self._edges.append(edge)
        return self

    def remove_edge(self, source: VertexId, target: VertexId) -> "LogicalGraphBuilder":
        self._edges = [edge for edge in self._edges if not (edge.source == source and edge.target == target)]
        return self

    def remove_vertex(self, vertex_id: VertexId) -> "LogicalGraphBuilder":
        self._vertices.pop(vertex_id, None)
        self._edges = [edge for edge in self._edges if vertex_id not in {edge.source, edge.target}]
        return self

    def downstream(self, vertex_id: VertexId) -> list[VertexId]:
        return [edge.target for edge in self._edges if edge.source == vertex_id]

    def upstream(self, vertex_id: VertexId) -> list[VertexId]:
        return [edge.source for edge in self._edges if edge.target == vertex_id]

    def edge(self, source: VertexId, target: VertexId) -> EdgeSpec | None:
        for edge in self._edges:
            if edge.source == source and edge.target == target:
                return edge
        return None

    def build(self) -> LogicalGraph:
        return LogicalGraph(self._vertices, tuple(self._edges))
