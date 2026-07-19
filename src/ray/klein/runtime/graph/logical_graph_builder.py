# SPDX-License-Identifier: Apache-2.0

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec


class LogicalGraphBuilder:
    """Mutable builder for assembling / transforming a :class:`LogicalGraph`.

    Rules take a graph, derive a builder (``graph.to_builder()``), mutate it, and
    ``build()`` a fresh immutable graph — keeping transformations pure at the
    graph level while remaining ergonomic.
    """

    def __init__(self, job_name: str, config: Configuration) -> None:
        if not job_name:
            raise ValueError("logical graph job_name must not be empty")
        if not isinstance(config, Configuration):
            raise TypeError("logical graph config must be a Configuration")
        self._job_name = job_name
        self._config = config
        self._vertices: dict[VertexId, VertexSpec] = {}
        self._edges: list[EdgeSpec] = []

    def add_vertex(self, spec: VertexSpec) -> "LogicalGraphBuilder":
        existing = self._vertices.get(spec.id)
        if existing is not None and existing != spec:
            raise ValueError(f"logical vertex {spec.id} was defined more than once with different attributes")
        self._vertices[spec.id] = spec
        return self

    def replace_vertex(self, spec: VertexSpec) -> "LogicalGraphBuilder":
        if spec.id not in self._vertices:
            raise KeyError(f"cannot replace missing logical vertex {spec.id}")
        self._vertices[spec.id] = spec
        return self

    def vertex(self, vertex_id: VertexId) -> VertexSpec:
        return self._vertices[vertex_id]

    def add_edge(self, edge: EdgeSpec) -> "LogicalGraphBuilder":
        if edge.source not in self._vertices or edge.target not in self._vertices:
            raise KeyError(f"cannot add logical edge {edge.source} -> {edge.target}: one or both vertices do not exist")
        for existing in self._edges:
            if existing.source == edge.source and existing.target == edge.target:
                if existing == edge:
                    return self
                raise ValueError(f"logical edge {edge.source} -> {edge.target} has conflicting partitioners")
        self._edges.append(edge)
        return self

    def remove_edge(self, source: VertexId, target: VertexId) -> "LogicalGraphBuilder":
        if self.edge(source, target) is None:
            raise KeyError(f"cannot remove missing logical edge {source} -> {target}")
        self._edges = [edge for edge in self._edges if not (edge.source == source and edge.target == target)]
        return self

    def remove_vertex(self, vertex_id: VertexId) -> "LogicalGraphBuilder":
        if vertex_id not in self._vertices:
            raise KeyError(f"cannot remove missing logical vertex {vertex_id}")
        self._vertices.pop(vertex_id)
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
        return LogicalGraph(self._job_name, self._config, self._vertices, tuple(self._edges))
