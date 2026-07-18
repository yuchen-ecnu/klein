# SPDX-License-Identifier: Apache-2.0
"""Pure LogicalGraph optimization rules.

Each rule is a pure ``LogicalGraph -> LogicalGraph`` transform: it derives a
builder from the input graph, mutates the builder, and returns a fresh graph.
The input graph remains unchanged.
"""

from abc import ABC, abstractmethod
from dataclasses import replace

from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.partitioning.forward_partitioner import ForwardPartitioner


class LogicalRule(ABC):
    """A pure transform over a LogicalGraph."""

    @abstractmethod
    def apply(self, graph: LogicalGraph) -> LogicalGraph:
        """Return the transformed graph."""


class ChainingRule(LogicalRule):
    """Fuse a vertex with its single forward-connected downstream(s) into one
    ChainedOperator vertex, eliminating an inter-actor hop.

    The transform operates on the immutable logical IR.
    """

    def apply(self, graph: LogicalGraph) -> LogicalGraph:
        builder = graph.to_builder()
        # Work on a snapshot of source ids; chaining mutates the builder.
        for source in graph.sources:
            self._traverse(source, builder)
        return builder.build()

    def _traverse(
        self,
        root: VertexId,
        builder: LogicalGraphBuilder,
        visited: set[VertexId] | None = None,
    ) -> None:
        if visited is None:
            visited = set()
        if root in visited:
            return
        visited.add(root)
        downstream = builder.downstream(root)
        if not downstream:
            return
        for target in downstream:
            self._traverse(target, builder, visited)

        downstream = builder.downstream(root)  # re-read; may have changed
        if len(downstream) == 1:
            target = downstream[0]
            edge = builder.edge(root, target)
            if self._can_chain(builder, root, target, edge):
                self._chain(builder, root, [target])
            return

        # multiple downstreams: chainable iff all are leaves and all chainable
        for target in downstream:
            if builder.downstream(target):
                return
            edge = builder.edge(root, target)
            if not self._can_chain(builder, root, target, edge):
                return
        self._chain(builder, root, list(downstream))

    def _can_chain(
        self,
        builder: LogicalGraphBuilder,
        root: VertexId,
        target: VertexId,
        edge: EdgeSpec | None,
    ) -> bool:
        if edge is None or not isinstance(edge.partitioner, ForwardPartitioner):
            return False
        if len(builder.upstream(target)) > 1:
            return False
        root_vertex = builder.vertex(root)
        target_vertex = builder.vertex(target)
        if root_vertex.operator.stateful or target_vertex.operator.stateful:
            return False
        # The chain forwards records in-process via the synchronous
        # process_element only; an async operator is driven via
        # process_async_element, so chaining it would call its sync path and
        # ship an un-awaited coroutine downstream. Refuse to chain either side
        # when async is enabled.
        if root_vertex.operator.runtime_info.async_enabled or target_vertex.operator.runtime_info.async_enabled:
            return False
        return self._execution_contract_matches(root_vertex, target_vertex)

    @staticmethod
    def _execution_contract_matches(root: VertexSpec, target: VertexSpec) -> bool:
        resources_match = root.resources.cpus == target.resources.cpus and root.resources.gpus == target.resources.gpus
        runtime_match = (
            root.concurrency == target.concurrency
            and root.batch_size == target.batch_size
            and root.async_buffer_size == target.async_buffer_size
        )
        return resources_match and runtime_match

    def _chain(
        self,
        builder: LogicalGraphBuilder,
        root: VertexId,
        targets: list[VertexId],
    ) -> None:
        root_vertex = builder.vertex(root)
        target_vertices = [builder.vertex(target) for target in targets]
        new_name = f"{root_vertex.name} -> {', '.join(vertex.name for vertex in target_vertices)}"
        chained_spec = OperatorSpec.chain(
            root_vertex.operator,
            tuple(vertex.operator for vertex in target_vertices),
            new_name,
        )
        builder.replace_vertex(replace(root_vertex, operator=chained_spec, name=new_name))

        if len(targets) == 1:
            target = targets[0]
            # Rewire the target's downstreams onto root, then drop the target.
            for downstream in builder.downstream(target):
                output_edge = builder.edge(target, downstream)
                if output_edge is None:
                    raise RuntimeError(f"Missing logical edge {target} -> {downstream}")
                builder.remove_edge(target, downstream)
                builder.add_edge(EdgeSpec(root, downstream, output_edge.partitioner))
            builder.remove_edge(root, target)
            builder.remove_vertex(target)
        else:
            # All targets are leaves: just absorb them.
            for target in targets:
                builder.remove_edge(root, target)
                builder.remove_vertex(target)


class UnionRule(LogicalRule):
    """Remove UNION vertices, directly connecting their upstreams to their
    downstreams (the union is a no-op merge at runtime).

    Upstream and downstream edges are rewired through the graph builder.
    """

    def apply(self, graph: LogicalGraph) -> LogicalGraph:
        from ray.klein._internal.partitioning import default_partitioner
        from ray.klein.api.node_type import NodeType

        builder = graph.to_builder()
        union_ids = [vertex_id for vertex_id, vertex in graph.vertices.items() if vertex.node_type == NodeType.UNION]
        for union_id in union_ids:
            upstream_ids = builder.upstream(union_id)
            downstream_ids = builder.downstream(union_id)
            for upstream_id in upstream_ids:
                upstream = builder.vertex(upstream_id)
                for downstream_id in downstream_ids:
                    downstream = builder.vertex(downstream_id)
                    partitioner = default_partitioner(
                        upstream.concurrency,
                        downstream.concurrency,
                    )
                    builder.add_edge(EdgeSpec(upstream_id, downstream_id, partitioner))
            builder.remove_vertex(union_id)
        return builder.build()
