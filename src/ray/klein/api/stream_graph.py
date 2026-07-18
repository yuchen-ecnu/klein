# SPDX-License-Identifier: Apache-2.0
from collections.abc import Mapping, Sequence
from types import MappingProxyType

from ray.klein._internal.logging import get_logger
from ray.klein._internal.partitioning import default_partitioner
from ray.klein.api.resource_edge import ResourceEdge
from ray.klein.api.resource_node import ResourceNode
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.api.stream import Stream
from ray.klein.api.stream_node import StreamNode
from ray.klein.api.stream_sink import StreamSink
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.resources import Resources

logger = get_logger(__name__)


class StreamGraph:
    """Mutable operator graph assembled from terminal stream sinks."""

    def __init__(self, job_name: str, config: Configuration | None = None) -> None:
        self.nodes: dict[int, StreamNode] = {}
        # One edge map is the source of truth; adjacency is derived on demand.
        self._edges: dict[tuple[int, int], Partitioner] = {}
        self.job_name: str = job_name
        self.config: Configuration = config if config is not None else Configuration()

    @property
    def edges(self) -> Mapping[tuple[int, int], Partitioner]:
        """Read-only view of the graph's edge-to-partitioner mapping."""
        return MappingProxyType(self._edges)

    def partitioner_for(self, src_node: int, dst_node: int) -> Partitioner:
        return self._edges[src_node, dst_node]

    @classmethod
    def from_sinks(cls, streams: Sequence[StreamSink], job_name: str, config: Configuration) -> "StreamGraph":
        graph = cls(job_name, config)
        for stream in streams:
            graph._process_stream(stream)
        return graph

    def apply_resource_plan(self, plan: ResourcePlan) -> "StreamGraph":
        if not self.build_resource_plan().is_compatible_with(plan):
            raise ValueError(f"Cannot apply resource plan {plan} to stream graph:\n{self}")
        for node in self.nodes.values():
            tuned = plan[node.resource_plan_node.key]
            node.resources = Resources(tuned.num_cpus, tuned.num_gpus, tuned.concurrency)
            logical_function = node.operator.logical_function
            if logical_function is not None:
                logical_function.apply_resources(node.resources)
            self._reconcile_batch_override(node, tuned)
        return self

    @staticmethod
    def _reconcile_batch_override(node: StreamNode, tuned: ResourceNode) -> None:
        """Push a ResourcePlan batch override back into the single source of truth.

        ``RuntimeInfo`` (on the operator's LogicalFunction) is what the runtime
        actually reads; the plan record only carries ``batch_size``/
        ``async_buffer_size`` for the user-editable plan JSON. Without this the
        override would update the plan record but never reach a running actor.
        """
        logical_function = node.operator.logical_function
        if logical_function is None:
            return
        logical_function.apply_runtime_overrides(
            batch_size=tuned.batch_size,
            async_buffer_size=tuned.async_buffer_size,
        )

    def build_resource_plan(self) -> ResourcePlan:
        nodes = {}
        edges = []
        for node in self.nodes.values():
            resource_node = node.resource_plan_node
            nodes[resource_node.key] = resource_node
        for (src, dst), partitioner in self._edges.items():
            edges.append(ResourceEdge(src, dst, str(partitioner)))
        return ResourcePlan(nodes, edges)

    def add_node(self, node: StreamNode) -> None:
        if node.id in self.nodes:
            return
        self.nodes[node.id] = node

    def add_edge(self, src_node: int, dst_node: int, partitioner: Partitioner) -> None:
        if src_node not in self.nodes or dst_node not in self.nodes:
            raise KeyError(f"cannot add edge {src_node} -> {dst_node}: endpoint does not exist")
        self._edges[src_node, dst_node] = partitioner

    def downstream_nodes(self, node: int) -> list[int]:
        return [dst for (src, dst) in self._edges if src == node]

    def upstream_nodes(self, node: int) -> list[int]:
        return [src for (src, dst) in self._edges if dst == node]

    def remove_node(self, node_id: int) -> None:
        """Remove a node and every incident edge."""
        self.nodes.pop(node_id, None)
        self._edges = {
            (src, dst): partitioner for (src, dst), partitioner in self._edges.items() if node_id not in {src, dst}
        }

    def _process_stream(self, stream: Stream, visited: set[int] | None = None) -> None:
        if visited is None:
            visited = set()
        node = StreamNode.load(stream)
        if node.id in visited:
            return
        visited.add(node.id)
        self.add_node(node)
        parent_streams = stream.input_streams
        if parent_streams is None:
            return
        for parent_stream in parent_streams:
            if parent_stream is not None:
                parent_node = StreamNode.load(parent_stream)
                self.add_node(parent_node)
                if parent_stream.partitioner is None:
                    input_partitioner = default_partitioner(parent_stream.concurrency, stream.concurrency)
                else:
                    input_partitioner = parent_stream.partitioner
                self.add_edge(parent_node.id, node.id, input_partitioner)
                self._process_stream(parent_stream, visited)

    @property
    def sink_nodes(self) -> set[int]:
        source_ids = {source for source, _target in self._edges}
        return self.nodes.keys() - source_ids

    @property
    def source_nodes(self) -> set[int]:
        target_ids = {target for _source, target in self._edges}
        return self.nodes.keys() - target_ids

    def __str__(self) -> str:
        # Prefer the rich tree view; fall back to the plain indented form below
        # where rich is unavailable (logs / minimal environments).
        try:
            from ray.klein.observability.progress_view import render_stream_graph

            rendered = render_stream_graph(self)
            if rendered:
                return "\n" + rendered.rstrip()
        except Exception:
            logger.debug("Rich StreamGraph rendering failed", exc_info=True)
        result = []
        sink_nodes = self.sink_nodes

        def print_node(node_id: int, node_indent: dict[int, str]) -> None:
            indent = node_indent.get(node_id, "")
            node = self.nodes[node_id]
            result.append(f"{(indent + ' +- ') if len(indent) > 0 else ''}{node}")
            upstream_nodes = self.upstream_nodes(node_id)
            for upstream_node in upstream_nodes:
                indent = indent.replace("|", " ")
                node_indent[upstream_node] = indent + "    | " if len(upstream_nodes) > 1 else indent + " " * 6
            for upstream_node in upstream_nodes:
                indent = node_indent.get(upstream_node, "")[:-3]
                result.append(f"{indent} +- {self.partitioner_for(upstream_node, node_id)}")
                print_node(upstream_node, node_indent)

        for node_id in sink_nodes:
            node_indent = {node_id: ""}
            print_node(node_id, node_indent)
            result.append("")

        return "\n".join(result)
