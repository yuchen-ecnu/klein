# SPDX-License-Identifier: Apache-2.0
"""Pure Ray Serve region rewriting for the logical graph."""

from dataclasses import dataclass

from ray.klein._internal.logging import get_logger
from ray.klein._internal.partitioning import default_partitioner
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.functions.ray_data_lowering import lower_map_batches
from ray.klein.api.node_type import NodeType
from ray.klein.config.serve_options import ServeOptions
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.operator.map_operator import MapOperator
from ray.klein.runtime.resources import Resources

logger = get_logger(__name__)

_PROXY_NAME = "EmbeddedProxyClient"


@dataclass(frozen=True, slots=True)
class _ServeRegion:
    node_ids: tuple[VertexId, ...]

    @property
    def proxy_id(self) -> VertexId:
        return min(self.node_ids, key=lambda vertex_id: vertex_id.index)


@dataclass(frozen=True, slots=True)
class _ProxyConfig:
    num_cpus: float
    concurrency: int | tuple[int, int]
    batch_size: int
    batch_timeout: int
    async_buffer_size: int


class ServeRewriter:
    """Discover and replace the graph's one Ray Serve operator chain."""

    def __init__(self, graph: LogicalGraph) -> None:
        self._graph = graph

    def extract_serve_functions(self) -> tuple[LogicalFunction, ...]:
        """Return the region's function recipes without changing the graph."""

        region = self._discover_region()
        return () if region is None else self._region_functions(region)

    def rewrite(self) -> LogicalGraph:
        """Return a graph with the Serve region replaced by one proxy vertex."""

        region = self._discover_region()
        if region is None:
            return self._graph
        proxy = self._build_proxy_vertex(region, self._resolve_proxy_config(region))
        return self._splice_in_proxy(region, proxy)

    def _discover_region(self) -> _ServeRegion | None:
        serve_ids = {vertex_id for vertex_id, vertex in self._graph.vertices.items() if vertex.ray_serve_enabled}
        if not serve_ids:
            return None
        components = self._connected_components(serve_ids)
        if len(components) != 1:
            raise ValueError("Multiple disconnected ray_serve_enabled regions found in the LogicalGraph.")
        return _ServeRegion(tuple(self._ordered_chain(set(components[0]))))

    def _connected_components(self, serve_ids: set[VertexId]) -> list[list[VertexId]]:
        seen: set[VertexId] = set()
        components: list[list[VertexId]] = []
        for start in sorted(serve_ids, key=lambda vertex_id: vertex_id.index):
            if start in seen:
                continue
            component: list[VertexId] = []
            stack = [start]
            while stack:
                vertex_id = stack.pop()
                if vertex_id in seen:
                    continue
                seen.add(vertex_id)
                component.append(vertex_id)
                neighbors = (*self._graph.downstream(vertex_id), *self._graph.upstream(vertex_id))
                stack.extend(neighbor for neighbor in neighbors if neighbor in serve_ids and neighbor not in seen)
            components.append(component)
        return components

    def _ordered_chain(self, region: set[VertexId]) -> list[VertexId]:
        def within(neighbors: tuple[VertexId, ...]) -> list[VertexId]:
            return [neighbor for neighbor in neighbors if neighbor in region]

        heads: list[VertexId] = []
        for vertex_id in sorted(region, key=lambda candidate: candidate.index):
            upstream = within(self._graph.upstream(vertex_id))
            downstream = within(self._graph.downstream(vertex_id))
            if len(upstream) > 1 or len(downstream) > 1:
                raise ValueError(
                    f"ray_serve_enabled vertex {self._graph.get(vertex_id).name} branches inside the Serve "
                    "region; only a linear chain is supported"
                )
            if not upstream:
                heads.append(vertex_id)
        if len(heads) != 1:
            raise ValueError(f"Ray Serve region must have exactly one head; found {len(heads)}")

        ordered: list[VertexId] = []
        current: VertexId | None = heads[0]
        while current is not None:
            ordered.append(current)
            downstream = within(self._graph.downstream(current))
            current = downstream[0] if downstream else None
        if len(ordered) != len(region):
            raise ValueError("Ray Serve region is not a connected linear chain")
        return ordered

    def _region_functions(self, region: _ServeRegion) -> tuple[LogicalFunction, ...]:
        functions: list[LogicalFunction] = []
        for vertex_id in region.node_ids:
            function = self._graph.get(vertex_id).operator.logical_function
            if function is None:
                raise ValueError(f"Ray Serve vertex {vertex_id} has no logical function")
            functions.append(function)
        logger.debug("Found %d functions in the Ray Serve region", len(functions))
        return tuple(functions)

    def _resolve_proxy_config(self, region: _ServeRegion) -> _ProxyConfig:
        async_buffer_size = self._graph.config.get(ServeOptions.CLIENT_ASYNC_BUFFER_SIZE)
        if len(region.node_ids) == 1:
            vertex = self._graph.get(region.node_ids[0])
            runtime_info = vertex.operator.runtime_info
            if runtime_info.batch_size is None or runtime_info.batch_timeout is None:
                raise ValueError("A Ray Serve vertex must configure batch_size and batch_timeout")
            return _ProxyConfig(
                num_cpus=vertex.resources.cpus,
                concurrency=vertex.resources.effective_concurrency,
                batch_size=runtime_info.batch_size,
                batch_timeout=runtime_info.batch_timeout,
                async_buffer_size=async_buffer_size,
            )

        self._require_client_config(region)
        return _ProxyConfig(
            num_cpus=self._graph.config.get(ServeOptions.CLIENT_NUM_CPUS),
            concurrency=self._graph.config.get(ServeOptions.CLIENT_CONCURRENCY),
            batch_size=self._graph.config.get(ServeOptions.CLIENT_BATCH_SIZE),
            batch_timeout=self._graph.config.get(ServeOptions.CLIENT_BATCH_TIMEOUT),
            async_buffer_size=async_buffer_size,
        )

    def _require_client_config(self, region: _ServeRegion) -> None:
        required = (
            ServeOptions.CLIENT_NUM_CPUS,
            ServeOptions.CLIENT_CONCURRENCY,
            ServeOptions.CLIENT_BATCH_SIZE,
            ServeOptions.CLIENT_BATCH_TIMEOUT,
        )
        if any(self._graph.config.get_optional(option) is None for option in required):
            raise ValueError(
                f"The {len(region.node_ids)}-operator Ray Serve region requires explicit client CPU, "
                "concurrency, batch-size and batch-timeout configuration"
            )

    def _build_proxy_vertex(self, region: _ServeRegion, config: _ProxyConfig) -> VertexSpec:
        try:
            from ray.klein.runtime.serve_client import EmbeddedProxyClient
        except ImportError as exc:
            raise ImportError(
                "Ray Serve integration requires optional dependencies; install ray-klein[serve]."
            ) from exc

        resources = Resources(config.num_cpus, 0.0, config.concurrency)
        operator = MapOperator(
            LogicalFunction(
                EmbeddedProxyClient,
                lowering=lower_map_batches,
                resources=resources,
                batch_size=config.batch_size,
                batch_timeout=config.batch_timeout,
                async_buffer_size=config.async_buffer_size,
            )
        )
        operator.id = region.proxy_id.index
        operator.name = _PROXY_NAME
        return VertexSpec(
            id=region.proxy_id,
            name=f"{_PROXY_NAME}[{region.proxy_id.index}]",
            operator=operator.to_spec(),
            node_type=NodeType.TRANSFORM,
            resources=resources,
        )

    def _splice_in_proxy(self, region: _ServeRegion, proxy: VertexSpec) -> LogicalGraph:
        region_ids = set(region.node_ids)
        upstream = {
            vertex_id
            for region_id in region.node_ids
            for vertex_id in self._graph.upstream(region_id)
            if vertex_id not in region_ids
        }
        downstream = {
            vertex_id
            for region_id in region.node_ids
            for vertex_id in self._graph.downstream(region_id)
            if vertex_id not in region_ids
        }

        builder = self._graph.to_builder()
        for vertex_id in region.node_ids:
            builder.remove_vertex(vertex_id)
        builder.add_vertex(proxy)
        for vertex_id in sorted(upstream, key=lambda candidate: candidate.index):
            source = self._graph.get(vertex_id)
            builder.add_edge(
                EdgeSpec(
                    vertex_id,
                    proxy.id,
                    default_partitioner(source.concurrency, proxy.concurrency).to_spec(),
                )
            )
        for vertex_id in sorted(downstream, key=lambda candidate: candidate.index):
            target = self._graph.get(vertex_id)
            builder.add_edge(
                EdgeSpec(
                    proxy.id,
                    vertex_id,
                    default_partitioner(proxy.concurrency, target.concurrency).to_spec(),
                )
            )
        return builder.build()
