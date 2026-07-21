# SPDX-License-Identifier: Apache-2.0
"""Pure Ray Serve region rewriting for the logical graph."""

import inspect
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
from ray.klein.runtime.partitioning.channel_topology import ChannelPattern
from ray.klein.runtime.partitioning.forward_partitioner import ForwardPartitioner
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec
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
        region = _ServeRegion(tuple(self._ordered_chain(set(components[0]))))
        self._validate_region(region)
        return region

    def _validate_region(self, region: _ServeRegion) -> None:
        """Reject shapes and functions the current Serve runtime cannot preserve."""

        region_ids = set(region.node_ids)
        head, tail = region.node_ids[0], region.node_ids[-1]
        for edge in self._graph.edges:
            if edge.source not in region_ids and edge.target in region_ids and edge.target != head:
                raise ValueError(
                    "Ray Serve region has an external input into an internal vertex; "
                    "external inputs may only enter the region head"
                )
            if edge.source in region_ids and edge.target not in region_ids and edge.source != tail:
                raise ValueError(
                    "Ray Serve region has an external output from an internal vertex; "
                    "external outputs may only leave the region tail"
                )

        for vertex_id in region.node_ids:
            self._validate_supported_vertex(self._graph.get(vertex_id))

    @staticmethod
    def _validate_supported_vertex(vertex: VertexSpec) -> None:
        logical_function = vertex.operator.logical_function
        if (
            vertex.operator.operator_class is not MapOperator
            or logical_function is None
            or logical_function.batch_lowering is not lower_map_batches
        ):
            raise ValueError(f"Ray Serve vertex {vertex.name} is unsupported; only map_batches operators are supported")

        batch_format = logical_function.runtime_info.batch_format
        if batch_format not in {"default", "numpy"}:
            raise ValueError(
                f"Ray Serve vertex {vertex.name} has unsupported batch_format {batch_format!r}; "
                "use 'default' or 'numpy'"
            )

        ServeRewriter._validate_callable(vertex.name, logical_function.function)

    @staticmethod
    def _validate_callable(vertex_name: str, function) -> None:
        candidates = ServeRewriter._callable_candidates(vertex_name, function)
        candidates = [ServeRewriter._unwrap_callable(vertex_name, candidate) for candidate in candidates]
        if any(ServeRewriter._is_async_or_generator(candidate) for candidate in candidates):
            raise ValueError(
                f"Ray Serve vertex {vertex_name} is unsupported; only synchronous, non-generator callables "
                "are supported"
            )
        ServeRewriter._validate_close(vertex_name, function)
        ServeRewriter._validate_constructor_context(vertex_name, function)

    @staticmethod
    def _callable_candidates(vertex_name: str, function) -> list:
        if inspect.isclass(function):
            if not any("__call__" in base.__dict__ for base in function.__mro__):
                raise ValueError(
                    f"Ray Serve vertex {vertex_name} is unsupported; callable classes must define __call__"
                )
            return [function, function.__call__]
        if not callable(function):
            raise ValueError(f"Ray Serve vertex {vertex_name} is unsupported; map_batches function must be callable")
        return [function] if inspect.isfunction(function) else [function, type(function).__call__]

    @staticmethod
    def _unwrap_callable(vertex_name: str, function) -> object:
        try:
            return inspect.unwrap(function)
        except ValueError as error:
            raise ValueError(f"Ray Serve vertex {vertex_name} has an invalid callable wrapper chain") from error

    @staticmethod
    def _validate_close(vertex_name: str, function) -> None:
        close = inspect.getattr_static(function, "close", None)
        if isinstance(close, (classmethod, staticmethod)):
            close = close.__func__
        if close is not None:
            close = ServeRewriter._unwrap_callable(f"{vertex_name} close()", close)
            if ServeRewriter._is_async_or_generator(close):
                raise ValueError(
                    f"Ray Serve vertex {vertex_name} is unsupported; close() must be synchronous and non-generator"
                )

    @staticmethod
    def _validate_constructor_context(vertex_name: str, function) -> None:
        if inspect.isclass(function):
            try:
                needs_runtime_context = "runtime_context" in inspect.signature(function.__init__).parameters
            except (TypeError, ValueError):
                needs_runtime_context = False
            if needs_runtime_context:
                raise ValueError(
                    f"Ray Serve vertex {vertex_name} is unsupported; callable classes requiring "
                    "runtime_context cannot run in a Serve deployment"
                )

    @staticmethod
    def _is_async_or_generator(function) -> bool:
        return any(
            predicate(function)
            for predicate in (
                inspect.iscoroutinefunction,
                inspect.isasyncgenfunction,
                inspect.isgeneratorfunction,
            )
        )

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
        incoming = [edge for edge in self._graph.edges if edge.source not in region_ids and edge.target in region_ids]
        outgoing = [edge for edge in self._graph.edges if edge.source in region_ids and edge.target not in region_ids]

        builder = self._graph.to_builder()
        for vertex_id in region.node_ids:
            builder.remove_vertex(vertex_id)
        builder.add_vertex(proxy)
        for edge in sorted(incoming, key=lambda candidate: candidate.source.index):
            source = self._graph.get(edge.source)
            builder.add_edge(
                EdgeSpec(
                    edge.source,
                    proxy.id,
                    self._retarget_partitioner(edge.partitioner, source.concurrency, proxy.concurrency),
                )
            )
        for edge in sorted(outgoing, key=lambda candidate: candidate.target.index):
            target = self._graph.get(edge.target)
            builder.add_edge(
                EdgeSpec(
                    proxy.id,
                    edge.target,
                    self._retarget_partitioner(edge.partitioner, proxy.concurrency, target.concurrency),
                )
            )
        return builder.build()

    @staticmethod
    def _retarget_partitioner(
        partitioner: PartitionerSpec,
        source_concurrency: int | tuple[int, int],
        target_concurrency: int | tuple[int, int],
    ) -> PartitionerSpec:
        if partitioner.topology.pattern is ChannelPattern.FORWARD and source_concurrency != target_concurrency:
            if partitioner.partitioner_class is not ForwardPartitioner:
                raise ValueError(
                    f"Cannot preserve custom FORWARD partitioner {partitioner.name!r} when replacing a Ray Serve "
                    f"boundary with concurrency {source_concurrency!r} -> {target_concurrency!r}"
                )
            return default_partitioner(source_concurrency, target_concurrency).to_spec()
        return partitioner
