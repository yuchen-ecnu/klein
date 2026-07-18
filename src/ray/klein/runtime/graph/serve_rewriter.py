# SPDX-License-Identifier: Apache-2.0
"""Ray Serve region rewriter.

A ``ray_serve_enabled`` pipeline marks a connected run of operators that should
execute behind a Ray Serve deployment instead of as ordinary Klein tasks. Two
things need to happen with that region, and they are deliberately separate
public methods rather than one flag-driven entry point:

* :meth:`ServeRewriter.extract_serve_functions` — read-only. Returns the
  region's logical functions so the serve subprocess can rebuild the operators
  inside the deployment. The graph is left untouched.
* :meth:`ServeRewriter.rewrite` — mutating. Replaces the whole region with a
  single :class:`EmbeddedProxyClient` node that forwards batches to the
  deployment, then rewires the region's external upstream/downstream edges to
  that proxy.

Both share region discovery and validation; only ``rewrite`` performs graph
surgery.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger
from ray.klein._internal.partitioning import default_partitioner
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.functions.ray_data_lowering import lower_map_batches
from ray.klein.api.node_type import NodeType
from ray.klein.api.stream_node import StreamNode
from ray.klein.config.serve_options import ServeOptions
from ray.klein.runtime.operator.map_operator import MapOperator
from ray.klein.runtime.resources import Resources

logger = get_logger(__name__)

if TYPE_CHECKING:
    from ray.klein.api.stream_graph import StreamGraph

_PROXY_NAME = "EmbeddedProxyClient"


@dataclass(frozen=True, slots=True)
class _ServeRegion:
    """The single connected set of ``ray_serve_enabled`` nodes in a graph."""

    node_ids: tuple[int, ...]

    @property
    def proxy_id(self) -> int:
        # The replacement proxy reuses the lowest id in the region so it keeps a
        # stable, source-ward position in the (id-ordered) graph.
        return min(self.node_ids)


@dataclass(frozen=True, slots=True)
class _ProxyConfig:
    """Resolved resource/batching settings for the proxy node."""

    num_cpus: float
    concurrency: int | tuple[int, int]
    batch_size: int
    batch_timeout: int
    async_buffer_size: int


class ServeRewriter:
    """Discover the ray_serve region of a StreamGraph and act on it."""

    def __init__(self, stream_graph: "StreamGraph") -> None:
        self._stream_graph = stream_graph

    # ---- public API -------------------------------------------------------

    def extract_serve_functions(self) -> list[LogicalFunction]:
        """Return the region's logical functions without touching the graph."""
        region = self._discover_region()
        if region is None:
            return []
        return self._region_functions(region)

    def rewrite(self) -> list[LogicalFunction]:
        """Replace the region with a proxy node; return the region's functions."""
        region = self._discover_region()
        if region is None:
            return []
        functions = self._region_functions(region)
        config = self._resolve_proxy_config(region)
        proxy_node = self._build_proxy_node(region, config)
        self._splice_in_proxy(region, proxy_node)
        return functions

    # ---- region discovery -------------------------------------------------

    def _discover_region(self) -> _ServeRegion | None:
        """Find and validate the single connected ray_serve region (if any)."""
        serve_ids = {node.id for node in self._stream_graph.nodes.values() if node.ray_serve_enabled}
        if not serve_ids:
            return None

        components = self._connected_components(serve_ids)
        if len(components) > 1:
            raise ValueError("Multiple disconnected ray_serve_enabled regions found in the StreamGraph.")

        # The deployment runs the region as a linear operator chain
        # (`for op in operators: data = op(data)`), so the region must itself be
        # a straight line: each node has at most one in-region upstream and one
        # in-region downstream. `_ordered_chain` both validates this and returns
        # the nodes in execution order (an id sort is not guaranteed to be a
        # topological order).
        return _ServeRegion(tuple(self._ordered_chain(set(components[0]))))

    def _connected_components(self, serve_ids: set[int]) -> list[list[int]]:
        """Group serve nodes into connected components (serve-to-serve edges)."""
        stream_graph = self._stream_graph
        seen: set[int] = set()
        components: list[list[int]] = []
        for start in serve_ids:
            if start in seen:
                continue
            component: list[int] = []
            stack = [start]
            while stack:
                node_id = stack.pop()
                if node_id in seen:
                    continue
                seen.add(node_id)
                component.append(node_id)
                neighbors = stream_graph.downstream_nodes(node_id) + stream_graph.upstream_nodes(node_id)
                stack.extend(neighbor for neighbor in neighbors if neighbor in serve_ids and neighbor not in seen)
            components.append(component)
        return components

    def _ordered_chain(self, region_set: set[int]) -> list[int]:
        """Validate the region is a linear chain; return it in execution order.

        Only in-region edges constrain the order — external fan-in/fan-out
        (e.g. a bypass sink hanging off a serve node) is fine, since the rewrite
        rewires those external edges to the proxy separately.
        """
        stream_graph = self._stream_graph

        def in_region(node_id: int, neighbors: list[int]) -> list[int]:
            return [neighbor for neighbor in neighbors if neighbor in region_set]

        heads = []
        for node_id in region_set:
            upstream_ids = in_region(node_id, stream_graph.upstream_nodes(node_id))
            downstream_ids = in_region(node_id, stream_graph.downstream_nodes(node_id))
            if len(upstream_ids) > 1 or len(downstream_ids) > 1:
                raise ValueError(
                    f"ray_serve_enabled node {stream_graph.nodes[node_id].name} has multiple "
                    "in-region upstream/downstream nodes; the serve region must be "
                    "a linear chain. It's not allowed for now."
                )
            if not upstream_ids:
                heads.append(node_id)

        if len(heads) != 1:
            raise ValueError(
                f"ray_serve_enabled region must be a single linear chain with one head, but found {len(heads)} head(s)."
            )

        ordered: list[int] = []
        current: int | None = heads[0]
        while current is not None:
            ordered.append(current)
            downstream_ids = in_region(current, stream_graph.downstream_nodes(current))
            current = downstream_ids[0] if downstream_ids else None

        if len(ordered) != len(region_set):
            raise ValueError("ray_serve_enabled region is not a connected linear chain.")
        return ordered

    def _region_functions(self, region: _ServeRegion) -> list[LogicalFunction]:
        functions: list[LogicalFunction] = []
        for node_id in region.node_ids:
            logical_function = self._stream_graph.nodes[node_id].operator.logical_function
            if logical_function is None:
                raise ValueError(f"Ray Serve node {node_id} has no logical function")
            functions.append(logical_function)
        logger.debug("Found %d functions in the Ray Serve region", len(functions))
        return functions

    # ---- proxy construction ----------------------------------------------

    def _resolve_proxy_config(self, region: _ServeRegion) -> _ProxyConfig:
        """Pick proxy resources: a lone serve node keeps its own, otherwise the Serve client defaults."""
        stream_graph = self._stream_graph
        async_buffer_size = stream_graph.config.get(ServeOptions.CLIENT_ASYNC_BUFFER_SIZE)

        if len(region.node_ids) == 1:
            logger.info("Using the single Ray Serve node's resource configuration for its embedded client")
            node = stream_graph.nodes[region.node_ids[0]]
            runtime_info = node.operator.runtime_info
            return _ProxyConfig(
                num_cpus=node.resources.cpus,
                concurrency=node.resources.effective_concurrency,
                batch_size=runtime_info.batch_size,
                batch_timeout=runtime_info.batch_timeout,
                async_buffer_size=async_buffer_size,
            )

        self._require_client_config(region)
        return _ProxyConfig(
            num_cpus=stream_graph.config.get(ServeOptions.CLIENT_NUM_CPUS),
            concurrency=stream_graph.config.get(ServeOptions.CLIENT_CONCURRENCY),
            batch_size=stream_graph.config.get(ServeOptions.CLIENT_BATCH_SIZE),
            batch_timeout=stream_graph.config.get(ServeOptions.CLIENT_BATCH_TIMEOUT),
            async_buffer_size=async_buffer_size,
        )

    def _require_client_config(self, region: _ServeRegion) -> None:
        config = self._stream_graph.config
        missing = (
            config.get_optional(ServeOptions.CLIENT_NUM_CPUS) is None
            or config.get_optional(ServeOptions.CLIENT_CONCURRENCY) is None
            or config.get_optional(ServeOptions.CLIENT_BATCH_SIZE) is None
        )
        if missing:
            raise ValueError(
                f"Since there are {len(region.node_ids)} operators marked as "
                "ray_serve, you have to specify the number of cpus, concurrency, "
                "batch size and batch timeout for the client by setting the "
                "`serve.client.num-cpus`, `serve.client.concurrency`, "
                "`serve.client.batch-size`, `serve.client.batch-timeout` in the "
                "configuration."
            )

    def _build_proxy_node(self, region: _ServeRegion, config: _ProxyConfig) -> StreamNode:
        try:
            from ray.klein.runtime.serve_client import EmbeddedProxyClient
        except ImportError as exc:
            raise ImportError(
                "Ray Serve integration requires optional dependencies; install ray-klein[serve]."
            ) from exc

        proxy_resources = Resources(config.num_cpus, 0.0, config.concurrency)
        proxy_operator = MapOperator(
            LogicalFunction(
                EmbeddedProxyClient,
                lowering=lower_map_batches,
                resources=proxy_resources,
                batch_size=config.batch_size,
                batch_timeout=config.batch_timeout,
                async_buffer_size=config.async_buffer_size,
            ),
        )
        proxy_operator.id = region.proxy_id
        proxy_operator.name = _PROXY_NAME
        return StreamNode(
            region.proxy_id,
            _PROXY_NAME,
            proxy_operator,
            proxy_resources,
            NodeType.TRANSFORM,
        )

    # ---- graph surgery ----------------------------------------------------

    def _splice_in_proxy(self, region: _ServeRegion, proxy_node: StreamNode) -> None:
        """Remove the region, insert the proxy, rewire external edges to it."""
        stream_graph = self._stream_graph
        region_set = set(region.node_ids)

        external_upstreams: set[int] = set()
        external_downstreams: set[int] = set()
        for node_id in region.node_ids:
            external_upstreams.update(stream_graph.upstream_nodes(node_id))
            external_downstreams.update(stream_graph.downstream_nodes(node_id))
        external_upstreams -= region_set
        external_downstreams -= region_set

        for node_id in region.node_ids:
            stream_graph.remove_node(node_id)
        stream_graph.add_node(proxy_node)

        for upstream in external_upstreams:
            stream_graph.add_edge(
                upstream,
                proxy_node.id,
                default_partitioner(
                    stream_graph.nodes[upstream].resources.effective_concurrency,
                    proxy_node.concurrency,
                ),
            )
        for downstream in external_downstreams:
            stream_graph.add_edge(
                proxy_node.id,
                downstream,
                default_partitioner(
                    proxy_node.concurrency,
                    stream_graph.nodes[downstream].resources.effective_concurrency,
                ),
            )
