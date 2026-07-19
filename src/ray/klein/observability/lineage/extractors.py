# SPDX-License-Identifier: Apache-2.0
"""Klein logical-graph lineage extraction for portable integrations."""

from ray.klein._internal.logging import get_logger
from ray.klein.api.ray_data.call import RayDataCall
from ray.klein.observability.lineage.models import DatasetInfo
from ray.klein.runtime.graph.logical_graph import LogicalGraph

logger = get_logger(__name__)


def extract_datasets_from_klein_graph(graph: LogicalGraph) -> tuple[list[DatasetInfo], list[DatasetInfo]]:
    inputs: list[DatasetInfo] = []
    outputs: list[DatasetInfo] = []

    try:
        for source_id in graph.sources:
            vertex = graph.get(source_id)
            ds_info = _extract_klein_node_dataset(vertex, is_source=True)
            if ds_info:
                inputs.append(ds_info)
    except Exception:
        logger.debug("Failed to extract Klein source info", exc_info=True)

    try:
        for sink_id in graph.sinks:
            vertex = graph.get(sink_id)
            ds_info = _extract_klein_node_dataset(vertex, is_source=False)
            if ds_info:
                outputs.append(ds_info)
    except Exception:
        logger.debug("Failed to extract Klein sink info", exc_info=True)

    return inputs, outputs


def _format_bootstrap_servers(bootstrap_servers) -> str | None:
    if not bootstrap_servers:
        return None
    if isinstance(bootstrap_servers, str):
        servers = bootstrap_servers
    else:
        servers = ",".join(str(server) for server in bootstrap_servers)
    return servers or None


def _extract_ray_kafka_call(call: RayDataCall, *, is_source: bool) -> DatasetInfo | None:
    expected_target = "read_kafka" if is_source else "write_kafka"
    if call.target != expected_target:
        return None
    topic_value = call.args[0] if call.args else call.kwargs.get("topics" if is_source else "topic", "unknown")
    if isinstance(topic_value, str):
        topic_name = topic_value
    else:
        topic_name = ",".join(str(topic) for topic in topic_value) or "unknown"
    bootstrap_servers = call.kwargs.get("bootstrap_servers")
    if bootstrap_servers is None and len(call.args) > 1:
        bootstrap_servers = call.args[1]
    return DatasetInfo(
        namespace="kafka",
        name=topic_name,
        bootstrap_servers=_format_bootstrap_servers(bootstrap_servers),
    )


def _extract_klein_node_dataset(vertex, is_source: bool) -> DatasetInfo | None:
    try:
        logical_function = vertex.operator.logical_function
        if logical_function is None:
            return None
        lowering = logical_function.batch_lowering
        if isinstance(lowering, RayDataCall):
            dataset = _extract_ray_kafka_call(lowering, is_source=is_source)
            if dataset is not None:
                return dataset

        function_class = logical_function.function
        args = logical_function.constructor_args
        kwargs = logical_function.constructor_kwargs
        is_redis_sink = (
            isinstance(function_class, type)
            and function_class.__name__ == "RedisSink"
            and function_class.__module__ == "ray.klein.integrations.redis.sink"
        )
        if not is_source and is_redis_sink:
            connection = args[0] if args else kwargs.get("connection")
            if all(hasattr(connection, attribute) for attribute in ("host", "port", "database")):
                return DatasetInfo(
                    namespace="redis",
                    name=f"redis://{connection.host}:{connection.port}/{connection.database}",
                )

        function_name = getattr(function_class, "__name__", str(function_class))
        logger.debug("Unsupported Klein connector for lineage: %s", function_name)
    except Exception:
        logger.debug("Failed to extract Klein node dataset info", exc_info=True)

    return None
