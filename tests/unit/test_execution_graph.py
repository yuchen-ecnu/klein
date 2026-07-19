# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer


def _execution_graph() -> ExecutionGraph:
    config = Configuration(include_environment=False)
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    context = KleinContext(config)
    context.from_values({"value": 1}, {"value": 2}).map(lambda value: value, concurrency=2).write(ConsoleSinkFunction)
    logical = LogicalGraph.from_sinks(context.sinks, "execution-test", config)
    optimized = LogicalOptimizer(config).optimize(logical)
    return ExecutionGraph.expand(optimized, config, JobMetricGroup("execution-test"), "execution-test")


def test_physical_graph_views_are_stable_sequences() -> None:
    graph = _execution_graph()

    assert isinstance(graph.execution_vertices, tuple)
    assert isinstance(graph.source_execution_vertices, tuple)
    assert isinstance(graph.sink_execution_vertices, tuple)
    assert [vertex.id for vertex in graph.execution_vertices] == [
        ExecutionVertexId(1, 0),
        ExecutionVertexId(2, 0),
        ExecutionVertexId(2, 1),
        ExecutionVertexId(3, 0),
    ]
    assert graph.source_job_vertices == (1,)
    assert graph.sink_job_vertices == (3,)


def test_physical_graph_distinguishes_strict_and_optional_lookup() -> None:
    graph = _execution_graph()
    missing = ExecutionVertexId(99, 0)

    assert graph.find_execution_vertex(missing) is None
    with pytest.raises(KeyError):
        graph.execution_vertex(missing)
