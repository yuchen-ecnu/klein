# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph


def expand_execution_graph(logical_graph) -> ExecutionGraph:
    """Expand a logical graph with test-only metrics and no job configuration."""

    return ExecutionGraph.expand(
        logical_graph,
        Configuration(),
        JobMetricGroup(job_name="test"),
        "test",
    )
