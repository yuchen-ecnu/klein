# SPDX-License-Identifier: Apache-2.0
import os

from ray.klein.api.node_type import NodeType
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.api.stream_node import StreamNode
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.runtime.graph.resource_plan_generator import generate_resource_plan
from ray.klein.runtime.partitioning import AdaptivePartitioner, ForwardPartitioner, RescalePartitioner
from ray.klein.runtime.resources import Resources


def assert_operator_resource(
    plan: ResourcePlan,
    operator: str,
    num_cpus: float | None,
    num_gpus: float | None,
    concurrency: int | tuple[int, int] | None,
    batch_size: int | None,
) -> None:
    node = plan.nodes[operator]
    assert (node.num_cpus, node.num_gpus, node.concurrency, node.batch_size) == (
        num_cpus,
        num_gpus,
        concurrency,
        batch_size,
    )


def test_dry_run_restores_environment(monkeypatch, tmp_path, project_root) -> None:
    monkeypatch.setenv(EnvironmentVariables.DEBUG, "original-debug")
    monkeypatch.setenv(EnvironmentVariables.COMPILE_ONLY, "original-compile")

    generate_resource_plan(
        str(project_root / "tests/fixtures/resource_plan"),
        "pipeline.py",
        str(tmp_path / "resource-graph.json"),
    )

    assert os.environ[EnvironmentVariables.DEBUG] == "original-debug"
    assert os.environ[EnvironmentVariables.COMPILE_ONLY] == "original-compile"


def test_dry_run_generates_expected_resource_plan(tmp_path, project_root) -> None:
    output = tmp_path / "resource-graph.json"
    generate_resource_plan(str(project_root / "tests/fixtures/resource_plan"), "pipeline.py", str(output))
    plan = ResourcePlan.read(output)

    assert_operator_resource(plan, "RayData.read_csv[1]", None, None, None, None)
    assert_operator_resource(plan, "FlatMap[2]", 1.2, 0.25, 2, None)
    assert_operator_resource(plan, "MapBatches[3]", 0.25, None, None, 16)
    assert_operator_resource(plan, "Map[4]", None, 1.4, None, None)
    assert_operator_resource(plan, "Map[5]", 2.0, None, 6, None)
    assert_operator_resource(plan, "ep1[6]", 1, 0.5, None, None)
    assert_operator_resource(plan, "ep2[7]", 1, 0.5, None, None)
    assert_operator_resource(plan, "RayData.write_parquet[8]", None, None, None, None)
    assert_operator_resource(plan, "RayData.write_parquet[9]", None, None, None, None)


def test_graph_adjacency() -> None:
    graph = StreamGraph("test-job")
    graph.add_node(StreamNode(1, "source", None, Resources(1.0, 1.0, 2), NodeType.SOURCE))
    graph.add_node(StreamNode(2, "pre-process", None, Resources(1.0, 1.0, (1, 2)), NodeType.TRANSFORM))
    graph.add_node(StreamNode(3, "infer", None, Resources(1.0, 1.0, 2), NodeType.TRANSFORM))
    graph.add_node(StreamNode(4, "sink-1", None, Resources(1.0, 1.0, 2), NodeType.SINK))
    graph.add_node(StreamNode(5, "sink-2", None, Resources(1.0, 1.0, 2), NodeType.SINK))
    graph.add_edge(1, 2, RescalePartitioner())
    graph.add_edge(2, 3, AdaptivePartitioner())
    graph.add_edge(3, 4, ForwardPartitioner())
    graph.add_edge(3, 5, ForwardPartitioner())

    assert set(graph.nodes) == {1, 2, 3, 4, 5}
    assert graph.upstream_nodes(1) == []
    assert graph.downstream_nodes(1) == [2]
    assert graph.upstream_nodes(2) == [1]
    assert graph.downstream_nodes(2) == [3]
    assert graph.upstream_nodes(3) == [2]
    assert set(graph.downstream_nodes(3)) == {4, 5}
    assert graph.downstream_nodes(4) == []
    assert graph.downstream_nodes(5) == []
