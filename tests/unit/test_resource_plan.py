# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables


@pytest.fixture()
def resource_plan_case(monkeypatch, tmp_path, test_data_dir):
    output_path = tmp_path / "stream-graph.json"
    monkeypatch.setenv(EnvironmentVariables.DEBUG, "1")
    monkeypatch.setenv(EnvironmentVariables.COMPILE_ONLY, "1")
    monkeypatch.setenv(EnvironmentVariables.RESOURCE_PLAN_OUTPUT, str(output_path))
    context = KleinContext()
    context.from_values({"id": 1}, {"id": 2}, {"id": 3}, name="TestValueSource").map(
        lambda row: {"id": row["id"]}, num_cpus=4, num_gpus=1, name="TestInfer"
    ).write_kafka(
        topic="test_topic",
        bootstrap_servers="example.com",
    )
    return context, output_path, test_data_dir.parent / "plans"


def assert_resources(
    plan: ResourcePlan,
    *,
    num_infer_gpus=1.0,
    infer_batch_size=None,
    num_infer_cpu=4.0,
    infer_concurrency=None,
) -> None:
    source = plan.nodes["TestValueSource[1]"]
    infer = plan.nodes["TestInfer[2]"]
    sink = plan.nodes["KafkaSink[3]"]
    assert (source.id, source.name, source.num_cpus, source.num_gpus, source.concurrency, source.batch_size) == (
        1,
        "TestValueSource",
        None,
        None,
        None,
        None,
    )
    assert (infer.id, infer.name, infer.num_cpus, infer.num_gpus, infer.concurrency, infer.batch_size) == (
        2,
        "TestInfer",
        num_infer_cpu,
        num_infer_gpus,
        infer_concurrency,
        infer_batch_size,
    )
    assert (sink.id, sink.name, sink.num_cpus, sink.num_gpus, sink.concurrency, sink.batch_size) == (
        3,
        "KafkaSink",
        None,
        None,
        None,
        None,
    )


def test_resource_plan_round_trip(resource_plan_case) -> None:
    _, output_path, plans_dir = resource_plan_case
    plan = ResourcePlan.read(plans_dir / "sample_resource_plan.json")

    plan.write(output_path)

    assert ResourcePlan.read(output_path) == plan


def test_resource_plan_overrides_execution(resource_plan_case, monkeypatch, tmp_path) -> None:
    context, output_path, plans_dir = resource_plan_case
    overwrite_path = tmp_path / "overwrite.json"
    plan = ResourcePlan.read(plans_dir / "sample_resource_plan.json")
    plan.update_node(
        "TestInfer[2]",
        batch_size=64,
        num_gpus=0.1,
        num_cpus=2,
        concurrency=(3, 5),
    )
    plan.write(overwrite_path)

    monkeypatch.setenv(EnvironmentVariables.RESOURCE_PLAN_INPUT, str(overwrite_path))
    context.execute("resource-overrides")

    assert_resources(
        ResourcePlan.read(output_path),
        num_infer_gpus=0.1,
        infer_batch_size=64,
        num_infer_cpu=2,
        infer_concurrency=(3, 5),
    )


def test_batch_size_override_reaches_runtime_info(resource_plan_case) -> None:
    context, _, _ = resource_plan_case
    graph = StreamGraph.from_sinks(context.sinks, "batch-override", Configuration())
    plan = graph.build_resource_plan()
    plan.update_node("TestInfer[2]", batch_size=64)

    graph.apply_resource_plan(plan)

    assert graph.nodes[2].resource_plan_node.batch_size == 64
    assert graph.nodes[2].operator.logical_function.runtime_info.batch_size == 64


def test_resource_plan_updates_revalidate_immutable_nodes(resource_plan_case) -> None:
    context, _, _ = resource_plan_case
    graph = StreamGraph.from_sinks(context.sinks, "validated-override", Configuration())
    plan = graph.build_resource_plan()

    with pytest.raises(ValueError, match="batch_size"):
        plan.update_node("TestInfer[2]", batch_size=0)
    with pytest.raises(TypeError, match="unsupported resource overrides"):
        plan.update_node("TestInfer[2]", name="renamed")

    assert plan["TestInfer[2]"].batch_size is None


def test_compile_persists_default_resource_plan(resource_plan_case) -> None:
    context, output_path, _ = resource_plan_case

    context.execute("default-plan")

    assert_resources(ResourcePlan.read(output_path))


def test_valid_resource_plan_is_loaded(resource_plan_case, monkeypatch) -> None:
    context, output_path, plans_dir = resource_plan_case
    monkeypatch.setenv(EnvironmentVariables.RESOURCE_PLAN_INPUT, str(plans_dir / "valid_resource_plan.json"))

    context.execute("valid-plan")

    assert_resources(ResourcePlan.read(output_path), num_infer_gpus=1.5, infer_batch_size=6)


@pytest.mark.parametrize(
    "filename",
    [
        "invalid_name_resource_plan.json",
        "invalid_node_resource_plan.json",
        "missing_node_resource_plan.json",
        "redundant_node_resource_plan.json",
    ],
)
def test_invalid_resource_plans_fail_fast(resource_plan_case, monkeypatch, filename) -> None:
    context, _, plans_dir = resource_plan_case
    monkeypatch.setenv(EnvironmentVariables.RESOURCE_PLAN_INPUT, str(plans_dir / filename))

    with pytest.raises(ValueError):
        context.execute("invalid-plan")
