# SPDX-License-Identifier: Apache-2.0
"""Pure control-plane tests for task deployment (no Ray cluster required)."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from ray.klein.api.node_type import NodeType
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.operator.sink import CollectOperator
from ray.klein.runtime.partitioning import RoundRobinPartitioner
from ray.klein.runtime.resources import Resources
from ray.klein.runtime.scheduler import task_deployer
from ray.klein.runtime.scheduler.errors import DeploymentError
from ray.klein.runtime.scheduler.placement import PlacementPlan
from ray.klein.runtime.worker.source_stream_task import SourceStreamTask
from ray.klein.runtime.worker.stream_task import StreamTask


def _vertex(
    vertex_id: int,
    node_type: NodeType,
    parallelism: int,
    *,
    operator_class: type[StreamOperator] = StreamOperator,
) -> VertexSpec:
    operator_type = OperatorType.SOURCE if node_type == NodeType.SOURCE else OperatorType.ONE_INPUT
    return VertexSpec(
        VertexId("deploy-test", vertex_id),
        f"operator-{vertex_id}",
        OperatorSpec(operator_class, None, vertex_id, f"operator-{vertex_id}", operator_type),
        node_type,
        Resources(num_cpus=vertex_id / 4, num_gpus=vertex_id / 10, concurrency=parallelism),
    )


def _graph(*, collecting_sink: bool = False) -> ExecutionGraph:
    config = Configuration(include_environment=False)
    config.set(PipelineOptions.INPUT_BUFFER_SIZE, 23)
    config.set(PipelineOptions.OUTPUT_BUFFER_MAX_ROWS, 17)
    config.set(PipelineOptions.INPUT_BUFFER_PUT_TIMEOUT, timedelta(seconds=2.5))
    builder = LogicalGraphBuilder("deploy-test", config)
    source = _vertex(1, NodeType.SOURCE, 2)
    transform = _vertex(2, NodeType.TRANSFORM, 2)
    sink = _vertex(
        3,
        NodeType.TAKE if collecting_sink else NodeType.SINK,
        1,
        operator_class=CollectOperator if collecting_sink else StreamOperator,
    )
    for spec in (source, transform, sink):
        builder.add_vertex(spec)
    builder.add_edge(EdgeSpec(source.id, transform.id, RoundRobinPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(transform.id, sink.id, RoundRobinPartitioner().to_spec()))
    graph = ExecutionGraph.expand(builder.build(), config, JobMetricGroup("deploy-test"), "deploy-ns")
    graph.mark_rescale_epoch(2, "topology-epoch-2")
    return graph


@pytest.mark.parametrize(
    ("workers", "expected"),
    [([], False), ([SimpleNamespace(node_id="worker")], True)],
)
def test_has_schedulable_worker_nodes_uses_non_head_inventory(workers, expected) -> None:
    with patch(
        "ray.klein.runtime.scheduler.assignment.cluster_worker_nodes",
        return_value=(workers, ["worker-id"] if workers else []),
    ):
        assert task_deployer.has_schedulable_worker_nodes() is expected


def test_validate_vertex_statuses_accepts_created_and_terminal_vertices() -> None:
    graph = _graph()
    graph.execution_vertices[-1].transition_to(ExecutionVertexStatus.FAILED)

    task_deployer.validate_vertex_statuses(graph)


def test_validate_vertex_statuses_rejects_live_vertex() -> None:
    graph = _graph()
    live = graph.execution_vertices[1]
    live.transition_to(ExecutionVertexStatus.DEPLOYED)

    with pytest.raises(DeploymentError, match="can not be recreated") as caught:
        task_deployer.validate_vertex_statuses(graph)

    assert caught.value.stage == "create workers"
    assert live.name in str(caught.value.cause)


def test_build_descriptor_preserves_routing_buffer_and_identity_data() -> None:
    graph = _graph()
    target = graph.job_vertex(2)
    vertex = target.execution_vertex(0)
    target.output_queue = object()

    descriptor = task_deployer.build_descriptor(
        graph,
        target,
        vertex,
        restore_operation_id="restore-7",
    )

    source_ids = tuple(source.id for source in graph.job_vertex(1).execution_vertices.values())
    assert descriptor.operator is target.operator_spec
    assert descriptor.vertex_id == vertex.id
    assert descriptor.task_name == vertex.name
    assert descriptor.task_generation == vertex.task_generation
    assert descriptor.task_index == 0
    assert descriptor.parallelism == 2
    assert descriptor.config is target.config
    assert descriptor.metric_group is vertex.task_metric_group
    assert dict(descriptor.barrier_split) == graph.barrier_splits[vertex.id]
    assert descriptor.is_committer is False
    assert descriptor.input_buffer_size == 23
    assert descriptor.output_queue is target.output_queue
    assert descriptor.namespace == "deploy-ns"
    assert descriptor.input_vertex_ids == source_ids
    assert descriptor.restore_operation_id == "restore-7"
    assert len(descriptor.out_edges) == 1
    edge = descriptor.out_edges[0]
    assert edge.target_task_names == (graph.job_vertex(3).execution_vertex(0).name,)
    assert edge.partitioner is graph.output_job_edges(2)[0].partitioner
    assert edge.control_target_indices == (0,)
    assert edge.output_buffer_max_rows == 17
    assert edge.put_timeout == 2.5
    assert edge.topology_epoch == "topology-epoch-2"


def test_build_descriptor_for_source_and_sink_has_exact_boundary_fields() -> None:
    graph = _graph()
    source = graph.job_vertex(1)
    source_descriptor = task_deployer.build_descriptor(graph, source, source.execution_vertex(1))
    sink = graph.job_vertex(3)
    sink_descriptor = task_deployer.build_descriptor(graph, sink, sink.execution_vertex(0))

    assert source_descriptor.input_vertex_ids == ()
    assert source_descriptor.out_edges[0].target_task_names == tuple(
        vertex.name for vertex in graph.job_vertex(2).execution_vertices.values()
    )
    assert source_descriptor.out_edges[0].control_target_indices == (0, 1)
    assert source_descriptor.out_edges[0].topology_epoch == "topology-epoch-2"
    assert sink_descriptor.is_committer is True
    assert sink_descriptor.out_edges == ()
    assert sink_descriptor.input_vertex_ids == tuple(
        vertex.id for vertex in graph.job_vertex(2).execution_vertices.values()
    )


@pytest.mark.parametrize("placement_kind", ["native", "node", "placement-group"])
def test_instantiate_job_vertex_passes_exact_actor_recipe_and_placement(placement_kind: str) -> None:
    graph = _graph()
    job_vertex_id = 1 if placement_kind == "placement-group" else 2
    job_vertex = graph.job_vertex(job_vertex_id)
    vertex = job_vertex.execution_vertex(0)
    old_generation = vertex.task_generation
    actor_handle = object()
    if placement_kind == "node":
        plan = PlacementPlan(node_by_vertex={vertex.id: "node-9"})
        expected_node, expected_group, expected_bundle = "node-9", None, -1
    elif placement_kind == "placement-group":
        group = object()
        plan = PlacementPlan(placement_group=group, bundle_by_vertex={vertex.id: 4})
        expected_node, expected_group, expected_bundle = None, group, 4
    else:
        plan = PlacementPlan()
        expected_node, expected_group, expected_bundle = None, None, -1

    with patch.object(task_deployer, "create_remote_actor", return_value=actor_handle) as create:
        task_deployer.instantiate_job_vertex(
            graph,
            job_vertex,
            plan,
            restore_operation_id="restore-11",
            vertices=(vertex,),
        )

    actor_class = SourceStreamTask if job_vertex_id == 1 else StreamTask
    assert vertex.stream_task is actor_handle
    assert vertex.task_generation != old_generation
    assert vertex.restore_operation_id == "restore-11"
    assert vertex.status == ExecutionVertexStatus.CREATED
    create.assert_called_once()
    args, kwargs = create.call_args
    assert args == (actor_class,)
    descriptor = kwargs["construct_args"]["descriptor"]
    assert descriptor.task_generation == vertex.task_generation
    assert descriptor.restore_operation_id == "restore-11"
    assert kwargs["ray_remote_args"] == {
        "name": vertex.name,
        "num_cpus": vertex.resources.cpus,
        "num_gpus": vertex.resources.gpus,
        "max_restarts": -1,
        "namespace": graph.namespace,
    }
    assert kwargs["schedule_node_id"] == expected_node
    assert kwargs["placement_group"] is expected_group
    assert kwargs["placement_group_bundle_index"] == expected_bundle


def test_instantiate_collecting_job_vertex_creates_and_describes_output_queue() -> None:
    graph = _graph(collecting_sink=True)
    sink = graph.job_vertex(3)
    output_queue = object()

    with (
        patch.object(task_deployer, "Queue", return_value=output_queue) as queue,
        patch.object(task_deployer, "create_remote_actor", return_value=object()) as create,
    ):
        task_deployer.instantiate_job_vertex(graph, sink, PlacementPlan())

    queue.assert_called_once_with()
    assert sink.output_queue is output_queue
    assert create.call_args.kwargs["construct_args"]["descriptor"].output_queue is output_queue


def test_place_workers_instantiates_in_graph_order_and_keeps_successful_plan() -> None:
    graph = _graph()
    plan = PlacementPlan(on_rollback=MagicMock())
    strategy = MagicMock()
    strategy.plan.return_value = plan
    created = []

    with patch.object(
        task_deployer,
        "instantiate_job_vertex",
        side_effect=lambda _graph, job_vertex, _plan: created.append(job_vertex.id),
    ):
        result = task_deployer.place_workers(graph, strategy)

    assert result is plan
    assert created == [1, 2, 3]
    strategy.plan.assert_called_once_with(graph)
    plan.on_rollback.assert_not_called()


def test_place_workers_preserves_creation_error_despite_kill_and_rollback_errors() -> None:
    graph = _graph()
    creation_error = RuntimeError("constructor failed")
    rollback_error = RuntimeError("placement cleanup failed")
    rollback = MagicMock(side_effect=rollback_error)
    plan = PlacementPlan(on_rollback=rollback)
    strategy = MagicMock()
    strategy.plan.return_value = plan
    handles = [object(), object(), object()]

    def instantiate(_graph, job_vertex, _plan) -> None:
        if job_vertex.id == 1:
            job_vertex.execution_vertex(0).stream_task = handles[0]
            job_vertex.execution_vertex(1).stream_task = handles[1]
            return
        job_vertex.execution_vertex(0).stream_task = handles[2]
        raise creation_error

    with (
        patch.object(task_deployer, "instantiate_job_vertex", side_effect=instantiate),
        patch.object(task_deployer.klein, "kill", side_effect=[RuntimeError("kill failed"), None, None]) as kill,
        pytest.raises(DeploymentError) as caught,
    ):
        task_deployer.place_workers(graph, strategy)

    assert caught.value.stage == "create workers"
    assert caught.value.cause is creation_error
    assert caught.value.__cause__ is creation_error
    assert kill.call_args_list == [call(handle) for handle in handles]
    assert graph.job_vertex(1).execution_vertex(0).stream_task is handles[0]
    assert graph.job_vertex(1).execution_vertex(1).stream_task is None
    assert graph.job_vertex(2).execution_vertex(0).stream_task is None
    rollback.assert_called_once_with()


def test_select_vertices_validates_type_ownership_and_duplicates() -> None:
    graph = _graph()
    source = graph.job_vertex(1)
    source_vertex = source.execution_vertex(0)

    assert task_deployer._select_vertices(source, None) == tuple(source.execution_vertices.values())
    assert task_deployer._select_vertices(source, (source_vertex,)) == (source_vertex,)
    with pytest.raises(TypeError, match="ExecutionVertex"):
        task_deployer._select_vertices(source, (object(),))
    with pytest.raises(ValueError, match="does not belong"):
        task_deployer._select_vertices(source, (graph.job_vertex(2).execution_vertex(0),))
    with pytest.raises(ValueError, match="duplicate"):
        task_deployer._select_vertices(source, (source_vertex, source_vertex))


def test_deploy_job_vertex_transitions_selected_tasks() -> None:
    graph = _graph()
    target = graph.job_vertex(2)
    selected = (target.execution_vertex(1),)
    selected[0].stream_task = object()

    task_deployer.deploy_job_vertex(target, vertices=selected)

    assert target.execution_vertex(0).status == ExecutionVertexStatus.CREATED
    assert selected[0].status == ExecutionVertexStatus.DEPLOYED


def test_deploy_job_vertex_marks_missing_actor_failed() -> None:
    target = _graph().job_vertex(2)
    missing = target.execution_vertex(0)

    with pytest.raises(DeploymentError, match="has not been created") as caught:
        task_deployer.deploy_job_vertex(target, vertices=(missing,))

    assert caught.value.stage == "deploy operator"
    assert missing.status == ExecutionVertexStatus.FAILED


def test_wait_job_vertex_created_pings_selected_actors_in_one_batch() -> None:
    target = _graph().job_vertex(2)
    references = []
    for index, vertex in target.execution_vertices.items():
        handle = MagicMock()
        handle.ping.return_value = f"ping-{index}"
        vertex.stream_task = handle
        references.append(handle.ping.return_value)

    with patch.object(task_deployer.klein, "get") as get:
        task_deployer.wait_job_vertex_created(target, timeout=4.5)

    get.assert_called_once_with(references, timeout=4.5)


def test_wait_job_vertex_created_skips_get_for_empty_subset() -> None:
    target = _graph().job_vertex(2)
    with patch.object(task_deployer.klein, "get") as get:
        task_deployer.wait_job_vertex_created(target, timeout=1, vertices=())
    get.assert_not_called()


@pytest.mark.parametrize("failure_site", ["missing", "ping", "get"])
def test_wait_job_vertex_created_wraps_readiness_failures(failure_site: str) -> None:
    target = _graph().job_vertex(2)
    vertex = target.execution_vertex(0)
    failure = RuntimeError(f"{failure_site} failure")
    get_side_effect = None
    if failure_site != "missing":
        vertex.stream_task = MagicMock()
        vertex.stream_task.ping.side_effect = failure if failure_site == "ping" else None
        get_side_effect = failure if failure_site == "get" else None

    with (
        patch.object(task_deployer.klein, "get", side_effect=get_side_effect),
        pytest.raises(DeploymentError) as caught,
    ):
        task_deployer.wait_job_vertex_created(target, timeout=1, vertices=(vertex,))

    assert caught.value.stage == "await operator readiness"
    if failure_site == "missing":
        assert "has not been created" in str(caught.value.cause)
    else:
        assert caught.value.cause is failure


@pytest.mark.parametrize("paused_operation_id", [None, "rescale-3"])
def test_start_job_vertex_uses_requested_start_protocol(paused_operation_id: str | None) -> None:
    target = _graph().job_vertex(2)
    selected = tuple(target.execution_vertices.values())
    references = []
    for index, vertex in target.execution_vertices.items():
        handle = MagicMock()
        method = handle.setup_and_run if paused_operation_id is None else handle.setup_for_rescale
        method.return_value = f"start-{index}"
        references.append(method.return_value)
        vertex.stream_task = handle
        vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
    selected[1].transition_to(ExecutionVertexStatus.FINISHED)

    with patch.object(task_deployer.klein, "get") as get:
        task_deployer.start_job_vertex(
            target,
            timeout=6,
            paused_operation_id=paused_operation_id,
        )

    get.assert_called_once_with(references, timeout=6)
    assert selected[0].status == ExecutionVertexStatus.RUNNING
    assert selected[1].status == ExecutionVertexStatus.FINISHED
    for vertex in selected:
        if paused_operation_id is None:
            vertex.stream_task.setup_and_run.assert_called_once_with()
            vertex.stream_task.setup_for_rescale.assert_not_called()
        else:
            vertex.stream_task.setup_for_rescale.assert_called_once_with(paused_operation_id)
            vertex.stream_task.setup_and_run.assert_not_called()


def test_start_job_vertex_wraps_setup_or_wait_failure() -> None:
    target = _graph().job_vertex(2)
    vertex = target.execution_vertex(0)
    vertex.stream_task = MagicMock()
    vertex.stream_task.setup_and_run.return_value = "start-ref"
    failure = TimeoutError("start timed out")

    with (
        patch.object(task_deployer.klein, "get", side_effect=failure),
        pytest.raises(DeploymentError) as caught,
    ):
        task_deployer.start_job_vertex(target, timeout=1, vertices=(vertex,))

    assert caught.value.stage == "start operator"
    assert caught.value.cause is failure
    assert vertex.status == ExecutionVertexStatus.CREATED


def test_cancel_created_tasks_continues_after_kill_failure_and_retains_failed_handle() -> None:
    target = _graph().job_vertex(2)
    handles = [object(), object()]
    for vertex, handle in zip(target.execution_vertices.values(), handles, strict=True):
        vertex.stream_task = handle

    with patch.object(task_deployer.klein, "kill", side_effect=[RuntimeError("gone"), None]) as kill:
        task_deployer._cancel_created_tasks(target)

    assert kill.call_args_list == [call(handle) for handle in handles]
    assert target.execution_vertex(0).stream_task is handles[0]
    assert target.execution_vertex(1).stream_task is None


def test_cancel_created_tasks_ignores_vertices_without_handles() -> None:
    target = _graph().job_vertex(2)
    with patch.object(task_deployer.klein, "kill") as kill:
        task_deployer._cancel_created_tasks(target)
    kill.assert_not_called()


def test_deploy_workers_transitions_every_actor() -> None:
    graph = _graph()
    for vertex in graph.execution_vertices:
        vertex.stream_task = object()

    task_deployer.deploy_workers(graph)

    assert {vertex.status for vertex in graph.execution_vertices} == {ExecutionVertexStatus.DEPLOYED}


def test_deploy_workers_fails_at_first_missing_actor_without_advancing_later_tasks() -> None:
    graph = _graph()
    first, missing, later = graph.execution_vertices[:3]
    first.stream_task = object()
    later.stream_task = object()

    with pytest.raises(DeploymentError, match="has not been created") as caught:
        task_deployer.deploy_workers(graph)

    assert caught.value.stage == "deploy workers"
    assert first.status == ExecutionVertexStatus.DEPLOYED
    assert missing.status == ExecutionVertexStatus.FAILED
    assert later.status == ExecutionVertexStatus.CREATED


def test_sink_first_levels_handles_shared_downstream_and_empty_graph() -> None:
    downstream = {1: (2, 3), 2: (4,), 3: (4,), 4: ()}
    graph = SimpleNamespace(
        job_vertices={vertex_id: object() for vertex_id in downstream},
        downstream_job_vertices=lambda vertex_id: downstream[vertex_id],
    )
    empty = SimpleNamespace(job_vertices={}, downstream_job_vertices=MagicMock())

    assert task_deployer._sink_first_levels(graph) == [[4], [2, 3], [1]]
    assert task_deployer._sink_first_levels(empty) == []


def test_start_workers_batches_each_sink_first_level_and_transitions_vertices() -> None:
    graph = _graph()
    setup_order = []
    refs_by_job_vertex = {}
    for job_vertex_id, job_vertex in graph.job_vertices.items():
        refs_by_job_vertex[job_vertex_id] = []
        for vertex in job_vertex.execution_vertices.values():
            vertex.stream_task = MagicMock()
            reference = f"start-{job_vertex_id}-{vertex.index}"
            refs_by_job_vertex[job_vertex_id].append(reference)
            vertex.stream_task.setup_and_run.side_effect = lambda ref=reference, job_id=job_vertex_id: (
                setup_order.append(job_id) or ref
            )
            vertex.transition_to(ExecutionVertexStatus.DEPLOYED)

    with patch.object(task_deployer.klein, "get") as get:
        task_deployer.start_workers(graph, timeout=8)

    assert setup_order == [3, 2, 2, 1, 1]
    assert get.call_args_list == [
        call(refs_by_job_vertex[3], timeout=8),
        call(refs_by_job_vertex[2], timeout=8),
        call(refs_by_job_vertex[1], timeout=8),
    ]
    assert {vertex.status for vertex in graph.execution_vertices} == {ExecutionVertexStatus.RUNNING}


def test_start_workers_wraps_wave_error_and_does_not_start_upstream() -> None:
    graph = _graph()
    for vertex in graph.execution_vertices:
        vertex.stream_task = MagicMock()
        vertex.stream_task.setup_and_run.return_value = vertex.name
        vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
    failure = RuntimeError("sink setup failed")

    with (
        patch.object(task_deployer.klein, "get", side_effect=failure),
        pytest.raises(DeploymentError) as caught,
    ):
        task_deployer.start_workers(graph, timeout=1)

    assert caught.value.stage == "start workers"
    assert caught.value.cause is failure
    for vertex in graph.job_vertex(3).execution_vertices.values():
        vertex.stream_task.setup_and_run.assert_called_once_with()
    for job_vertex_id in (1, 2):
        for vertex in graph.job_vertex(job_vertex_id).execution_vertices.values():
            vertex.stream_task.setup_and_run.assert_not_called()


def test_start_wave_does_not_overwrite_terminal_status() -> None:
    graph = _graph()
    sink = graph.job_vertex(3)
    vertex = sink.execution_vertex(0)
    vertex.stream_task = MagicMock()
    vertex.stream_task.setup_and_run.return_value = "start-ref"
    vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
    vertex.transition_to(ExecutionVertexStatus.FINISHED)

    with patch.object(task_deployer.klein, "get", return_value=None):
        task_deployer._start_wave([sink], timeout=2)

    assert vertex.status == ExecutionVertexStatus.FINISHED


def test_bootstrap_vertex_refreshes_descriptor_and_waits_without_status_transition() -> None:
    graph = _graph()
    vertex = graph.job_vertex(2).execution_vertex(1)
    vertex.restore_operation_id = "restore-bootstrap"
    vertex.stream_task = MagicMock()
    vertex.stream_task.setup_and_run_with_descriptor.return_value = "bootstrap-ref"

    with (
        patch.object(task_deployer, "build_descriptor", return_value=object()) as build,
        patch.object(task_deployer.klein, "get") as get,
    ):
        task_deployer.bootstrap_vertex(graph, vertex, timeout=9)

    descriptor = build.return_value
    build.assert_called_once_with(
        graph,
        graph.job_vertex(2),
        vertex,
        restore_operation_id="restore-bootstrap",
    )
    vertex.stream_task.setup_and_run_with_descriptor.assert_called_once_with(descriptor)
    get.assert_called_once_with("bootstrap-ref", timeout=9)
    assert vertex.status == ExecutionVertexStatus.CREATED
