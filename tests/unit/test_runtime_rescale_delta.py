# SPDX-License-Identifier: Apache-2.0
"""Focused contracts for delta actor lifecycle during operator rescaling."""

from unittest.mock import MagicMock, call, patch

import pytest

from ray.klein.api.node_type import NodeType
from ray.klein.config.configuration import Configuration
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
from ray.klein.runtime.partitioning import RoundRobinPartitioner
from ray.klein.runtime.resources import Resources
from ray.klein.runtime.scheduler import job_master as job_master_module
from ray.klein.runtime.scheduler import task_deployer, task_terminator
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.scheduler.placement import NativeStrategy


def _vertex(index: int, node_type: NodeType, parallelism: int) -> VertexSpec:
    operator_type = OperatorType.SOURCE if node_type == NodeType.SOURCE else OperatorType.ONE_INPUT
    return VertexSpec(
        VertexId("job", index),
        f"op{index}",
        OperatorSpec(StreamOperator, None, index, f"op{index}", operator_type),
        node_type,
        Resources(num_cpus=0, concurrency=parallelism),
    )


def _graphs(target_parallelism: int):
    config = Configuration(include_environment=False)
    builder = LogicalGraphBuilder("job", config)
    source = _vertex(1, NodeType.SOURCE, 1)
    target = _vertex(2, NodeType.TRANSFORM, target_parallelism)
    sink = _vertex(3, NodeType.SINK, 1)
    for vertex in (source, target, sink):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, target.id, RoundRobinPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(target.id, sink.id, RoundRobinPartitioner().to_spec()))
    logical = builder.build()
    physical = ExecutionGraph.expand(logical, config, JobMetricGroup("job"), "job")
    return logical, physical


def _mark_target_running(graph: ExecutionGraph) -> tuple[MagicMock, ...]:
    handles = tuple(MagicMock(name=f"task-{index}") for index in graph.job_vertex(2).execution_vertices)
    for vertex, handle in zip(graph.job_vertex(2).execution_vertices.values(), handles, strict=True):
        vertex.stream_task = handle
        vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
        vertex.transition_to(ExecutionVertexStatus.RUNNING)
    return handles


def test_scale_out_rebinds_overlap_without_mutating_old_graph() -> None:
    logical, old_graph = _graphs(2)
    handles = _mark_target_running(old_graph)
    old_target = old_graph.job_vertex(2)

    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 4), 2)
    new_target = new_graph.job_vertex(2)

    assert set(new_target.execution_vertices) - set(old_target.execution_vertices) == {2, 3}
    assert set(old_target.execution_vertices) - set(new_target.execution_vertices) == set()
    for index in (0, 1):
        old = old_target.execution_vertex(index)
        retained = new_target.execution_vertex(index)
        assert retained is not old
        assert retained.id == old.id
        assert retained.name == old.name
        assert retained.stream_task is handles[index]
        assert retained.task_generation == old.task_generation
        assert retained.status == old.status == ExecutionVertexStatus.RUNNING
        assert retained.task_metric_group is old.task_metric_group
        assert retained.concurrency == 4
        assert old.concurrency == 2
        assert retained.resources.concurrency == 4
        assert old.resources.concurrency == 2

    for index in (2, 3):
        added = new_target.execution_vertex(index)
        assert added.stream_task is None
        assert added.status == ExecutionVertexStatus.CREATED
        assert added.concurrency == 4

    descriptor = task_deployer.build_descriptor(new_graph, new_target, new_target.execution_vertex(0))
    assert descriptor.parallelism == 4

    new_target.execution_vertex(0).transition_to(ExecutionVertexStatus.CANCELLED)
    assert old_target.execution_vertex(0).status == ExecutionVertexStatus.RUNNING


def test_scale_in_rebinds_overlap_and_leaves_removed_vertices_on_old_graph() -> None:
    logical, old_graph = _graphs(4)
    handles = _mark_target_running(old_graph)
    old_target = old_graph.job_vertex(2)

    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 2), 2)
    new_target = new_graph.job_vertex(2)

    assert set(new_target.execution_vertices) - set(old_target.execution_vertices) == set()
    assert set(old_target.execution_vertices) - set(new_target.execution_vertices) == {2, 3}
    for index in (0, 1):
        old = old_target.execution_vertex(index)
        retained = new_target.execution_vertex(index)
        assert retained is not old
        assert retained.stream_task is handles[index]
        assert retained.name == old.name
        assert retained.task_generation == old.task_generation
        assert retained.task_metric_group is old.task_metric_group
        assert retained.concurrency == 2
        assert old.concurrency == 4
    for index in (2, 3):
        assert old_target.execution_vertex(index).stream_task is handles[index]
        assert old_target.execution_vertex(index).status == ExecutionVertexStatus.RUNNING


def test_deployer_subset_creation_readiness_deploy_and_start() -> None:
    logical, old_graph = _graphs(2)
    retained_handles = _mark_target_running(old_graph)
    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 4), 2)
    target = new_graph.job_vertex(2)
    added = tuple(target.execution_vertex(index) for index in (2, 3))
    added_handles = (MagicMock(name="added-2"), MagicMock(name="added-3"))

    with patch.object(task_deployer, "create_remote_actor", side_effect=added_handles) as create:
        task_deployer.instantiate_job_vertex(
            new_graph,
            target,
            NativeStrategy().plan(new_graph),
            restore_operation_id="resize-1",
            vertices=added,
        )

    assert create.call_count == 2
    assert tuple(vertex.stream_task for vertex in added) == added_handles
    assert tuple(target.execution_vertex(index).stream_task for index in (0, 1)) == retained_handles

    with patch.object(task_deployer.klein, "get", side_effect=lambda value, **_kwargs: value) as get:
        task_deployer.wait_job_vertex_created(target, timeout=7, vertices=added)
        readiness_call = get.call_args
        task_deployer.deploy_job_vertex(target, vertices=added)
        task_deployer.start_job_vertex(
            target,
            timeout=11,
            paused_operation_id="resize-1",
            vertices=added,
        )

    assert readiness_call.kwargs == {"timeout": 7}
    for handle in added_handles:
        handle.ping.assert_called_once_with()
        handle.setup_for_rescale.assert_called_once_with("resize-1")
    for vertex in added:
        assert vertex.status == ExecutionVertexStatus.RUNNING
        assert vertex.restore_operation_id == "resize-1"
    for handle in retained_handles:
        handle.ping.assert_not_called()
        handle.setup_for_rescale.assert_not_called()


def test_terminator_stops_only_explicit_vertex_subset() -> None:
    _logical, graph = _graphs(4)
    handles = _mark_target_running(graph)
    target = graph.job_vertex(2)
    removed = tuple(target.execution_vertex(index) for index in (2, 3))

    with (
        patch.object(task_terminator.klein, "get", side_effect=lambda value, **_kwargs: value),
        patch.object(
            task_terminator.klein, "get_actor_status", return_value=task_terminator.StreamTaskStatus.NOT_EXIST
        ),
        patch.object(task_terminator.klein, "kill") as kill,
    ):
        task_terminator.stop_job_vertex(
            target,
            graph.namespace,
            timeout=5,
            force=True,
            vertices=removed,
        )

    assert kill.call_args_list == [call(handles[2]), call(handles[3])]
    for index in (0, 1):
        retained = target.execution_vertex(index)
        assert retained.stream_task is handles[index]
        assert retained.status == ExecutionVertexStatus.RUNNING
    for vertex in removed:
        assert vertex.stream_task is None
        assert vertex.status == ExecutionVertexStatus.CANCELLED


def test_terminator_does_not_forget_an_actor_that_survives_every_kill() -> None:
    _logical, graph = _graphs(2)
    handles = _mark_target_running(graph)
    target = graph.job_vertex(2)
    removed = (target.execution_vertex(1),)

    with (
        patch.object(task_terminator.klein, "get", side_effect=lambda value, **_kwargs: value),
        patch.object(
            task_terminator.klein,
            "get_actor_status",
            return_value=task_terminator.StreamTaskStatus.ALIVE,
        ),
        patch.object(task_terminator.klein, "kill"),
        patch.object(task_terminator.klein, "kill_actor_by_name") as kill_by_name,
        patch.object(task_terminator, "_KILL_ACTOR_MAX_RETRIES", 2),
        patch.object(task_terminator, "_KILL_ACTOR_RETRY_DELAY", 0),
        pytest.raises(RuntimeError, match="failed to stop operator actor"),
    ):
        task_terminator.stop_job_vertex(
            target,
            graph.namespace,
            timeout=5,
            force=True,
            vertices=removed,
        )

    assert kill_by_name.call_count == 2
    assert removed[0].stream_task is handles[1]
    assert removed[0].status == ExecutionVertexStatus.RUNNING


def test_terminator_retries_a_dead_named_actor_until_it_no_longer_exists() -> None:
    with (
        patch.object(
            task_terminator.klein,
            "get_actor_status",
            side_effect=[task_terminator.StreamTaskStatus.DEAD, task_terminator.StreamTaskStatus.NOT_EXIST],
        ),
        patch.object(task_terminator.klein, "kill_actor_by_name") as kill_by_name,
        patch.object(task_terminator, "_KILL_ACTOR_RETRY_DELAY", 0),
    ):
        assert task_terminator._kill_actor_with_retry("retired", "job") is True

    assert kill_by_name.call_count == 2


def test_job_master_prewarms_only_added_actors_before_the_rescale_barrier() -> None:
    logical, old_graph = _graphs(2)
    _mark_target_running(old_graph)
    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 3), 2)
    old_target = old_graph.job_vertex(2)
    new_target = new_graph.job_vertex(2)
    master = JobMaster(old_graph, Configuration(include_environment=False))
    master.coordinator = MagicMock()
    events: list[str] = []
    master.coordinator.begin_operator_rescale.side_effect = lambda *_args: events.append("checkpoint-gate") or True
    master.coordinator.reconfigure_execution_graph.side_effect = lambda *_args: (
        events.append("coordinator-topology") or True
    )
    master._recovery = MagicMock()
    master._recovery.clear_stable_rescale_metadata.side_effect = lambda: events.append("retire-old-identity") or True
    attempt = job_master_module._LocalRescaleAttempt()

    with (
        patch.object(job_master_module.klein, "get", side_effect=lambda value, **_kwargs: value),
        patch.object(
            task_deployer,
            "instantiate_job_vertex",
            side_effect=lambda *_args, **_kwargs: events.append("instantiate-added"),
        ) as instantiate,
        patch.object(
            task_deployer,
            "deploy_job_vertex",
            side_effect=lambda *_args, **_kwargs: events.append("deploy-added"),
        ) as deploy,
        patch.object(
            task_deployer,
            "wait_job_vertex_created",
            side_effect=lambda *_args, **_kwargs: events.append("ping-added"),
        ) as wait_created,
        patch.object(
            task_deployer,
            "start_job_vertex",
            side_effect=lambda *_args, **_kwargs: events.append("start-added"),
        ) as start,
        patch.object(
            master,
            "_prepare_local_rescale",
            side_effect=lambda *_args: events.append("rescale-barrier") or ([], []),
        ),
        patch.object(
            master,
            "_await_local_rescale_cut",
            side_effect=lambda *_args: events.append("aligned-cut") or [None, None],
        ),
        patch.object(
            master,
            "_stage_local_rescale_state",
            side_effect=lambda *_args: events.append("stage-state"),
        ),
        patch.object(
            master,
            "_prepare_retained_target_runtimes",
            side_effect=lambda *_args: events.append("prepare-retained"),
        ),
        patch.object(
            master,
            "_prepare_live_task_topologies",
            side_effect=lambda *_args, **_kwargs: events.append("prepare-routes") or [],
        ),
        patch.object(
            master,
            "_activate_live_task_topologies",
            side_effect=lambda *_args: events.append("activate-routes"),
        ),
        patch.object(
            master,
            "_commit_live_task_topologies",
            side_effect=lambda *_args: events.append("commit-routes"),
        ),
        patch.object(
            master,
            "_replace_recovery_graph",
            side_effect=lambda *_args: events.append("replace-recovery-graph"),
        ),
        patch.object(
            master,
            "_commit_retained_target_runtimes",
            side_effect=lambda *_args: events.append("commit-retained"),
        ),
        patch.object(
            master,
            "_finish_local_rescale_gate",
            side_effect=lambda *_args, **_kwargs: events.append("finish-gate"),
        ),
        patch.object(
            master,
            "_release_committed_rescale",
            side_effect=lambda *_args: events.append("resume-new-topology"),
        ),
        patch.object(
            master,
            "_request_rescale_stabilization_checkpoint",
            side_effect=lambda *_args: events.append("stabilization-checkpoint"),
        ),
    ):
        master._apply_local_rescale(
            new_graph,
            "resize-1",
            2,
            old_graph,
            old_target,
            new_target,
            10,
            attempt,
        )

    assert events == [
        "instantiate-added",
        "deploy-added",
        "ping-added",
        "checkpoint-gate",
        "retire-old-identity",
        "rescale-barrier",
        "aligned-cut",
        "stage-state",
        "start-added",
        "prepare-retained",
        "prepare-routes",
        "activate-routes",
        "coordinator-topology",
        "commit-routes",
        "replace-recovery-graph",
        "commit-retained",
        "finish-gate",
        "resume-new-topology",
        "stabilization-checkpoint",
    ]
    added = (new_target.execution_vertex(2),)
    assert instantiate.call_args.kwargs["vertices"] == added
    assert deploy.call_args.kwargs["vertices"] == added
    assert wait_created.call_args.kwargs["vertices"] == added
    assert start.call_args.kwargs["vertices"] == added
    assert attempt.committed is True


def test_job_master_scale_in_creates_nothing_and_stops_only_removed_actors() -> None:
    logical, old_graph = _graphs(4)
    _mark_target_running(old_graph)
    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 2), 2)
    old_target = old_graph.job_vertex(2)
    new_target = new_graph.job_vertex(2)
    master = JobMaster(old_graph, Configuration(include_environment=False))
    master.coordinator = MagicMock()
    master.coordinator.begin_operator_rescale.return_value = True
    master.coordinator.reconfigure_execution_graph.return_value = True
    master._recovery = MagicMock()
    master._recovery.clear_stable_rescale_metadata.return_value = True

    with (
        patch.object(job_master_module.klein, "get", side_effect=lambda value, **_kwargs: value),
        patch.object(task_deployer, "instantiate_job_vertex") as instantiate,
        patch.object(task_deployer, "deploy_job_vertex") as deploy,
        patch.object(task_deployer, "wait_job_vertex_created") as wait_created,
        patch.object(task_deployer, "start_job_vertex") as start,
        patch.object(master, "_prepare_local_rescale", return_value=([], [])),
        patch.object(master, "_await_local_rescale_cut", return_value=[None] * 4),
        patch.object(master, "_stage_local_rescale_state"),
        patch.object(master, "_prepare_retained_target_runtimes"),
        patch.object(master, "_prepare_live_task_topologies", return_value=[]),
        patch.object(master, "_activate_live_task_topologies"),
        patch.object(master, "_commit_live_task_topologies"),
        patch.object(master, "_replace_recovery_graph"),
        patch.object(master, "_commit_retained_target_runtimes"),
        patch.object(master, "_finish_local_rescale_gate"),
        patch.object(master, "_release_committed_rescale"),
        patch.object(master, "_request_rescale_stabilization_checkpoint"),
        patch.object(task_terminator, "stop_job_vertex") as stop,
    ):
        master._apply_local_rescale(
            new_graph,
            "resize-1",
            2,
            old_graph,
            old_target,
            new_target,
            10,
            job_master_module._LocalRescaleAttempt(),
        )

    instantiate.assert_not_called()
    deploy.assert_not_called()
    wait_created.assert_not_called()
    start.assert_not_called()
    assert tuple(vertex.index for vertex in stop.call_args.kwargs["vertices"]) == (2, 3)


def test_postcommit_recovery_graph_failure_is_fenced_and_cleans_removed_actors() -> None:
    logical, old_graph = _graphs(4)
    _mark_target_running(old_graph)
    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 2), 2)
    old_target = old_graph.job_vertex(2)
    new_target = new_graph.job_vertex(2)
    master = JobMaster(old_graph, Configuration(include_environment=False))
    master.coordinator = MagicMock()
    master.coordinator.begin_operator_rescale.return_value = True
    master.coordinator.reconfigure_execution_graph.return_value = True
    master._recovery = MagicMock()
    master._recovery.clear_stable_rescale_metadata.return_value = True
    attempt = job_master_module._LocalRescaleAttempt()

    with (
        patch.object(job_master_module.klein, "get", side_effect=lambda value, **_kwargs: value),
        patch.object(master, "_prepare_local_rescale", return_value=([], [])),
        patch.object(master, "_await_local_rescale_cut", return_value=[None] * 4),
        patch.object(master, "_stage_local_rescale_state"),
        patch.object(master, "_prepare_retained_target_runtimes"),
        patch.object(master, "_prepare_live_task_topologies", return_value=[]),
        patch.object(master, "_activate_live_task_topologies"),
        patch.object(master, "_commit_live_task_topologies"),
        patch.object(master, "_replace_recovery_graph", side_effect=RuntimeError("recovery install failed")),
        patch.object(master, "_commit_retained_target_runtimes") as commit_retained,
        patch.object(master, "_finish_local_rescale_gate") as finish_gate,
        patch.object(master, "_release_committed_rescale") as release,
        patch.object(master, "_request_rescale_stabilization_checkpoint"),
        patch.object(task_terminator, "stop_job_vertex") as stop,
        pytest.raises(RuntimeError, match="recovery install failed"),
    ):
        master._apply_local_rescale(
            new_graph,
            "resize-1",
            2,
            old_graph,
            old_target,
            new_target,
            10,
            attempt,
        )

    assert attempt.committed is True
    master._recovery.require_global_recovery.assert_called_once()
    finish_gate.assert_called_once_with("resize-1", committed=True)
    commit_retained.assert_not_called()
    release.assert_not_called()
    assert tuple(vertex.index for vertex in stop.call_args.kwargs["vertices"]) == (2, 3)


def test_committed_release_rejects_a_false_resume_response() -> None:
    logical, old_graph = _graphs(2)
    _mark_target_running(old_graph)
    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 3), 2)
    new_target = new_graph.job_vertex(2)
    new_target.execution_vertex(2).stream_task = MagicMock()

    with (
        patch.object(job_master_module.klein, "get", return_value=[False]),
        pytest.raises(RuntimeError, match="did not resume"),
    ):
        JobMaster._release_committed_rescale(
            old_graph,
            new_target,
            2,
            "resize-1",
            10,
        )


def test_failed_candidate_cleanup_stays_registered_until_a_later_success() -> None:
    logical, old_graph = _graphs(2)
    new_graph = old_graph.rescale_operator(logical.rescale_operator(2, 3), 2)
    new_target = new_graph.job_vertex(2)
    added = new_target.execution_vertex(2)
    added.stream_task = MagicMock()
    master = JobMaster(old_graph, Configuration(include_environment=False))
    attempt = job_master_module._LocalRescaleAttempt(candidate_created=True)

    with (
        patch.object(
            task_terminator,
            "stop_job_vertex",
            side_effect=[RuntimeError("kill failed"), None],
        ) as stop,
        patch.object(master, "_replace_recovery_graph"),
        patch.object(master, "_discard_local_rescale_state"),
    ):
        restored = master._restore_precommit_runtime(
            new_graph,
            "resize-1",
            2,
            old_graph,
            new_target,
            10,
            attempt,
        )
        assert restored is False
        assert set(master._pending_rescale_actor_cleanup) == {added.name}

        master._cleanup_pending_rescale_actors(10)

    assert stop.call_count == 2
    assert master._pending_rescale_actor_cleanup == {}
