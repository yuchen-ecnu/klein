# SPDX-License-Identifier: Apache-2.0
"""Focused contracts for local single-operator runtime rescaling."""

import asyncio
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from ray.klein.api.job_status import JobStatus
from ray.klein.api.node_type import NodeType
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.collector.edge_output import EdgeOutput
from ray.klein.runtime.collector.task_output import TaskOutput
from ray.klein.runtime.coordinator import checkpoint_coordinator as checkpoint_coordinator_module
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.job_manager.job_manager import JobManager
from ray.klein.runtime.message import Barrier, Record, RescaleBarrier
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.partitioning import ForwardPartitioner, RoundRobinPartitioner
from ray.klein.runtime.resources import Resources
from ray.klein.runtime.scheduler import job_master as job_master_module
from ray.klein.runtime.scheduler import recovery_manager as recovery_manager_module
from ray.klein.runtime.scheduler import task_deployer, task_terminator
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.worker.source_stream_task import SourceStreamTask
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry
from ray.klein.state.state_snapshot_reference import StateSnapshotReference


def _vertex(job: str, index: int, node_type: NodeType, parallelism: int) -> VertexSpec:
    operator_type = OperatorType.SOURCE if node_type == NodeType.SOURCE else OperatorType.ONE_INPUT
    return VertexSpec(
        VertexId(job, index),
        f"op{index}",
        OperatorSpec(StreamOperator, None, index, f"op{index}", operator_type),
        node_type,
        Resources(num_cpus=0, concurrency=parallelism),
    )


def _graphs(parallelism: int = 2, source_parallelism: int = 1):
    config = Configuration(include_environment=False)
    builder = LogicalGraphBuilder("job", config)
    source = _vertex("job", 1, NodeType.SOURCE, source_parallelism)
    target = _vertex("job", 2, NodeType.TRANSFORM, parallelism)
    sink = _vertex("job", 3, NodeType.SINK, 2)
    for vertex in (source, target, sink):
        builder.add_vertex(vertex)
    source_partitioner = ForwardPartitioner() if source_parallelism == parallelism else RoundRobinPartitioner()
    builder.add_edge(EdgeSpec(source.id, target.id, source_partitioner.to_spec()))
    builder.add_edge(EdgeSpec(target.id, sink.id, ForwardPartitioner().to_spec()))
    logical = builder.build()
    physical = ExecutionGraph.expand(logical, config, JobMetricGroup("job"), "job")
    return logical, physical


def _mark_running(graph: ExecutionGraph) -> None:
    for vertex in graph.execution_vertices:
        vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
        vertex.transition_to(ExecutionVertexStatus.RUNNING)


def test_physical_rescale_replaces_only_target_and_epochs_incident_edges() -> None:
    logical, physical = _graphs()
    source_handle = object()
    sink_handle = object()
    physical.job_vertex(1).execution_vertex(0).stream_task = source_handle
    physical.job_vertex(3).execution_vertex(0).stream_task = sink_handle

    resized_logical = logical.rescale_operator(2, 4)
    resized = physical.rescale_operator(resized_logical, 2)
    resized.mark_rescale_epoch(2, "resize-1")

    assert resized.job_vertex(1) is physical.job_vertex(1)
    assert resized.job_vertex(3) is physical.job_vertex(3)
    assert resized.job_vertex(2) is not physical.job_vertex(2)
    assert resized.job_vertex(2).concurrency == 4
    assert resized.job_vertex(1).execution_vertex(0).stream_task is source_handle
    assert resized.job_vertex(3).execution_vertex(0).stream_task is sink_handle
    assert resized.topology_epoch(1, 2) == "resize-1"
    assert resized.topology_epoch(2, 3) == "resize-1"


@pytest.mark.asyncio
async def test_checkpoint_gate_and_transient_state_do_not_pollute_global_checkpoint() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    await coordinator.begin_operator_rescale("resize-1", timeout=1)

    registration = coordinator.register_checkpoint(ExecutionVertexId(1, 0))
    payload = b"managed-state"
    reference = StateSnapshotReference(
        len(payload),
        f"sha256:{hashlib.sha256(payload).hexdigest()}",
        inline_payload=payload,
    )
    await coordinator.stage_operator_rescale_state("resize-1", 2, (reference,))

    assert registration.barrier_id is None
    assert "paused" in registration.reason
    assert coordinator.operator_rescale_states("resize-1", ExecutionVertexId(2, 9)) == (reference,)
    assert coordinator._latest_operator_states == {}
    assert coordinator._restored_operator_states == {}
    assert coordinator.finish_operator_rescale("resize-1") is True


@pytest.mark.asyncio
async def test_checkpoint_gate_waits_for_old_topology_inflight() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._inflight_checkpoints[1] = Mock()
    waiting = asyncio.create_task(coordinator.begin_operator_rescale("resize-1", timeout=1))
    await asyncio.sleep(0)
    assert not waiting.done()
    coordinator._inflight_checkpoints.clear()
    assert await waiting is True
    coordinator.finish_operator_rescale("resize-1")


@pytest.mark.asyncio
async def test_checkpoint_gate_waits_for_aligned_checkpoint_completion() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._completing_checkpoints.add(1)
    waiting = asyncio.create_task(coordinator.begin_operator_rescale("resize-1", timeout=1))
    await asyncio.sleep(0)
    assert not waiting.done()
    coordinator._completing_checkpoints.clear()
    assert await waiting is True
    coordinator.finish_operator_rescale("resize-1")


@pytest.mark.asyncio
async def test_next_rescale_waits_for_the_stabilization_checkpoint() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._rescale_recovery_fence = "resize-1"
    waiting = asyncio.create_task(coordinator.begin_operator_rescale("resize-2", timeout=1))
    await asyncio.sleep(0)
    assert not waiting.done()

    coordinator._rescale_recovery_fence = None
    assert await waiting is True
    coordinator.finish_operator_rescale("resize-2")


def test_recovery_fence_requires_durable_state_from_every_source() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = {first, second}

    coordinator._state_revision = 7
    assert coordinator._record_rescale_stabilization_progress(first) is False
    assert coordinator.operator_rescale_recovery_fenced() is True
    assert coordinator._record_rescale_stabilization_progress(second) is True
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is True

    coordinator._persisted_state_revision = 7
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is False


@pytest.mark.asyncio
async def test_rescale_restore_falls_back_after_transient_state_is_superseded() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._ensure_locks()
    vertex_id = ExecutionVertexId(2, 0)
    payload = b"managed-state"
    reference = StateSnapshotReference(
        len(payload),
        f"sha256:{hashlib.sha256(payload).hexdigest()}",
        inline_payload=payload,
    )
    await coordinator.begin_operator_rescale("resize-1", timeout=1)

    with pytest.raises(RuntimeError, match=r"active operator rescale.*unavailable"):
        await coordinator.restore_operator_rescale_states("resize-1", vertex_id)

    await coordinator.stage_operator_rescale_state("resize-1", 2, (reference,))
    assert await coordinator.restore_operator_rescale_states("resize-1", vertex_id) == (reference,)
    coordinator.finish_operator_rescale("resize-1")
    coordinator._replace_logical_operator_states({"2:0": reference})

    assert await coordinator.restore_operator_rescale_states("resize-1", vertex_id) == (reference,)

    restarted = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    restarted._ensure_locks()
    restarted._latest_operator_states["2:0"] = reference
    with pytest.raises(RuntimeError, match=r"global checkpoint restore"):
        await restarted.restore_operator_rescale_states("resize-1", vertex_id)


@pytest.mark.asyncio
async def test_committed_rescale_recovery_fence_clears_after_state_is_superseded() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    source_id = ExecutionVertexId(1, 0)
    coordinator._execution_graph = SimpleNamespace(
        source_execution_vertices=[SimpleNamespace(id=source_id)],
    )
    payload = b"managed-state"
    reference = StateSnapshotReference(
        len(payload),
        f"sha256:{hashlib.sha256(payload).hexdigest()}",
        inline_payload=payload,
    )
    await coordinator.begin_operator_rescale("resize-1", timeout=1)
    await coordinator.stage_operator_rescale_state("resize-1", 2, (reference,))

    assert coordinator.finish_operator_rescale("resize-1", committed=True) is True
    assert coordinator.operator_rescale_recovery_fenced() is True
    coordinator._replace_logical_operator_states({"2:0": reference})
    assert coordinator._record_rescale_stabilization_progress(source_id) is True
    coordinator._clear_rescale_recovery_fence_if_durable()

    assert coordinator.operator_rescale_recovery_fenced() is True
    coordinator._persisted_state_revision = coordinator._state_revision
    coordinator._clear_rescale_recovery_fence_if_durable()

    assert coordinator.operator_rescale_recovery_fenced() is False


@pytest.mark.asyncio
async def test_stabilization_fence_survives_persistence_failure(tmp_path) -> None:
    config = Configuration(include_environment=False)
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    coordinator = CheckpointCoordinator(config, job_id="job")
    coordinator._ensure_locks()
    source_id = ExecutionVertexId(1, 0)
    coordinator._execution_graph = SimpleNamespace(source_execution_vertices=[])
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = {source_id}
    coordinator._latest_source_states["1:0"] = SourceCheckpointEntry(
        task_key="1:0",
        checkpoint_id=7,
        state={"offset": 7},
    )
    coordinator._state_revision = 1
    assert coordinator._record_rescale_stabilization_progress(source_id) is True

    with (
        patch.object(
            checkpoint_coordinator_module.checkpoint_io,
            "write_checkpoint",
            side_effect=OSError("storage unavailable"),
        ),
        pytest.raises(RuntimeError, match="persist checkpoint metadata"),
    ):
        await coordinator._persist_checkpoint_metadata(notify_sources=False, strict=True)

    assert coordinator.operator_rescale_recovery_fenced() is True
    assert await coordinator._persist_checkpoint_metadata(notify_sources=False, strict=True) is True
    assert coordinator.operator_rescale_recovery_fenced() is False


@pytest.mark.asyncio
async def test_job_manager_noop_rejection_success_and_failed_swap_preserve_committed_graph() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock()
    manager._job_config = Configuration(include_environment=False)
    manager._job_config.set(StateOptions.MAX_PARALLELISM, 2)
    manager._job_status = JobStatus.RUNNING

    stateful = SimpleNamespace(
        operator=SimpleNamespace(
            stateful=True,
            source=False,
            transactional_sink=False,
            collecting=False,
        )
    )
    assert manager._unsupported_rescale_reason(stateful, 3) == ("parallelism 3 exceeds state.keyed.max-parallelism=2")

    assert (await manager.rescale_operator(2, 2))["status"] == "NOOP"
    manager._rescale_in_progress = True
    assert (await manager.rescale_operator(2, 3))["status"] == "REJECTED"
    manager._rescale_in_progress = False

    manager.run_exclusive = AsyncMock(return_value=None)
    completed = await manager.rescale_operator("2", 3)
    assert completed["status"] == "COMPLETED"
    assert manager.execution_graph.job_vertex(1) is physical.job_vertex(1)
    committed = manager.execution_graph
    for vertex in committed.execution_vertices:
        if vertex.status == ExecutionVertexStatus.CREATED:
            vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
            vertex.transition_to(ExecutionVertexStatus.RUNNING)

    manager.run_exclusive = AsyncMock(side_effect=RuntimeError("swap failed"))
    failed = await manager.rescale_operator(2, 4)
    assert failed["status"] == "FAILED"
    assert manager.execution_graph is committed
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_job_manager_adopts_a_topology_that_failed_after_commit() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    resized_logical = logical.rescale_operator(2, 3)
    resized_graph = physical.rescale_operator(resized_logical, 2)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(execution_graph=physical)

    def fail_after_commit(*_args):
        manager.job_master.execution_graph = resized_graph
        raise RuntimeError("checkpoint gate response was lost")

    manager.run_exclusive = AsyncMock(side_effect=fail_after_commit)
    error = await manager._run_operator_rescale(resized_graph, resized_logical)

    assert "checkpoint gate response was lost" in error
    assert manager.logical_graph is resized_logical
    assert manager.execution_graph is resized_graph
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_local_rescale_rejects_jobs_with_multiple_physical_sources() -> None:
    logical, physical = _graphs(source_parallelism=2)
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock()
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING

    result = await manager.rescale_operator(2, 3)

    assert result["status"] == "REJECTED"
    assert "exactly one physical source task" in result["error"]
    resized_logical = logical.rescale_operator(2, 3)
    resized_graph = physical.rescale_operator(resized_logical, 2)
    master = JobMaster(physical, Configuration(include_environment=False))
    with pytest.raises(ValueError, match="exactly one physical source task"):
        master._validate_local_rescale(resized_graph)
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_cancel_waits_for_the_local_rescale_lifecycle_transaction() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock()
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _blocked_writer(*_args):
        entered.set()
        await release.wait()

    manager.run_exclusive = AsyncMock(side_effect=_blocked_writer)
    manager._stop_job = AsyncMock()
    rescale = asyncio.create_task(manager.rescale_operator(2, 3))
    await entered.wait()
    cancel = asyncio.create_task(manager.cancel(timeout=1))
    await asyncio.sleep(0)
    manager._stop_job.assert_not_awaited()

    release.set()
    assert (await rescale)["status"] == "COMPLETED"
    assert await cancel is True
    manager._stop_job.assert_awaited_once_with(force=True, timeout=1)
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_reused_task_name_rejects_a_stale_generation_status_report() -> None:
    logical_two, first_graph = _graphs(parallelism=2)
    first_vertex = first_graph.job_vertex(2).execution_vertex(0)
    logical_four = logical_two.rescale_operator(2, 4)
    graph_four = first_graph.rescale_operator(logical_four, 2)
    logical_two_again = logical_four.rescale_operator(2, 2)
    current_graph = graph_four.rescale_operator(logical_two_again, 2)
    current_vertex = current_graph.job_vertex(2).execution_vertex(0)

    assert current_vertex.id == first_vertex.id
    assert current_vertex.name == first_vertex.name
    assert current_vertex.task_generation != first_vertex.task_generation

    master = JobMaster(current_graph, Configuration(include_environment=False))
    accepted = master.on_task_status_report(
        current_vertex.id,
        ExecutionVertexStatus.FAILED,
        "late failure from generation one",
        first_vertex.name,
        first_vertex.task_generation,
    )
    assert accepted is False
    assert current_vertex.status == ExecutionVertexStatus.CREATED

    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.execution_graph = current_graph
    manager.job_master = master
    manager.run_exclusive = AsyncMock(return_value=(False, False, current_vertex.name))

    await manager.update_stream_task_status(
        current_vertex.id,
        ExecutionVertexStatus.FAILED,
        "late failure from generation one",
        task_name=first_vertex.name,
        task_generation=first_vertex.task_generation,
    )

    manager.run_exclusive.assert_awaited_once()
    assert manager._task_failure_details == {}
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_downstream_fence_advances_durability_boundary_and_pauses() -> None:
    task = object.__new__(StreamTask)
    task._task_name = "downstream"
    task._vertex_id = ExecutionVertexId(3, 0)
    task._rescale_operation_id = None
    task._rescale_role = None
    task._rescale_expected_senders = set()
    task._rescale_seen_senders = set()
    task._rescale_edge_indices = ()
    task._rescale_ready_obj = None
    task._rescale_resume_obj = None
    task._rescale_snapshot = None
    task._rescale_tombstones = []
    executor = ThreadPoolExecutor(max_workers=1)
    task._state = SimpleNamespace(async_runner=None, executor=executor)
    task._pump = Mock(flush_input=Mock())
    task._emit = AsyncMock()
    task._watermark = AsyncMock()
    sender = ExecutionVertexId(2, 0)
    task._begin_rescale("resize-1", "downstream")
    task._rescale_expected_senders = {sender}

    await task.handle_rescale_barrier(RescaleBarrier("resize-1", 2), sender)

    assert task._rescale_ready.is_set()
    task._watermark.advance.assert_awaited_once_with()
    task._emit.wait_idle.assert_awaited()
    executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_stateful_candidate_rejects_missing_transient_rescale_state() -> None:
    task = object.__new__(StreamTask)
    task._descriptor = SimpleNamespace(restore_operation_id="resize-1")
    task._state = SimpleNamespace(
        operator=SimpleNamespace(stateful=True),
        state_snapshot_cache=Mock(),
    )
    strategy = SimpleNamespace(
        restore_rescale_operator_states_async=AsyncMock(return_value=()),
    )

    with pytest.raises(RuntimeError, match=r"managed state.*resize-1.*unavailable"):
        await task._restore_operator_state(SimpleNamespace(checkpoint_strategy=strategy))


def test_replace_edges_preserves_unaffected_delivery_journal() -> None:
    keep = MagicMock(spec=EdgeOutput)
    replace = MagicMock(spec=EdgeOutput)
    replacement = MagicMock(spec=EdgeOutput)
    output = TaskOutput([keep, replace])
    context = SimpleNamespace(metric_group=None)
    output.open(context)

    output.replace_edges([None, replacement])

    keep.ensure_quiescent.assert_not_called()
    keep.close.assert_not_called()
    replace.ensure_quiescent.assert_called_once_with()
    replace.close.assert_called_once_with()
    replacement.open.assert_called_once_with(context)
    assert output._edges[0] is keep
    assert output._edges[1] is replacement


def test_transactional_edge_swap_rollback_restores_exact_journal_and_sequence() -> None:
    old = MagicMock(spec=EdgeOutput)
    replacement = MagicMock(spec=EdgeOutput)
    journal = DeliveryJournal(1)
    journal.configure(
        True,
        "sender",
        1024 * 1024,
        sender_task_name="sender-task",
        edge_index=0,
        topology_epoch="old-epoch",
    )
    record = Record({"value": 1})
    journal.record_delivery(0, (record,), 1)
    old._journal = journal
    output = TaskOutput([old])
    context = SimpleNamespace(metric_group=None)
    output.open(context)

    output.prepare_edge_swap("resize-1", [replacement])
    assert output._edges[0] is old
    output.activate_edge_swap("resize-1")
    assert output._edges[0] is replacement
    output.rollback_edge_swap("resize-1")

    assert output._edges[0] is old
    assert old._journal is journal
    assert journal.pending_for(0) == ((1, (record,)),)
    assert journal.next_sequence(0) == 2
    assert journal.delivery_channel(0).topology_epoch == "old-epoch"
    old.close.assert_not_called()
    replacement.close.assert_called_once_with()


def test_transactional_edge_swap_commit_closes_only_the_old_route() -> None:
    old = MagicMock(spec=EdgeOutput)
    replacement = MagicMock(spec=EdgeOutput)
    output = TaskOutput([old])
    output.open(SimpleNamespace(metric_group=None))

    output.prepare_edge_swap("resize-1", [replacement])
    output.activate_edge_swap("resize-1")
    assert output.commit_edge_swap("resize-1") is True

    assert output._edges[0] is replacement
    old.close.assert_called_once_with()
    replacement.close.assert_not_called()


@pytest.mark.asyncio
async def test_replacement_pump_stays_paused_until_rescale_resume() -> None:
    task = object.__new__(StreamTask)
    task._rescale_role = "replacement"
    task._rescale_operation_id = "resize-1"
    task._rescale_resume_obj = asyncio.Event()
    task._pump = AsyncMock()

    running = asyncio.create_task(task._run())
    await asyncio.sleep(0)
    task._pump.run_once.assert_not_awaited()
    task._rescale_resume_obj.set()
    await running
    task._pump.run_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_rescale_boundary_flushes_partial_batch_before_runner_barrier() -> None:
    events = []
    task = object.__new__(StreamTask)
    task._pump = SimpleNamespace(
        flush_input_async=AsyncMock(side_effect=lambda: events.append("flush")),
    )
    task._emit = AsyncMock()
    runner = SimpleNamespace(
        barrier=AsyncMock(side_effect=lambda: events.append("barrier")),
    )
    task._state = SimpleNamespace(async_runner=runner)

    await task._drain_rescale_boundary()

    assert events == ["flush", "barrier"]
    task._emit.wait_idle.assert_awaited_once_with(30.0)


def test_actor_topology_activation_failure_restores_every_local_component() -> None:
    previous = SimpleNamespace(
        out_edges=("old",),
        barrier_split={ExecutionVertexId(1, 0): 1},
        input_vertex_ids=(ExecutionVertexId(1, 0),),
    )
    pending = SimpleNamespace(
        out_edges=("new",),
        barrier_split={ExecutionVertexId(1, 0): 2},
        input_vertex_ids=(ExecutionVertexId(1, 0), ExecutionVertexId(1, 1)),
    )
    output = MagicMock(spec=TaskOutput)
    checkpoint = MagicMock()
    tracker = MagicMock()
    tracker.reconfigure_inputs.side_effect = [RuntimeError("tracker failed"), None]
    task = object.__new__(StreamTask)
    task._descriptor = previous
    task._state = SimpleNamespace(
        output=output,
        checkpoint_strategy=checkpoint,
        event_time_tracker=tracker,
        metrics=MagicMock(),
    )
    task._topology_operation_id = "resize-1"
    task._topology_previous_descriptor = previous
    task._topology_pending_descriptor = pending
    task._topology_active = False
    task._topology_commit_tombstones = []
    task._configure_output_replay = MagicMock()

    with pytest.raises(RuntimeError, match="tracker failed"):
        task.activate_topology_reconfiguration("resize-1")

    assert task._descriptor is previous
    assert task._topology_operation_id is None
    output.activate_edge_swap.assert_called_once_with("resize-1")
    output.rollback_edge_swap.assert_called_once_with("resize-1")
    assert checkpoint.reconfigure_barrier_split.call_args_list == [
        ((dict(pending.barrier_split),),),
        ((dict(previous.barrier_split),),),
    ]
    assert tracker.reconfigure_inputs.call_args_list == [
        ((pending.input_vertex_ids,),),
        ((previous.input_vertex_ids,),),
    ]


def test_committed_release_never_resumes_the_old_target() -> None:
    logical, old_graph = _graphs()
    new_logical = logical.rescale_operator(2, 3)
    new_graph = old_graph.rescale_operator(new_logical, 2)
    for vertex in old_graph.execution_vertices:
        vertex.stream_task = MagicMock()
    for vertex in new_graph.job_vertex(2).execution_vertices.values():
        vertex.stream_task = MagicMock()

    with patch.object(job_master_module.klein, "get", side_effect=lambda value, **_kwargs: value):
        JobMaster._release_committed_rescale(
            old_graph,
            new_graph.job_vertex(2),
            2,
            "resize-1",
            1,
        )

    for vertex in old_graph.job_vertex(2).execution_vertices.values():
        vertex.stream_task.resume_rescale.assert_not_called()
    for job_vertex_id in (1, 3):
        for vertex in old_graph.job_vertex(job_vertex_id).execution_vertices.values():
            vertex.stream_task.resume_rescale.assert_called_once_with("resize-1")
    for vertex in new_graph.job_vertex(2).execution_vertices.values():
        vertex.stream_task.resume_rescale.assert_called_once_with("resize-1")


def test_checkpoint_gate_release_rejects_a_false_coordinator_response() -> None:
    _logical, graph = _graphs()
    master = JobMaster(graph, Configuration(include_environment=False))
    master.coordinator = MagicMock()

    with (
        patch.object(job_master_module.klein, "get", return_value=False) as get,
        pytest.raises(RuntimeError, match="failed to release checkpoint gate"),
    ):
        master._finish_local_rescale_gate("resize-1", committed=True)

    assert get.call_count == 3


def test_incomplete_rescale_rollback_stays_fenced_and_forces_global_recovery() -> None:
    logical, old_graph = _graphs()
    resized_logical = logical.rescale_operator(2, 3)
    new_graph = old_graph.rescale_operator(resized_logical, 2)
    master = JobMaster(old_graph, Configuration(include_environment=False))
    recovery = MagicMock()
    master._recovery = recovery
    attempt = SimpleNamespace(
        coordinator_reconfiguration_attempted=True,
        routes_prepared=True,
        candidate_created=False,
    )

    with (
        patch.object(master, "_restore_precommit_topologies", return_value=False),
        patch.object(master, "_replace_recovery_graph"),
        patch.object(master, "_discard_local_rescale_state"),
        patch.object(master, "_finish_local_rescale_gate") as finish_gate,
        patch.object(master, "_release_rescale_participants") as release,
        pytest.raises(RuntimeError, match="global recovery is required"),
    ):
        master._rollback_local_rescale(
            new_graph,
            "resize-1",
            2,
            old_graph,
            new_graph.job_vertex(2),
            1,
            attempt,
        )

    recovery.require_global_recovery.assert_called_once()
    finish_gate.assert_called_once_with("resize-1", committed=True)
    release.assert_not_called()


def test_force_stop_kills_a_partially_created_candidate_handle() -> None:
    handle = MagicMock()
    vertex = SimpleNamespace(
        status=ExecutionVertexStatus.CREATED,
        stream_task=handle,
    )
    job_vertex = SimpleNamespace(execution_vertices={0: vertex})

    with patch.object(task_terminator.klein, "kill") as kill:
        assert task_terminator._stop_worker(job_vertex, force=True) == []

    kill.assert_called_once_with(handle)


def test_bootstrap_descriptor_preserves_rescale_restore_identity() -> None:
    _logical, graph = _graphs()
    vertex = graph.job_vertex(2).execution_vertex(0)
    vertex.restore_operation_id = "resize-1"
    vertex.stream_task = MagicMock()

    with patch.object(task_deployer.klein, "get", side_effect=lambda value, **_kwargs: value):
        task_deployer.bootstrap_vertex(graph, vertex, timeout=1)

    descriptor = vertex.stream_task.setup_and_run_with_descriptor.call_args.args[0]
    assert descriptor.restore_operation_id == "resize-1"


def test_rescale_recovery_fence_escalates_without_rebootstrapping() -> None:
    _logical, graph = _graphs()
    _mark_running(graph)
    coordinator = MagicMock()
    coordinator.needs_recovery.return_value = False
    coordinator.operator_rescale_recovery_fenced.return_value = True
    master = JobMaster(graph, Configuration(include_environment=False))
    master.coordinator = coordinator
    failed = graph.job_vertex(2).execution_vertex(0)
    failed.restore_operation_id = "resize-1"
    for vertex in graph.execution_vertices:
        vertex.stream_task = MagicMock()
        vertex.stream_task.is_running.return_value = vertex is not failed

    with (
        patch.object(
            recovery_manager_module.klein,
            "get_actor_status",
            return_value=StreamTaskStatus.ALIVE,
        ),
        patch.object(recovery_manager_module.klein, "get", side_effect=lambda value, **_kwargs: value),
    ):
        assert master.try_recover_tasks() is False

    failed.stream_task.setup_and_run_with_descriptor.assert_not_called()


def test_superseded_rescale_identity_is_cleared_before_tier_zero_recovery() -> None:
    _logical, graph = _graphs()
    _mark_running(graph)
    coordinator = MagicMock()
    coordinator.needs_recovery.return_value = False
    coordinator.operator_rescale_recovery_fenced.return_value = False
    master = JobMaster(graph, Configuration(include_environment=False))
    master.coordinator = coordinator
    for vertex in graph.execution_vertices:
        vertex.restore_operation_id = "resize-1"
        vertex.stream_task = MagicMock()
        vertex.stream_task.is_running.return_value = True

    with (
        patch.object(
            recovery_manager_module.klein,
            "get_actor_status",
            return_value=StreamTaskStatus.ALIVE,
        ),
        patch.object(recovery_manager_module.klein, "get", side_effect=lambda value, **_kwargs: value),
    ):
        assert master.try_recover_tasks() is True

    assert all(vertex.restore_operation_id is None for vertex in graph.execution_vertices)


def test_coordinator_recovery_rearms_the_stabilization_checkpoint() -> None:
    _logical, graph = _graphs()
    _mark_running(graph)
    source = graph.source_execution_vertices[0]
    source.stream_task = MagicMock()
    source.stream_task.request_checkpoint.return_value = True
    target = graph.job_vertex(2).execution_vertex(0)
    target.restore_operation_id = "resize-1"
    coordinator = MagicMock()
    coordinator.needs_recovery.return_value = True
    coordinator.latest_checkpoint_path.return_value = "/tmp/chk/1"
    coordinator.barrier_epoch_floor.return_value = 0
    master = JobMaster(graph, Configuration(include_environment=False))
    master.coordinator = coordinator

    with (
        patch.object(
            recovery_manager_module.klein,
            "get_actor_status",
            return_value=StreamTaskStatus.ALIVE,
        ),
        patch.object(recovery_manager_module.klein, "get", side_effect=lambda value, **_kwargs: value),
    ):
        assert master.recover_coordinator_if_needed() is True

    source.stream_task.request_checkpoint.assert_called_once_with()


def test_durable_rescale_clears_graph_restore_identity() -> None:
    _logical, graph = _graphs()
    target = graph.job_vertex(2).execution_vertex(0)
    target.restore_operation_id = "resize-1"
    coordinator = MagicMock()
    coordinator.needs_recovery.return_value = False
    coordinator.operator_rescale_recovery_fenced.return_value = False
    master = JobMaster(graph, Configuration(include_environment=False))
    master.coordinator = coordinator

    with patch.object(
        recovery_manager_module.klein,
        "get",
        side_effect=lambda value, **_kwargs: value,
    ):
        assert master._recovery.clear_stable_rescale_metadata() is True

    assert target.restore_operation_id is None


@pytest.mark.asyncio
async def test_fenced_stream_task_reports_unhealthy() -> None:
    task = object.__new__(StreamTask)
    task._task_name = "fenced"
    task._running = True
    task._task = asyncio.create_task(asyncio.sleep(10))
    task._rescale_operation_id = "resize-1"
    task._rescale_role = "replacement"
    task._rescale_ready_obj = asyncio.Event()
    try:
        healthy, reason = task.health_info()
        assert healthy is False
        assert "not running" in reason
    finally:
        task._task.cancel()


def test_source_forced_checkpoint_is_emitted_at_the_next_idle_boundary() -> None:
    task = object.__new__(SourceStreamTask)
    task._running = True
    task._eof_reached = False
    task._drain_requested = False
    task._forced_checkpoint_requested = threading.Event()
    task._source_rescale_requested = threading.Event()
    task._state = SimpleNamespace(
        operator=SimpleNamespace(end_of_stream=False),
        checkpoint_strategy=SimpleNamespace(should_trigger=Mock(return_value=False)),
        metrics=SimpleNamespace(barriers_out=Mock()),
    )
    barrier = Barrier(7, ExecutionVertexId(1, 0))
    task._generate_barrier = Mock(return_value=barrier)

    assert task.request_checkpoint() is True
    assert task._on_records_emitted(record_emitted=False) is barrier

    task._generate_barrier.assert_called_once_with(force=True)
    task._state.metrics.barriers_out.inc.assert_called_once_with()
    assert task._forced_checkpoint_requested.is_set() is False
