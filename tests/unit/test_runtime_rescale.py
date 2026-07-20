# SPDX-License-Identifier: Apache-2.0
"""Focused contracts for local single-operator runtime rescaling."""

import asyncio
import hashlib
import pickle
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from ray.exceptions import RayTaskError

from ray.klein._internal.deadline import Deadline
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.job_status import JobStatus
from ray.klein.api.node_type import NodeType
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.collector.edge_output import EdgeOutput
from ray.klein.runtime.collector.task_output import TaskOutput
from ray.klein.runtime.coordinator import checkpoint_coordinator as checkpoint_coordinator_module
from ray.klein.runtime.coordinator.checkpoint import Checkpoint
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.runtime.coordinator.checkpoint_strategy import AlignedCheckpointStrategy, _BarrierAligner
from ray.klein.runtime.execution_graph.checkpoint_domain import CheckpointDomain
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.job_manager.job_manager import JobManager, _format_rescale_error
from ray.klein.runtime.job_manager.progress import OperatorProgress, ProgressSnapshot
from ray.klein.runtime.message import Barrier, DeliveryChannel, Record, RescaleBarrier
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.operator.sink import SinkOperator
from ray.klein.runtime.partitioning import ForwardPartitioner, RoundRobinPartitioner
from ray.klein.runtime.resources import Resources
from ray.klein.runtime.scheduler import job_master as job_master_module
from ray.klein.runtime.scheduler import recovery_manager as recovery_manager_module
from ray.klein.runtime.scheduler import task_deployer, task_terminator
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.scheduler.placement import NativeStrategy
from ray.klein.runtime.scheduler.rescale_plan import (
    RescalePhase,
    RescalePlan,
    RescaleTransaction,
)
from ray.klein.runtime.worker.source_stream_task import SourceStreamTask
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.state.key_group_range import KeyGroupRange
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


def _graphs(
    parallelism: int = 2,
    source_parallelism: int = 1,
    *,
    console_sink: bool = False,
):
    config = Configuration(include_environment=False)
    builder = LogicalGraphBuilder("job", config)
    source = _vertex("job", 1, NodeType.SOURCE, source_parallelism)
    target = _vertex("job", 2, NodeType.TRANSFORM, parallelism)
    sink = (
        VertexSpec(
            VertexId("job", 3),
            "ConsoleSinkAll[3]",
            OperatorSpec(
                SinkOperator,
                LogicalFunction(ConsoleSinkFunction),
                3,
                "ConsoleSinkAll",
                OperatorType.SINK,
            ),
            NodeType.SINK,
            Resources(num_cpus=0, concurrency=2),
        )
        if console_sink
        else _vertex("job", 3, NodeType.SINK, 2)
    )
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


def _bare_rescale_task(name: str = "task") -> StreamTask:
    task = object.__new__(StreamTask)
    task._task_name = name
    task._rescale_operation_id = None
    task._rescale_role = None
    task._rescale_expected_senders = set()
    task._rescale_seen_senders = set()
    task._rescale_edge_indices = ()
    task._rescale_ready_obj = None
    task._rescale_resume_obj = None
    task._rescale_snapshot = None
    task._rescale_tombstones = []
    task._topology_operation_id = None
    task._topology_previous_descriptor = None
    task._topology_pending_descriptor = None
    task._topology_active = False
    task._topology_commit_tombstones = []
    return task


def _managed_state_reference(
    key_groups: dict[int, bytes] | None = None,
    *,
    max_parallelism: int = 8,
    watermark: int = -1,
) -> StateSnapshotReference:
    payload = pickle.dumps(
        {
            "format_version": 2,
            "max_parallelism": max_parallelism,
            "key_group_range": KeyGroupRange(0, max_parallelism - 1),
            "key_groups": key_groups or {},
            "watermark": watermark,
        },
        protocol=pickle.HIGHEST_PROTOCOL,
    )
    return StateSnapshotReference(
        len(payload),
        f"sha256:{hashlib.sha256(payload).hexdigest()}",
        inline_payload=payload,
    )


async def _wait_for_rescale_status(
    manager: JobManager,
    operation_id: str,
    expected_status: str,
) -> dict:
    for _ in range(200):
        operation = manager._rescale_operations[operation_id]
        if operation["status"] == expected_status:
            return operation
        await asyncio.sleep(0.01)
    pytest.fail(
        f"rescale {operation_id} did not reach {expected_status}; "
        f"last status was {manager._rescale_operations[operation_id]['status']}"
    )


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


def test_lifecycle_operator_must_explicitly_opt_into_overlap_safe_rescale() -> None:
    class ExternalSink(SinkFunction):
        def write(self, value: dict) -> None:
            del value

    unsafe = OperatorSpec(
        StreamOperator,
        LogicalFunction(ExternalSink),
        2,
        "sink",
        OperatorType.ONE_INPUT,
    )
    assert unsafe.supports_concurrent_rescale is False

    class OverlapSafeSink(ExternalSink):
        supports_concurrent_rescale = True

    safe = OperatorSpec(
        StreamOperator,
        LogicalFunction(OverlapSafeSink),
        2,
        "sink",
        OperatorType.ONE_INPUT,
    )
    assert safe.supports_concurrent_rescale is True


def test_narrow_physical_resize_matches_logical_forward_edge_rewrite() -> None:
    logical, physical = _graphs(parallelism=2, source_parallelism=2)

    resized_logical = logical.rescale_operator(2, 3)
    expected = physical.rescale_operator(resized_logical, 2)
    resized = physical.resize_operator(2, 3)

    assert tuple(str(edge.partitioner) for edge in resized.job_edges) == tuple(
        str(edge.partitioner) for edge in expected.job_edges
    )
    assert all(not edge.partitioner.is_type(ForwardPartitioner) for edge in resized.job_edges)

    # Once an implicit FORWARD edge became a shuffle, scaling back preserves
    # that explicit shuffle just like LogicalGraph.rescale_operator does.
    restored_logical = resized_logical.rescale_operator(2, 2)
    expected_restored = resized.rescale_operator(restored_logical, 2)
    restored = resized.resize_operator(2, 2)
    assert tuple(str(edge.partitioner) for edge in restored.job_edges) == tuple(
        str(edge.partitioner) for edge in expected_restored.job_edges
    )
    assert all(not edge.partitioner.is_type(ForwardPartitioner) for edge in restored.job_edges)


@pytest.mark.asyncio
async def test_checkpoint_gate_and_transient_state_do_not_pollute_global_checkpoint() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    await coordinator.begin_operator_rescale("resize-1", timeout=1)

    registration = coordinator.register_checkpoint(ExecutionVertexId(1, 0))
    reference = _managed_state_reference({0: b"group-0", 7: b"group-7"})
    await coordinator.stage_operator_rescale_state("resize-1", 2, 2, (reference,))

    assert registration.barrier_id is None
    assert "paused" in registration.reason
    left = coordinator.operator_rescale_states("resize-1", ExecutionVertexId(2, 0))
    right = coordinator.operator_rescale_states("resize-1", ExecutionVertexId(2, 1))
    assert len(left) == len(right) == 1
    assert pickle.loads(left[0].materialize(checkpoint_coordinator_module.klein.get))["key_groups"] == {0: b"group-0"}
    assert pickle.loads(right[0].materialize(checkpoint_coordinator_module.klein.get))["key_groups"] == {7: b"group-7"}
    assert coordinator._latest_operator_states == {}
    assert coordinator._restored_operator_states == {}
    assert coordinator.finish_operator_rescale("resize-1") is True


def test_rescale_stabilization_sources_share_one_checkpoint_epoch() -> None:
    _logical, graph = _graphs(source_parallelism=2)
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = graph
    sources = {vertex.id for vertex in graph.source_execution_vertices}
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = set(sources)

    registrations = [coordinator.register_checkpoint(source_id, force=True) for source_id in sources]

    assert {registration.barrier_id for registration in registrations} == {registrations[0].barrier_id}
    assert all(registration.coordinated for registration in registrations)
    assert len(coordinator._inflight_checkpoints) == 1
    checkpoint = next(iter(coordinator._inflight_checkpoints.values()))
    assert checkpoint.coordinated is True
    assert set(checkpoint.trigger_sources) == sources
    assert checkpoint.required_acknowledgements == len(graph.sink_execution_vertices)
    assert set(checkpoint.required_committers) == {vertex.id for vertex in graph.sink_execution_vertices}


def test_coordinated_barrier_aligns_direct_inputs_once_with_multiplicity() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    aligner = _BarrierAligner({}, (first, first, second))
    barrier = Barrier(7, first, coordinated=True)
    first_edge = DeliveryChannel(first, "first", 0, 0)
    second_edge = DeliveryChannel(first, "first", 1, 0)
    third_edge = DeliveryChannel(second, "second", 0, 0)

    assert aligner.receive(barrier, first, first_edge) is False
    assert aligner.receive(barrier, second, third_edge) is False
    assert aligner.receive(barrier, first, second_edge) is True
    assert aligner.receive(barrier, first, first_edge) is False


def test_aborted_coordinated_barrier_releases_partial_alignment() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    aligner = _BarrierAligner({}, (first, second))
    barrier = Barrier(7, first, coordinated=True)

    assert aligner.receive(barrier, first) is False
    assert aligner.discard(7) == 1
    aligner.validate_reconfiguration()
    assert aligner.receive(barrier, first) is False
    replacement = Barrier(8, first, coordinated=True)
    assert aligner.receive(replacement, first) is False
    assert aligner.receive(replacement, second) is True


@pytest.mark.asyncio
async def test_shared_checkpoint_atomically_persists_all_sources_and_target_shards(tmp_path) -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    notifications: list[tuple[ExecutionVertexId, int]] = []

    class _SourceTask:
        def __init__(self, vertex_id: ExecutionVertexId, offset: int) -> None:
            self.vertex_id = vertex_id
            self.offset = offset

        def source_checkpoint_state(self, barrier_id: int):
            return True, {"offset": self.offset, "barrier_id": barrier_id}

        def notify_source_checkpoint_persisted(self, barrier_id: int) -> bool:
            notifications.append((self.vertex_id, barrier_id))
            return True

        def discard_source_checkpoint(self, _barrier_id: int) -> bool:
            return True

    source_vertices = {
        first: SimpleNamespace(id=first, stream_task=_SourceTask(first, 11)),
        second: SimpleNamespace(id=second, stream_task=_SourceTask(second, 22)),
    }

    class _Graph:
        source_execution_vertices = tuple(source_vertices.values())
        sink_execution_vertices = (SimpleNamespace(),)

        @staticmethod
        def execution_vertex(vertex_id: ExecutionVertexId):
            return source_vertices[vertex_id]

    config = Configuration(include_environment=False)
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    coordinator = CheckpointCoordinator(config, job_id="job")
    coordinator._ensure_locks()
    coordinator._execution_graph = _Graph()
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = {first, second}
    coordinator._coordinated_checkpoint_barrier_id = 7
    left = _managed_state_reference({0: b"left"})
    right = _managed_state_reference({7: b"right"})
    coordinator._inflight_operator_states[7] = {"2:0": left, "2:1": right}
    coordinator._transient_rescale_states[("resize-1", 2)] = (left, right)
    checkpoint = Checkpoint(7, 1, (first, second), coordinated=True)
    checkpoint.mark_in_progress()

    assert await coordinator._complete_aligned_checkpoint(checkpoint, 7) is True

    assert checkpoint.status.name == "COMPLETED"
    assert coordinator.operator_rescale_recovery_fenced() is False
    assert set(coordinator._latest_operator_states) == {"2:0", "2:1"}
    assert set(coordinator._latest_source_states) == {"1:0", "1:1"}
    assert set(notifications) == {(first, 7), (second, 7)}


@pytest.mark.asyncio
async def test_durable_source_release_and_callback_retry_are_independent() -> None:
    source_id = ExecutionVertexId(1, 0)

    class _SourceTask:
        attempts = 0

        def notify_source_checkpoint_persisted(self, _barrier_id: int) -> bool:
            self.attempts += 1
            return self.attempts > 1

    source_task = _SourceTask()
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = SimpleNamespace(
        source_execution_vertices=(SimpleNamespace(id=source_id, stream_task=source_task),),
    )
    coordinator._durable_source_states["1:0"] = SourceCheckpointEntry("1:0", 7, {"offset": 7})
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = {source_id}
    coordinator._rescale_recovery_required_state_revision = 1
    coordinator._coordinated_checkpoint_barrier_id = 7
    coordinator._persisted_state_revision = 1

    await coordinator._notify_durable_source_checkpoints()
    coordinator._clear_rescale_recovery_fence_if_durable()

    assert coordinator.operator_rescale_recovery_fenced() is False
    assert coordinator._released_source_checkpoint_ids["1:0"] == 7
    assert "1:0" not in coordinator._notified_source_checkpoint_ids

    await coordinator._notify_durable_source_checkpoints()
    assert coordinator._notified_source_checkpoint_ids["1:0"] == 7
    assert source_task.attempts == 2


@pytest.mark.asyncio
async def test_lost_durable_source_rpc_retains_fence_until_retry() -> None:
    source_id = ExecutionVertexId(1, 0)

    class _SourceTask:
        fail = True

        def notify_source_checkpoint_persisted(self, _barrier_id: int) -> bool:
            if self.fail:
                raise ConnectionError("response lost")
            return True

    source_task = _SourceTask()
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = SimpleNamespace(
        source_execution_vertices=(SimpleNamespace(id=source_id, stream_task=source_task),),
    )
    coordinator._durable_source_states["1:0"] = SourceCheckpointEntry("1:0", 7, {"offset": 7})
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = {source_id}
    coordinator._rescale_recovery_required_state_revision = 1
    coordinator._coordinated_checkpoint_barrier_id = 7
    coordinator._persisted_state_revision = 1

    await coordinator._notify_durable_source_checkpoints()
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is True

    source_task.fail = False
    await coordinator._notify_durable_source_checkpoints()
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is False


@pytest.mark.asyncio
async def test_checkpoint_maintenance_starts_when_periodic_persistence_is_disabled() -> None:
    config = Configuration(include_environment=False)
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 0)
    coordinator = CheckpointCoordinator(config, job_id="job")

    await coordinator.start()
    try:
        assert coordinator.healthy is True
    finally:
        await coordinator.stop()


@pytest.mark.asyncio
async def test_disabled_persistence_still_runs_checkpoint_expiry_maintenance() -> None:
    config = Configuration(include_environment=False)
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 0)
    coordinator = CheckpointCoordinator(config, job_id="job")
    coordinator._expire_stale_checkpoints = AsyncMock()
    coordinator._commit_durable_sink_committables = AsyncMock(return_value=True)
    coordinator._notify_durable_source_checkpoints = AsyncMock()

    with patch.object(checkpoint_coordinator_module.asyncio, "sleep", new=AsyncMock()):
        await coordinator._run()

    coordinator._expire_stale_checkpoints.assert_awaited_once_with()
    coordinator._notify_durable_source_checkpoints.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_aligned_checkpoint_source_collection_has_a_finite_deadline() -> None:
    source_id = ExecutionVertexId(1, 0)
    source_task = SimpleNamespace(
        source_checkpoint_state=Mock(return_value=object()),
        discard_source_checkpoint=Mock(return_value=True),
        discard_checkpoint=Mock(return_value=0),
    )
    source_vertex = SimpleNamespace(id=source_id, stream_task=source_task)
    graph = SimpleNamespace(
        execution_vertex=Mock(return_value=source_vertex),
        execution_vertices=(source_vertex,),
    )
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = graph
    checkpoint = Checkpoint(7, 1, (source_id,), coordinated=True)
    checkpoint.mark_in_progress()

    with patch.object(
        checkpoint_coordinator_module.klein,
        "aget",
        new=AsyncMock(side_effect=TimeoutError("source RPC stalled")),
    ) as aget:
        assert await coordinator._complete_aligned_checkpoint(checkpoint, 7) is False

    assert aget.await_args_list[0].kwargs["timeout"] == coordinator._checkpoint_operation_timeout()
    assert checkpoint.status.name == "FAILED"


@pytest.mark.asyncio
async def test_failed_abort_cleanup_is_retried_before_reusing_shared_epoch() -> None:
    source_id = ExecutionVertexId(1, 0)
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._coordinated_checkpoint_barrier_id = 7
    coordinator._coordinated_checkpoint_registered_sources = {source_id}
    coordinator._attempt_checkpoint_release = AsyncMock(side_effect=[False, True])

    await coordinator._release_checkpoint_sources(7, (source_id,))
    assert coordinator._coordinated_checkpoint_barrier_id == 7
    assert 7 in coordinator._pending_checkpoint_releases

    await coordinator._retry_pending_checkpoint_releases()
    assert coordinator._coordinated_checkpoint_barrier_id is None
    assert coordinator._coordinated_checkpoint_registered_sources == set()
    assert coordinator._pending_checkpoint_releases == {}


@pytest.mark.asyncio
async def test_checkpoint_release_accepts_an_idempotent_source_noop() -> None:
    source_id = ExecutionVertexId(1, 0)
    source_task = SimpleNamespace(discard_source_checkpoint=Mock(return_value=object()))
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = SimpleNamespace(
        execution_vertex=Mock(return_value=SimpleNamespace(stream_task=source_task)),
    )
    coordinator._discard_checkpoint_alignment = AsyncMock(return_value=True)

    with patch.object(
        checkpoint_coordinator_module.klein,
        "aget",
        new=AsyncMock(return_value=False),
    ):
        assert await coordinator._attempt_checkpoint_release(7, (source_id,)) is True


def test_coordinated_source_keeps_running_after_ordered_emit() -> None:
    task = object.__new__(SourceStreamTask)
    task._coordinated_checkpoint_barrier_id = None
    emitted = threading.Event()
    output = Mock()
    output.collect.side_effect = lambda _barrier: emitted.set()
    task._state = SimpleNamespace(output=output)
    barrier = Barrier(7, ExecutionVertexId(1, 0), coordinated=True)

    worker = threading.Thread(target=task._emit_checkpoint_barrier, args=(barrier,))
    worker.start()
    assert emitted.wait(timeout=1)
    worker.join(timeout=1)
    assert worker.is_alive() is False
    assert task._coordinated_checkpoint_barrier_id == 7


@pytest.mark.asyncio
async def test_rescale_state_is_materialized_once_and_served_as_one_target_partition() -> None:
    config = Configuration(include_environment=False)
    config.set(StateOptions.OBJECT_STORE_CACHE_ENABLED, False)
    coordinator = CheckpointCoordinator(config, job_id="job")
    await coordinator.begin_operator_rescale("resize-1", timeout=1)
    payloads = {
        "old-0": _managed_state_reference({0: b"zero", 1: b"one"}).inline_payload,
        "old-1": _managed_state_reference({4: b"four", 7: b"seven"}).inline_payload,
    }
    references = tuple(
        StateSnapshotReference(
            len(payload),
            f"sha256:{hashlib.sha256(payload).hexdigest()}",
            object_ref=name,
        )
        for name, payload in payloads.items()
    )

    with patch.object(checkpoint_coordinator_module.klein, "get", side_effect=payloads.__getitem__) as get:
        await coordinator.stage_operator_rescale_state("resize-1", 2, 3, references)

    assert [call.args[0] for call in get.call_args_list] == ["old-0", "old-1"]
    restored = {}
    for index in range(3):
        target_references = coordinator.operator_rescale_states("resize-1", ExecutionVertexId(2, index))
        assert len(target_references) == 1
        target_payload = pickle.loads(target_references[0].inline_payload)
        restored.update(target_payload["key_groups"])
    assert restored == {0: b"zero", 1: b"one", 4: b"four", 7: b"seven"}


@pytest.mark.asyncio
async def test_checkpoint_gate_aborts_old_topology_inflight() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    source_id = ExecutionVertexId(1, 0)
    sink_id = ExecutionVertexId(3, 0)
    source_task = Mock()
    sink_task = Mock()
    vertices = {
        source_id: SimpleNamespace(id=source_id, stream_task=source_task),
        sink_id: SimpleNamespace(id=sink_id, stream_task=sink_task),
    }
    coordinator._execution_graph = SimpleNamespace(
        checkpoint_domains=(),
        find_execution_vertex=vertices.get,
        execution_vertex=vertices.__getitem__,
    )
    checkpoint = Checkpoint(
        1,
        1,
        (source_id,),
        domain_id="old-domain",
        required_committers=(sink_id,),
    )
    checkpoint.mark_in_progress()
    coordinator._checkpoint_history.append(checkpoint)
    coordinator._inflight_checkpoints[1] = checkpoint
    coordinator._active_checkpoint_by_domain["old-domain"] = 1
    coordinator._inflight_operator_states[1] = {"2:0": Mock()}
    coordinator._inflight_sink_committables[1] = {}

    assert await coordinator.begin_operator_rescale("resize-1", timeout=1) is True
    # Retrying the same operation is idempotent and must not repeat cleanup.
    assert await coordinator.begin_operator_rescale("resize-1", timeout=1) is True

    assert coordinator._inflight_checkpoints == {}
    assert coordinator._active_checkpoint_by_domain == {}
    assert coordinator._inflight_operator_states == {}
    assert coordinator._inflight_sink_committables == {}
    assert checkpoint.status.name == "FAILED"
    assert checkpoint.reason == "Checkpoint aborted for operator rescale resize-1."
    assert coordinator._checkpoints_failed.value == 1
    assert coordinator._checkpoints_in_progress.value == 0
    source_task.discard_source_checkpoint.assert_called_once_with(1)
    source_task.abort_checkpoint.assert_called_once_with(1)
    sink_task.abort_checkpoint.assert_called_once_with(1)
    coordinator.finish_operator_rescale("resize-1")


@pytest.mark.asyncio
async def test_checkpoint_gate_retry_waits_for_inflight_cleanup() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    source_id = ExecutionVertexId(1, 0)
    checkpoint = Checkpoint(1, 1, (source_id,))
    checkpoint.mark_in_progress()
    coordinator._inflight_checkpoints[1] = checkpoint
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def delayed_cleanup(*_args, **_kwargs) -> None:
        cleanup_started.set()
        await release_cleanup.wait()

    with patch.object(coordinator, "_fail_checkpoint", side_effect=delayed_cleanup) as cleanup:
        first = asyncio.create_task(coordinator.begin_operator_rescale("resize-1", timeout=1))
        await cleanup_started.wait()
        retry = asyncio.create_task(coordinator.begin_operator_rescale("resize-1", timeout=1))
        await asyncio.sleep(0)
        assert not retry.done()

        release_cleanup.set()
        assert await first is True
        assert await retry is True

    cleanup.assert_awaited_once_with(
        checkpoint,
        1,
        "Checkpoint aborted for operator rescale resize-1.",
        wait_for_task_abort=True,
    )
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


def test_recovery_fence_requires_one_shared_durable_source_cut() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    coordinator._rescale_recovery_fence = "resize-1"
    coordinator._rescale_recovery_pending_sources = {first, second}
    coordinator._coordinated_checkpoint_barrier_id = 7

    coordinator._state_revision = 7
    partial = Checkpoint(6, 1, (first,), coordinated=True)
    assert coordinator._record_rescale_stabilization_progress(partial) is False
    assert coordinator.operator_rescale_recovery_fenced() is True
    shared = Checkpoint(7, 1, (first, second), coordinated=True)
    assert coordinator._record_rescale_stabilization_progress(shared) is True
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is True

    coordinator._persisted_state_revision = 7
    coordinator._released_source_checkpoint_ids = {"1:0": 7, "1:1": 7}
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is False


@pytest.mark.asyncio
async def test_rescale_restore_falls_back_after_transient_state_is_superseded() -> None:
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._ensure_locks()
    vertex_id = ExecutionVertexId(2, 0)
    reference = _managed_state_reference({0: b"group-0"})
    await coordinator.begin_operator_rescale("resize-1", timeout=1)

    with pytest.raises(RuntimeError, match=r"active operator rescale.*unavailable"):
        await coordinator.restore_operator_rescale_states("resize-1", vertex_id)

    await coordinator.stage_operator_rescale_state("resize-1", 2, 1, (reference,))
    transient = await coordinator.restore_operator_rescale_states("resize-1", vertex_id)
    assert len(transient) == 1
    assert pickle.loads(transient[0].materialize(checkpoint_coordinator_module.klein.get))["key_groups"] == {
        0: b"group-0"
    }
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
    reference = _managed_state_reference({0: b"group-0"})
    await coordinator.begin_operator_rescale("resize-1", timeout=1)
    await coordinator.stage_operator_rescale_state("resize-1", 2, 1, (reference,))

    assert coordinator.finish_operator_rescale("resize-1", committed=True) is True
    assert coordinator.operator_rescale_recovery_fenced() is True
    coordinator._replace_logical_operator_states({"2:0": reference})
    coordinator._coordinated_checkpoint_barrier_id = 7
    checkpoint = Checkpoint(7, 1, (source_id,), coordinated=True)
    assert coordinator._record_rescale_stabilization_progress(checkpoint) is True
    coordinator._clear_rescale_recovery_fence_if_durable()

    assert coordinator.operator_rescale_recovery_fenced() is True
    coordinator._persisted_state_revision = coordinator._state_revision
    coordinator._released_source_checkpoint_ids["1:0"] = 7
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
    coordinator._coordinated_checkpoint_barrier_id = 7
    coordinator._latest_source_states["1:0"] = SourceCheckpointEntry(
        task_key="1:0",
        checkpoint_id=7,
        state={"offset": 7},
    )
    coordinator._state_revision = 1
    checkpoint = Checkpoint(7, 1, (source_id,), coordinated=True)
    assert coordinator._record_rescale_stabilization_progress(checkpoint) is True

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
    assert coordinator.operator_rescale_recovery_fenced() is True
    coordinator._released_source_checkpoint_ids["1:0"] = 7
    coordinator._clear_rescale_recovery_fence_if_durable()
    assert coordinator.operator_rescale_recovery_fenced() is False


@pytest.mark.asyncio
async def test_job_manager_noop_rejection_success_and_failed_swap_preserve_committed_graph() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(execution_graph=physical)
    manager.job_master.take_retired_rescale_counts.return_value = {}
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

    async def commit_resize(_callable, target_id, parallelism, _operation_id):
        manager.job_master.execution_graph = manager.job_master.execution_graph.resize_operator(
            target_id,
            parallelism,
        )

    manager.run_exclusive = AsyncMock(side_effect=commit_resize)
    completed = await manager.rescale_operator("2", 3)
    assert completed["status"] == "COMPLETED"
    assert manager.execution_graph.job_vertex(1) is physical.job_vertex(1)
    while manager._active_rescale_operation_id is not None:
        await asyncio.sleep(0)
    committed = manager.execution_graph
    for vertex in committed.execution_vertices:
        if vertex.status == ExecutionVertexStatus.CREATED:
            vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
            vertex.transition_to(ExecutionVertexStatus.RUNNING)

    manager.run_exclusive = AsyncMock(side_effect=RuntimeError("swap failed"))
    failed = await manager.rescale_operator(2, 4)
    assert failed["status"] == "FAILED"
    assert failed["error"] == "RuntimeError: swap failed"
    assert manager.execution_graph is committed
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_job_manager_async_rescale_admission_is_observable_and_rejects_a_second_request() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(coordinator=None)
    manager.job_master.restart_window.return_value = (0, 0, 0)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_rescale(*_args):
        entered.set()
        await release.wait()

    manager.run_exclusive = AsyncMock(side_effect=blocked_rescale)

    accepted = await manager.submit_operator_rescale(2, 3)

    assert accepted["status"] == "ACCEPTED"
    assert accepted["phase"] == "QUEUED"
    assert accepted["operation_id"]
    await entered.wait()

    snapshot = await manager.dashboard_snapshot()
    operation = snapshot["rescale_operations"][0]
    target = next(operator for operator in snapshot["operators"] if operator["op_id"] == 2)
    assert operation["operation_id"] == accepted["operation_id"]
    assert operation["status"] == "RUNNING"
    assert operation["phase"] == "COORDINATING"
    assert target["rescale_operation"]["operation_id"] == accepted["operation_id"]

    rejected = await manager.submit_operator_rescale(2, 4)
    assert rejected["status"] == "REJECTED"
    assert rejected["active_operation_id"] == accepted["operation_id"]

    release.set()
    completed = await manager._wait_for_rescale_operation(accepted["operation_id"])
    assert completed["status"] == "COMPLETED"
    assert manager._active_rescale_operation_id is None
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_job_manager_tracks_stabilization_without_holding_lifecycle_lock() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    fence = {"active": True}
    coordinator = SimpleNamespace(operator_rescale_recovery_fenced=lambda: fence["active"])
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(coordinator=coordinator)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    manager.run_exclusive = AsyncMock(return_value=None)

    synchronous = asyncio.create_task(manager.rescale_operator(2, 3))
    for _ in range(100):
        if manager._rescale_operations and next(iter(manager._rescale_operations.values()))["status"] == "STABILIZING":
            break
        await asyncio.sleep(0)
    operation = next(iter(manager._rescale_operations.values()))

    assert operation["status"] == "STABILIZING"
    assert manager._lifecycle_lock.locked() is False
    # The compatibility API returns at topology commit while the authoritative
    # operation continues through its stabilization checkpoint.
    assert (await asyncio.wait_for(synchronous, timeout=1))["status"] == "COMPLETED"
    assert operation["status"] == "STABILIZING"

    fence["active"] = False
    for _ in range(100):
        if operation["status"] == "COMPLETED":
            break
        await asyncio.sleep(0.01)
    assert operation["status"] == "COMPLETED"
    assert operation["ended_at_ms"] is not None
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_cancelling_rescale_caller_does_not_cancel_admitted_background_operation() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(coordinator=None)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_rescale(*_args):
        entered.set()
        await release.wait()

    manager.run_exclusive = AsyncMock(side_effect=blocked_rescale)
    caller = asyncio.create_task(manager.rescale_operator(2, 3))
    await entered.wait()
    operation_id = manager._active_rescale_operation_id
    background = manager._rescale_task_obj

    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    assert operation_id is not None
    assert background is not None
    assert background.done() is False
    assert manager._rescale_operations[operation_id]["status"] == "RUNNING"

    release.set()
    operation = await _wait_for_rescale_status(manager, operation_id, "COMPLETED")
    await asyncio.sleep(0)
    assert operation["target_parallelism"] == 3
    assert manager.execution_graph.job_vertex(2).concurrency == 3
    assert manager._active_rescale_operation_id is None
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_rescale_stabilization_failure_is_retained_after_topology_commit() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    coordinator = SimpleNamespace(operator_rescale_recovery_fenced=lambda: True)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(coordinator=coordinator)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    manager.run_exclusive = AsyncMock(return_value=None)

    accepted = await manager.submit_operator_rescale(2, 3)
    operation = await _wait_for_rescale_status(manager, accepted["operation_id"], "STABILIZING")
    assert operation["ended_at_ms"] is None

    manager._job_status = JobStatus.CANCELLED
    operation = await _wait_for_rescale_status(manager, accepted["operation_id"], "FAILED")
    await asyncio.sleep(0)

    assert operation["phase"] == "COMPLETED"
    assert "CANCELLED" in operation["error"]
    assert operation["ended_at_ms"] is not None
    assert manager._active_rescale_operation_id is None
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_async_scale_out_and_in_are_retained_in_snapshot_history() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(coordinator=None)
    manager.job_master.restart_window.return_value = (0, 0, 0)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    manager.run_exclusive = AsyncMock(return_value=None)

    scale_out = await manager.submit_operator_rescale(2, 4)
    assert scale_out["status"] == "ACCEPTED"
    assert scale_out["previous_parallelism"] == 2
    await _wait_for_rescale_status(manager, scale_out["operation_id"], "COMPLETED")
    await asyncio.sleep(0)
    assert manager.execution_graph.job_vertex(2).concurrency == 4

    for vertex in manager.execution_graph.execution_vertices:
        if vertex.status == ExecutionVertexStatus.CREATED:
            vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
            vertex.transition_to(ExecutionVertexStatus.RUNNING)
    scale_in = await manager.submit_operator_rescale(2, 2)
    assert scale_in["status"] == "ACCEPTED"
    assert scale_in["previous_parallelism"] == 4
    await _wait_for_rescale_status(manager, scale_in["operation_id"], "COMPLETED")
    await asyncio.sleep(0)
    assert manager.execution_graph.job_vertex(2).concurrency == 2

    for vertex in manager.execution_graph.execution_vertices:
        if vertex.status == ExecutionVertexStatus.CREATED:
            vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
            vertex.transition_to(ExecutionVertexStatus.RUNNING)
    snapshot = await manager.dashboard_snapshot()
    operations = snapshot["rescale_operations"]
    target = next(operator for operator in snapshot["operators"] if operator["op_id"] == 2)

    assert [operation["operation_id"] for operation in operations[:2]] == [
        scale_in["operation_id"],
        scale_out["operation_id"],
    ]
    assert [operation["target_parallelism"] for operation in operations[:2]] == [2, 4]
    assert all(operation["status"] == "COMPLETED" for operation in operations[:2])
    assert target["rescale_operation"]["operation_id"] == scale_in["operation_id"]
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_job_manager_rejects_unknown_operator_before_async_admission() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(coordinator=None)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING

    rejected = await manager.submit_operator_rescale(999, 3)

    assert rejected["status"] == "REJECTED"
    assert rejected["ended_at_ms"] is not None
    assert manager._active_rescale_operation_id is None
    assert manager._rescale_task_obj is None
    assert next(iter(manager._rescale_operations.values()))["operation_id"] == rejected["operation_id"]
    for operator_id in range(1_000, 1_025):
        assert (await manager.submit_operator_rescale(operator_id, 3))["status"] == "REJECTED"
    assert len(manager._rescale_operations) == 20
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_job_manager_rescale_failure_reports_nested_ray_cause_without_traceback() -> None:
    logical, physical = _graphs()
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock(execution_graph=physical)
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    timeout = TimeoutError("\x1b[31mtimed out waiting for in-flight checkpoints before rescale\x1b[0m")
    remote_timeout = RayTaskError(
        "begin_operator_rescale",
        "\x1b[36mray::CheckpointCoordinator.begin_operator_rescale()\x1b[39m\nremote traceback",
        timeout,
    ).as_instanceof_cause()
    manager.run_exclusive = AsyncMock(
        side_effect=RayTaskError(
            "rescale_operator",
            "\x1b[36mray::JobMaster.rescale_operator()\x1b[39m\nouter remote traceback",
            remote_timeout,
        )
    )

    failed = await manager.rescale_operator(2, 3)

    assert failed["status"] == "FAILED"
    assert failed["error"] == "TimeoutError: timed out waiting for in-flight checkpoints before rescale"
    assert "traceback" not in failed["error"]
    assert "\x1b" not in failed["error"]
    manager._writer.shutdown(wait=False)


def test_job_manager_rescale_error_without_a_ray_cause_does_not_expose_traceback() -> None:
    error = RayTaskError(
        "rescale_operator",
        "\x1b[36mray::JobMaster.rescale_operator()\x1b[39m\nsecret remote traceback",
        None,
    )

    assert _format_rescale_error(error) == "RayTaskError: remote task failed"


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
    manager.job_master.take_retired_rescale_counts.return_value = {}

    def fail_after_commit(*_args):
        manager.job_master.execution_graph = resized_graph
        raise RuntimeError("checkpoint gate response was lost")

    manager.run_exclusive = AsyncMock(side_effect=fail_after_commit)
    error = await manager._run_operator_rescale(2, 3, resized_logical)

    assert "checkpoint gate response was lost" in error
    assert manager.logical_graph is resized_logical
    assert manager.execution_graph is resized_graph
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_local_rescale_accepts_multiple_physical_sources() -> None:
    logical, physical = _graphs(source_parallelism=2)
    _mark_running(physical)
    for source in physical.source_execution_vertices:
        source.stream_task = MagicMock()
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    resized_graph = physical.resize_operator(2, 3)
    manager.job_master = Mock(execution_graph=resized_graph)
    manager.job_master.take_retired_rescale_counts.return_value = {}
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING
    manager.run_exclusive = AsyncMock(return_value=None)

    result = await manager.rescale_operator(2, 3)

    assert result["status"] == "COMPLETED"
    assert result["error"] is None
    master = JobMaster(physical, Configuration(include_environment=False))
    master.coordinator = MagicMock()
    with patch.object(master, "_coordinator_alive", return_value=True):
        master._validate_local_rescale(RescalePlan.build(physical, 2, 3, "resize-1"))
    manager._writer.shutdown(wait=False)


def test_stabilization_excludes_disconnected_dataflow_components() -> None:
    selected_source = SimpleNamespace(id=ExecutionVertexId(1, 0))
    unrelated_source = SimpleNamespace(id=ExecutionVertexId(4, 0))
    selected_sink = SimpleNamespace(id=ExecutionVertexId(3, 0))
    unrelated_sink = SimpleNamespace(id=ExecutionVertexId(6, 0))
    selected_domain = CheckpointDomain(
        "selected",
        (selected_source.id, ExecutionVertexId(2, 0), selected_sink.id),
        (selected_source.id,),
        (selected_sink.id,),
    )
    unrelated_domain = CheckpointDomain(
        "unrelated",
        (unrelated_source.id, ExecutionVertexId(5, 0), unrelated_sink.id),
        (unrelated_source.id,),
        (unrelated_sink.id,),
    )
    graph = SimpleNamespace(
        source_execution_vertices=(selected_source, unrelated_source),
        sink_execution_vertices=(selected_sink, unrelated_sink),
        checkpoint_domains_for_job_vertex=Mock(
            side_effect=lambda target: (selected_domain,) if target == 2 else (unrelated_domain,)
        ),
    )

    assert JobMaster._rescale_stabilization_sources(graph, 2) == (selected_source,)
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = graph
    coordinator._rescale_operation_id = "resize-1"
    assert coordinator.finish_operator_rescale("resize-1", committed=True, target_job_vertex_id=2) is True
    assert coordinator._rescale_recovery_pending_sources == {selected_source.id}
    assert coordinator._rescale_recovery_pending_sinks == {selected_sink.id}


@pytest.mark.asyncio
async def test_rescale_readiness_excludes_disconnected_checkpoint_domains() -> None:
    config = Configuration(include_environment=False)
    builder = LogicalGraphBuilder("job", config)
    vertices = [
        _vertex("job", index, node_type, 1)
        for index, node_type in (
            (1, NodeType.SOURCE),
            (2, NodeType.TRANSFORM),
            (3, NodeType.SINK),
            (4, NodeType.SOURCE),
            (5, NodeType.TRANSFORM),
            (6, NodeType.SINK),
        )
    ]
    for vertex in vertices:
        builder.add_vertex(vertex)
    forward = ForwardPartitioner().to_spec()
    for source, target in ((1, 2), (2, 3), (4, 5), (5, 6)):
        builder.add_edge(EdgeSpec(vertices[source - 1].id, vertices[target - 1].id, forward))
    logical = builder.build()
    physical = ExecutionGraph.expand(logical, config, JobMetricGroup("job"), "job")
    selected_domain = physical.checkpoint_domains_for_job_vertex(2)[0]
    for execution_vertex_id in selected_domain.vertex_ids:
        vertex = physical.execution_vertex(execution_vertex_id)
        vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
        vertex.transition_to(ExecutionVertexStatus.RUNNING)

    manager = JobManager(config, namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = Mock()
    manager._job_config = config
    manager._job_status = JobStatus.RUNNING

    vertex_id, _logical_vertex = manager._resolve_rescale_request(2, 2)

    assert vertex_id.index == 2
    assert all(
        physical.execution_vertex(execution_vertex_id).status == ExecutionVertexStatus.CREATED
        for execution_vertex_id in physical.checkpoint_domains_for_job_vertex(5)[0].vertex_ids
    )
    manager.progress_snapshot = AsyncMock(
        return_value=ProgressSnapshot(
            operators=(
                OperatorProgress("op2", 2, 1, "RUNNING", 0),
                OperatorProgress("op5", 5, 1, "CREATED", 0),
            )
        )
    )
    manager._checkpoint_dashboard_snapshot = AsyncMock(return_value={})

    snapshot = await manager.dashboard_snapshot()
    operators = {operator["op_id"]: operator for operator in snapshot["operators"]}

    assert operators[2]["can_rescale"] is True
    assert operators[5]["can_rescale"] is False
    assert "CheckpointDomain" in operators[5]["rescale_disabled_reason"]
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_dashboard_allows_console_sink_rescale_with_multiple_source_tasks() -> None:
    assert ConsoleSinkFunction.supports_concurrent_rescale is True

    logical, physical = _graphs(source_parallelism=2, console_sink=True)
    _mark_running(physical)
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.logical_graph = logical
    manager.execution_graph = physical
    manager.job_master = SimpleNamespace(
        coordinator=None,
        restart_window=lambda: (0, 0, 0),
    )
    manager._job_config = Configuration(include_environment=False)
    manager._job_status = JobStatus.RUNNING

    snapshot = await manager.dashboard_snapshot()
    operators = {operator["op_id"]: operator for operator in snapshot["operators"]}

    assert operators[1]["can_rescale"] is False
    assert "Source operators" in operators[1]["rescale_disabled_reason"]
    assert operators[2]["can_rescale"] is True
    assert operators[3]["name"] == "ConsoleSinkAll[3]"
    assert operators[3]["can_rescale"] is True
    assert operators[3]["rescale_disabled_reason"] is None
    assert manager._unsupported_rescale_reason(logical.get(VertexId("job", 3)), 3) is None
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
async def test_removed_then_recreated_task_rejects_a_stale_generation_status_report() -> None:
    logical_two, graph_two = _graphs(parallelism=2)
    logical_four = logical_two.rescale_operator(2, 4)
    first_graph_four = graph_two.rescale_operator(logical_four, 2)
    first_vertex = first_graph_four.job_vertex(2).execution_vertex(2)
    logical_two_again = logical_four.rescale_operator(2, 2)
    graph_two_again = first_graph_four.rescale_operator(logical_two_again, 2)
    logical_four_again = logical_two_again.rescale_operator(2, 4)
    current_graph = graph_two_again.rescale_operator(logical_four_again, 2)
    current_vertex = current_graph.job_vertex(2).execution_vertex(2)

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
async def test_topology_transaction_commits_once_and_rejects_conflicting_prepare() -> None:
    input_id = ExecutionVertexId(1, 0)
    previous = SimpleNamespace(
        vertex_id=ExecutionVertexId(2, 0),
        parallelism=1,
        task_name="target",
        task_generation="generation-1",
        operator=SimpleNamespace(source=False),
        out_edges=("old",),
        barrier_split={input_id: 1},
        input_vertex_ids=(input_id,),
    )
    pending = SimpleNamespace(
        vertex_id=previous.vertex_id,
        parallelism=previous.parallelism,
        task_name=previous.task_name,
        task_generation=previous.task_generation,
        operator=previous.operator,
        out_edges=("new",),
        barrier_split={input_id: 2},
        input_vertex_ids=(input_id, ExecutionVertexId(1, 1)),
    )
    output = MagicMock(spec=TaskOutput)
    checkpoint = MagicMock()
    tracker = MagicMock()
    task = _bare_rescale_task("target")
    task._descriptor = previous
    task._vertex_id = previous.vertex_id
    task._task_generation = previous.task_generation
    task._running = True
    task._task = asyncio.current_task()
    task._emit = None
    task._state = SimpleNamespace(
        output=output,
        checkpoint_strategy=checkpoint,
        event_time_tracker=tracker,
        metrics=MagicMock(),
        pipelined=False,
    )
    task._build_output_edge = MagicMock(return_value="replacement")
    task._configure_output_replay = MagicMock()

    assert await task.prepare_topology_reconfiguration("resize-1", pending) is True
    assert await task.prepare_topology_reconfiguration("resize-1", pending) is True
    with pytest.raises(RuntimeError, match="already active"):
        await task.prepare_topology_reconfiguration("resize-2", pending)

    assert task.activate_topology_reconfiguration("resize-1") is True
    assert task.activate_topology_reconfiguration("resize-1") is True
    assert task._descriptor is pending
    output.prepare_edge_swap.assert_called_once_with("resize-1", ["replacement"])
    output.activate_edge_swap.assert_called_once_with("resize-1")
    checkpoint.reconfigure_barrier_split.assert_called_once_with(
        dict(pending.barrier_split),
        pending.input_vertex_ids,
    )
    tracker.reconfigure_inputs.assert_called_once_with(pending.input_vertex_ids)

    assert task.commit_topology_reconfiguration("resize-1") is True
    assert task.commit_topology_reconfiguration("resize-1") is True
    assert task.rollback_topology_reconfiguration("resize-1") is False
    output.commit_edge_swap.assert_called_once_with("resize-1")


def test_topology_transaction_validates_actor_identity_and_lifecycle() -> None:
    vertex_id = ExecutionVertexId(2, 0)
    descriptor = SimpleNamespace(
        vertex_id=vertex_id,
        parallelism=2,
        task_name="target",
        task_generation="generation-1",
    )
    task = _bare_rescale_task("target")
    task._descriptor = descriptor
    task._vertex_id = vertex_id
    task._task_generation = descriptor.task_generation
    task._state = None
    task._running = False

    with pytest.raises(RuntimeError, match="not running"):
        task._validate_topology_reconfiguration(descriptor)

    task._state = SimpleNamespace()
    task._running = True
    for override, message in (
        ({"vertex_id": ExecutionVertexId(9, 0)}, "same execution vertex"),
        ({"parallelism": 3}, "cannot resize"),
        ({"task_name": "renamed"}, "cannot rename"),
        ({"task_generation": "generation-2"}, "cannot change"),
    ):
        candidate = SimpleNamespace(**(descriptor.__dict__ | override))
        with pytest.raises(ValueError, match=message):
            task._validate_topology_reconfiguration(candidate)


@pytest.mark.asyncio
async def test_rescale_participant_lifecycle_is_fenced_and_idempotent() -> None:
    task = _bare_rescale_task("target")
    task._descriptor = SimpleNamespace(input_vertex_ids=())
    task._begin_rescale("resize-1", "target")

    with pytest.raises(RuntimeError, match="already participates"):
        task._begin_rescale("resize-2", "target")
    with pytest.raises(ValueError, match="source operators"):
        task.prepare_rescale_target("resize-1")
    with pytest.raises(ValueError, match="at least one target input"):
        task.prepare_rescale_downstream("resize-1", ())
    with pytest.raises(ValueError, match="not participating"):
        await task.await_rescale_ready("resize-2", 0.01)
    assert task.resume_rescale("resize-2") is False

    task._topology_operation_id = "resize-1"
    task._topology_active = True
    task.commit_topology_reconfiguration = MagicMock(return_value=True)
    assert task.resume_rescale("resize-1") is True
    task.commit_topology_reconfiguration.assert_called_once_with("resize-1")
    assert task._rescale_operation_id is None
    assert task._rescale_tombstones == ["resize-1"]


@pytest.mark.asyncio
async def test_upstream_and_target_barriers_preserve_order_and_ignore_duplicates() -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    upstream = _bare_rescale_task("upstream")
    upstream._state = SimpleNamespace(
        async_runner=None,
        executor=executor,
        inbox=asyncio.Queue(),
        output=MagicMock(),
    )
    upstream._pump = Mock(flush_input=Mock())
    upstream._emit = AsyncMock()
    upstream._begin_rescale("resize-1", "upstream")
    upstream._rescale_edge_indices = (0,)
    barrier = RescaleBarrier("resize-1", 2)

    await upstream.handle_rescale_barrier(barrier, None)

    upstream._state.output.collect_to_edges.assert_called_once_with(barrier, (0,))
    assert upstream._rescale_ready.is_set()

    target = _bare_rescale_task("target")
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    output = MagicMock()
    operator = MagicMock(stateful=False)
    target._state = SimpleNamespace(
        async_runner=None,
        executor=executor,
        output=output,
        operator=operator,
        state_snapshot_cache=None,
    )
    target._pump = Mock(flush_input=Mock())
    target._emit = AsyncMock()
    target._begin_rescale("resize-1", "target")
    target._rescale_expected_senders = {first, second}

    await target.handle_rescale_barrier(barrier, first)
    await target.handle_rescale_barrier(barrier, first)
    assert target._rescale_ready.is_set() is False
    with pytest.raises(RuntimeError, match="unexpected rescale sender"):
        await target.handle_rescale_barrier(barrier, ExecutionVertexId(9, 0))

    await target.handle_rescale_barrier(barrier, second)

    assert target._rescale_ready.is_set()
    assert target._rescale_snapshot is None
    operator.flush.assert_called_once_with()
    output.flush.assert_called_once_with(force=True)
    output.collect.assert_called_once_with(barrier)

    target._rescale_tombstones.append("old-resize")
    await target.handle_rescale_barrier(RescaleBarrier("old-resize", 2), first)
    with pytest.raises(RuntimeError, match="unexpected rescale barrier"):
        await target.handle_rescale_barrier(RescaleBarrier("resize-2", 2), first)
    executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_prepare_upstream_requires_an_edge_and_enqueues_the_fence() -> None:
    task = _bare_rescale_task("upstream")
    task._state = SimpleNamespace(inbox=asyncio.Queue())

    with pytest.raises(ValueError, match="needs a target output edge"):
        await task.prepare_rescale_upstream("resize-1", 2, (), 0.01)

    waiting = asyncio.create_task(task.prepare_rescale_upstream("resize-1", 2, (1,), 1.0))
    envelope = await task._state.inbox.get()
    assert envelope.payload == RescaleBarrier("resize-1", 2)
    task._rescale_ready.set()
    assert await waiting is True


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
        ((dict(pending.barrier_split), pending.input_vertex_ids),),
        ((dict(previous.barrier_split), previous.input_vertex_ids),),
    ]
    assert tracker.reconfigure_inputs.call_args_list == [
        ((pending.input_vertex_ids,),),
        ((previous.input_vertex_ids,),),
    ]


def test_topology_rollback_is_idempotent_when_prepare_never_reached_the_actor() -> None:
    task = object.__new__(StreamTask)
    task._topology_commit_tombstones = []
    task._topology_operation_id = None

    assert task.rollback_topology_reconfiguration("resize-1") is True


def test_committed_release_resumes_retained_target_and_not_removed_target() -> None:
    logical_two, graph_two = _graphs()
    logical = logical_two.rescale_operator(2, 4)
    old_graph = graph_two.rescale_operator(logical, 2)
    for vertex in old_graph.execution_vertices:
        vertex.stream_task = MagicMock()
    new_logical = logical.rescale_operator(2, 2)
    new_graph = old_graph.rescale_operator(new_logical, 2)

    with patch.object(
        job_master_module.klein,
        "get",
        side_effect=lambda value, **_kwargs: [True] * len(value),
    ):
        JobMaster._release_committed_rescale(
            old_graph,
            new_graph.job_vertex(2),
            2,
            "resize-1",
            1,
        )

    for index, vertex in old_graph.job_vertex(2).execution_vertices.items():
        if index < 2:
            vertex.stream_task.resume_rescale.assert_called_once_with("resize-1")
        else:
            vertex.stream_task.resume_rescale.assert_not_called()
    for job_vertex_id in (1, 3):
        for vertex in old_graph.job_vertex(job_vertex_id).execution_vertices.values():
            vertex.stream_task.resume_rescale.assert_called_once_with("resize-1")
    for index, vertex in new_graph.job_vertex(2).execution_vertices.items():
        assert vertex.stream_task is old_graph.job_vertex(2).execution_vertex(index).stream_task


def test_checkpoint_gate_release_rejects_a_false_coordinator_response() -> None:
    _logical, graph = _graphs()
    master = JobMaster(graph, Configuration(include_environment=False))
    master.coordinator = MagicMock()

    with (
        patch.object(job_master_module.klein, "get", return_value=False) as get,
        pytest.raises(RuntimeError, match="failed to release checkpoint gate"),
    ):
        master._finish_local_rescale_gate("resize-1", committed=True, timeout=1)

    assert get.call_count == 3


def test_incomplete_rescale_rollback_stays_fenced_and_forces_global_recovery() -> None:
    _logical, old_graph = _graphs()
    master = JobMaster(old_graph, Configuration(include_environment=False))
    recovery = MagicMock()
    master._recovery = recovery
    plan = RescalePlan.build(old_graph, 2, 3, "resize-1")
    transaction = RescaleTransaction(plan, phase=RescalePhase.COORDINATOR)
    placement_transition = (
        NativeStrategy()
        .plan(old_graph)
        .begin_rescale(
            plan.new_graph,
            added=plan.delta.added,
            removed=plan.delta.removed,
        )
    )

    with (
        patch.object(master, "_restore_precommit_topologies", return_value=False),
        patch.object(master, "_restore_precommit_runtime", return_value=True),
        patch.object(master, "_replace_recovery_graph"),
        patch.object(master, "_discard_local_rescale_state"),
        patch.object(master, "_finish_local_rescale_gate") as finish_gate,
        patch.object(master, "_release_rescale_participants") as release,
        pytest.raises(RuntimeError, match="global recovery is required"),
    ):
        master._rollback_local_rescale(
            plan,
            Deadline(1),
            transaction,
            placement_transition,
        )

    recovery.require_global_recovery.assert_called_once()
    finish_gate.assert_called_once()
    assert finish_gate.call_args.args == ("resize-1",)
    assert finish_gate.call_args.kwargs["committed"] is True
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


def test_exhausted_source_rejects_checkpoint_before_finished_status_is_visible() -> None:
    task = object.__new__(SourceStreamTask)
    task._running = True
    task._eof_reached = False
    task._source_exhausted = threading.Event()
    task._source_exhausted.set()
    task._forced_checkpoint_requested = threading.Event()

    assert task.request_checkpoint() is False
    assert task._forced_checkpoint_requested.is_set() is False


def test_source_rescale_resume_retry_accepts_a_completed_operation() -> None:
    task = object.__new__(SourceStreamTask)
    task._rescale_tombstones = ["resize-1"]
    task._rescale_operation_id = None
    task._source_rescale_resume = threading.Event()

    assert task.resume_rescale("resize-1") is True
    assert task._source_rescale_resume.is_set() is False


def test_source_joins_coordinator_assigned_epoch_at_next_boundary() -> None:
    task = object.__new__(SourceStreamTask)
    task._running = True
    task._eof_reached = False
    task._drain_requested = False
    task._inflight_source_states = {}
    task._requested_checkpoint_ids = deque()
    task._checkpoint_request_lock = threading.Lock()
    task._resolved_checkpoint_floor = 0
    task._forced_checkpoint_requested = threading.Event()
    task._forced_checkpoint_requested.set()
    task._source_rescale_requested = threading.Event()
    strategy = SimpleNamespace(
        should_trigger=Mock(return_value=False),
        reset_trigger=Mock(),
    )
    task._state = SimpleNamespace(
        operator=SimpleNamespace(end_of_stream=False),
        checkpoint_strategy=strategy,
        metrics=SimpleNamespace(barriers_out=Mock()),
    )
    barrier = Barrier(7, ExecutionVertexId(1, 0))
    task._generate_barrier = Mock(return_value=barrier)

    assert task.request_checkpoint(7) is True
    assert task._on_records_emitted(record_emitted=False) is barrier

    task._generate_barrier.assert_called_once_with(checkpoint_id=7)
    strategy.reset_trigger.assert_called_once_with()
    task._state.metrics.barriers_out.inc.assert_called_once_with()
    assert task._forced_checkpoint_requested.is_set() is False


def test_source_drops_coordinator_epoch_canceled_before_start() -> None:
    strategy = object.__new__(AlignedCheckpointStrategy)
    strategy._vertex_id = ExecutionVertexId(1, 0)
    strategy._coordinator = SimpleNamespace(
        source_checkpoint_started=Mock(return_value=False),
    )

    assert strategy.generate_next_barrier(checkpoint_id=7) is None
    strategy._coordinator.source_checkpoint_started.assert_called_once_with(7, is_eof=False)


def test_source_retries_canceled_terminal_epoch_after_servicing_rescale() -> None:
    task = object.__new__(SourceStreamTask)
    task._checkpoint_wait_stop = threading.Event()
    task._emit_pending_rescale_barrier = Mock(return_value=False)
    replacement = Barrier(8, ExecutionVertexId(1, 0))
    task._generate_barrier = Mock(side_effect=(None, replacement))
    task._pop_requested_checkpoint = Mock(return_value=None)

    with patch("ray.klein.runtime.worker.source_stream_task.time.sleep", return_value=None):
        assert task._await_terminal_barrier(7) is replacement

    assert [invocation.kwargs for invocation in task._generate_barrier.call_args_list] == [
        {"is_eof": True, "force": True, "checkpoint_id": 7},
        {"is_eof": True, "force": True, "checkpoint_id": None},
    ]
    assert task._emit_pending_rescale_barrier.call_count == 2
