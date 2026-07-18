# SPDX-License-Identifier: Apache-2.0
import hashlib
from pathlib import Path
from unittest.mock import Mock

import pytest

from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.integrations.filesystem._file_part import FilePart
from ray.klein.integrations.filesystem.file_sink_committable import FileSinkCommittable
from ray.klein.runtime.coordinator import checkpoint_coordinator as coordinator_module
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.runtime.coordinator.checkpoint import Checkpoint
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem
from ray.klein.state.sink_committable_checkpoint_entry import SinkCommittableCheckpointEntry
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry
from ray.klein.state.state_snapshot_reference import StateSnapshotReference


@pytest.mark.parametrize(("needs_recovery", "expected"), [(False, True), (True, False)])
def test_coordinator_health_requires_an_opened_actor(monkeypatch, needs_recovery: bool, expected: bool) -> None:
    class _Coordinator:
        @staticmethod
        def needs_recovery() -> bool:
            return needs_recovery

    coordinator = _Coordinator()
    monkeypatch.setattr(
        CheckpointCoordinator,
        "find",
        staticmethod(lambda namespace: coordinator),
    )
    monkeypatch.setattr(coordinator_module.klein, "get", lambda value: value)

    assert CheckpointCoordinator.coordinator_healthy("job-a") is expected


@pytest.mark.asyncio
async def test_coordinator_discovers_latest_checkpoint_for_its_job(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    expected_state = SourceCheckpointEntry(task_key="11:0", checkpoint_id=7, state={"offset": 7})
    checkpoint_path = checkpoint_io.write_checkpoint(
        [expected_state],
        5,
        root_uri,
        barrier_high_water=41,
        job_id="job-a",
    )
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, root_uri)
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    execution_graph = Mock()
    execution_graph.barrier_splits = {}
    execution_graph.sink_execution_vertices = []

    await coordinator.open(execution_graph, restore_path=None)

    assert coordinator.latest_checkpoint_path() == checkpoint_path
    assert await coordinator.source_state(ExecutionVertexId(11, 0)) == expected_state
    assert coordinator.barrier_epoch_floor() > 41


@pytest.mark.asyncio
async def test_coordinator_restores_all_subtask_fragments_for_rescale(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    checkpoint_path = checkpoint_io.write_checkpoint(
        [],
        1,
        root_uri,
        job_id="job-a",
        operator_states={"2:0": b"left", "2:1": b"right", "3:0": b"other"},
    )
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, root_uri)
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    execution_graph = Mock()
    execution_graph.barrier_splits = {}
    execution_graph.sink_execution_vertices = []
    await coordinator.open(execution_graph, restore_path=checkpoint_path)

    references = await coordinator.durable_operator_states(ExecutionVertexId(2, 0))

    assert [reference.materialize(lambda _: b"") for reference in references] == [b"left", b"right"]

    payload = b"new-parallelism"
    replacement = StateSnapshotReference(
        size_bytes=len(payload),
        checksum=f"sha256:{hashlib.sha256(payload).hexdigest()}",
        inline_payload=payload,
    )
    coordinator._replace_logical_operator_states({"2:0": replacement})

    latest = await coordinator.latest_operator_states(ExecutionVertexId(2, 0))
    assert [reference.materialize(lambda _: b"") for reference in latest] == [payload]


@pytest.mark.asyncio
async def test_source_is_notified_only_after_its_state_is_durable(tmp_path: Path):
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    source_task = Mock()
    source_vertex = Mock(id=ExecutionVertexId(11, 0), stream_task=source_task)
    execution_graph = Mock()
    execution_graph.barrier_splits = {}
    execution_graph.sink_execution_vertices = []
    execution_graph.source_execution_vertices = [source_vertex]
    await coordinator.open(execution_graph, restore_path=None)

    state = SourceCheckpointEntry(task_key="11:0", checkpoint_id=7, state={"offset": 7})
    coordinator._update_latest_source_state(state)

    await coordinator.persist_now()
    await coordinator.persist_now()

    source_task.notify_source_checkpoint_persisted.assert_called_once_with(7)
    restored_id, restored_states, _high_water = checkpoint_io.restore_checkpoint(
        coordinator.latest_checkpoint_path(),
    )
    assert restored_id == 1
    assert restored_states == [state]


@pytest.mark.asyncio
async def test_durable_source_notification_is_replayed_after_recovery(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    state = SourceCheckpointEntry(task_key="11:0", checkpoint_id=7, state={"offset": 7})
    checkpoint_path = checkpoint_io.write_checkpoint([state], 3, root_uri, job_id="job-a")
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, root_uri)
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    source_task = Mock()
    source_vertex = Mock(id=ExecutionVertexId(11, 0), stream_task=source_task)
    execution_graph = Mock()
    execution_graph.barrier_splits = {}
    execution_graph.sink_execution_vertices = []
    execution_graph.source_execution_vertices = [source_vertex]
    await coordinator.open(execution_graph, restore_path=checkpoint_path)

    await coordinator.persist_now()

    source_task.notify_source_checkpoint_persisted.assert_called_once_with(7)


@pytest.mark.asyncio
async def test_durable_sink_transaction_is_committed_after_coordinator_recovery(tmp_path: Path) -> None:
    output_uri = (tmp_path / "output").as_uri()
    output_filesystem = CheckpointFileSystem(output_uri)
    pending_path = ".klein-staging/job/part.pending"
    output_filesystem.write_bytes(pending_path, b'{"id":1}\n')
    committable = FileSinkCommittable(
        root_uri=output_uri,
        storage_options=None,
        parts=(FilePart(pending_path, "part-00000.json"),),
        _transaction_id="job-0-chk-7",
    )
    checkpoint_path = checkpoint_io.write_checkpoint(
        [],
        1,
        (tmp_path / "checkpoints").as_uri(),
        job_id="job-a",
        sink_committables=(SinkCommittableCheckpointEntry("2:0", 7, committable),),
    )
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    execution_graph = Mock()
    execution_graph.barrier_splits = {}
    execution_graph.sink_execution_vertices = []

    await coordinator.open(execution_graph, restore_path=checkpoint_path)

    assert output_filesystem.read_bytes("part-00000.json") == b'{"id":1}\n'
    assert not output_filesystem.exists(pending_path)
    # The durable metadata may be replayed until a later checkpoint removes the
    # committable, so a second recovery must remain harmless.
    recovered_again = CheckpointCoordinator(config, job_id="job-a")
    await recovered_again.open(execution_graph, restore_path=checkpoint_path)


@pytest.mark.asyncio
async def test_terminal_flush_aborts_inflight_sink_transactions(tmp_path: Path) -> None:
    output_uri = (tmp_path / "output").as_uri()
    output_filesystem = CheckpointFileSystem(output_uri)
    pending_path = ".klein-staging/job/part.pending"
    output_filesystem.write_bytes(pending_path, b"uncommitted")
    committable = FileSinkCommittable(
        root_uri=output_uri,
        storage_options=None,
        parts=(FilePart(pending_path, "part-00000.json"),),
        _transaction_id="job-0-chk-7",
    )
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    execution_graph = Mock()
    execution_graph.barrier_splits = {}
    execution_graph.sink_execution_vertices = []
    await coordinator.open(execution_graph, restore_path=None)
    coordinator._inflight_sink_committables = {7: {"2:0": SinkCommittableCheckpointEntry("2:0", 7, committable)}}

    await coordinator.persist_now(notify_sources=False, abort_inflight_sinks=True)

    assert not output_filesystem.exists(pending_path)
    assert not output_filesystem.exists("part-00000.json")


def test_coordinator_rejects_zero_retained_checkpoints(tmp_path: Path):
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    config.set(CheckpointOptions.RETAINED_COUNT, 0)

    with pytest.raises(ValueError, match="num-retained"):
        CheckpointCoordinator(config, job_id="job-a")


def test_dashboard_snapshot_includes_per_operator_checkpoint_metrics(tmp_path: Path) -> None:
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    coordinator = CheckpointCoordinator(config, job_id="job-a")
    checkpoint = Checkpoint(7, 1, (ExecutionVertexId(1, 0),))
    checkpoint.mark_in_progress()
    coordinator._checkpoint_history.append(checkpoint)
    coordinator._inflight_checkpoints[7] = checkpoint
    execution_graph = Mock()
    job_vertex = Mock()
    job_vertex.name = "source"
    execution_graph.job_vertex.return_value = job_vertex
    coordinator._execution_graph = execution_graph

    assert coordinator.register_operator_checkpoint_metrics(
        7,
        ExecutionVertexId(1, 0),
        {
            "alignment_duration_ms": 12.5,
            "barrier_latency_ms": 30,
            "state_size_bytes": 1024,
            "rows_in": 10,
            "rows_out": 20,
        },
    )

    row = coordinator.dashboard_snapshot()["history"][0]
    operator = row["operators"][0]
    assert row["state_size_bytes"] == 1024
    assert row["alignment_duration_ms"] == 12.5
    assert row["barrier_latency_ms"] == 30
    assert operator["op_id"] == 1
    assert operator["state_size_bytes"] == 1024
    assert operator["subtasks"][0]["subtask_index"] == 0


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        (CheckpointOptions.PERSISTENCE_INTERVAL, -1, "persistence-interval"),
        (CheckpointOptions.MAX_CONCURRENT, 0, "max-concurrent-checkpoints"),
        (CheckpointOptions.TIMEOUT, -1, "checkpointing.timeout"),
        (CheckpointOptions.HISTORY_SIZE, 0, "max-history-size"),
        (CheckpointOptions.MAX_CONCURRENT, True, "must be an integer"),
    ],
)
def test_coordinator_rejects_invalid_checkpoint_limits(tmp_path: Path, option, value, message: str):
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    config.set(option, value)

    with pytest.raises((TypeError, ValueError), match=message):
        CheckpointCoordinator(config, job_id="job-a")
