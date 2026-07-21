# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.job_manager.job_manager import JobManager
from ray.klein.runtime.job_manager.progress import ProgressSnapshot, SubtaskCounts
from ray.klein.runtime.job_manager.progress_reporter import ProgressReporter


class _CountsHandle:
    def __init__(self, counts: SubtaskCounts) -> None:
        self.counts = counts

    def progress_counts(self) -> SubtaskCounts:
        return self.counts


class _Graph:
    def __init__(self, vertices: list[SimpleNamespace]) -> None:
        job_vertex = SimpleNamespace(
            id=7,
            name="map",
            concurrency=len(vertices),
            resources=SimpleNamespace(cpus=1.0, gpus=0.0),
            execution_vertices={vertex.index: vertex for vertex in vertices},
        )
        self.job_vertices = {job_vertex.id: job_vertex}

    @property
    def execution_vertices(self) -> tuple[SimpleNamespace, ...]:
        return tuple(self.job_vertices[7].execution_vertices.values())

    @staticmethod
    def downstream_job_vertices(_job_vertex_id: int) -> tuple[int, ...]:
        return ()


def _counts(seed: int) -> SubtaskCounts:
    return SubtaskCounts(
        rows_in=seed + 1,
        rows_out=seed + 2,
        bytes_in=seed + 3,
        bytes_out=seed + 4,
        queued=seed + 5,
        capacity=seed + 6,
        busy_ns=seed + 7,
        backpressure_ns=seed + 8,
        backpressure_events=seed + 9,
        barriers_in=seed + 10,
        barriers_out=seed + 11,
        checkpoint_alignment_ms=float(seed + 12),
        checkpoint_barrier_latency_ms=float(seed + 13),
        checkpoint_state_size_bytes=seed + 14,
        last_checkpoint_id=seed + 15,
    )


def _vertex(index: int, handle: _CountsHandle) -> SimpleNamespace:
    return SimpleNamespace(
        id=ExecutionVertexId(7, index),
        index=index,
        status=ExecutionVertexStatus.RUNNING,
        stream_task=handle,
    )


@pytest.mark.asyncio
async def test_scale_in_retains_removed_cumulative_counts_but_only_lists_live_subtasks() -> None:
    handles = [_CountsHandle(_counts(index * 100)) for index in range(4)]
    old_graph = _Graph([_vertex(index, handle) for index, handle in enumerate(handles)])
    reporter = ProgressReporter(old_graph, lambda: True, lambda: (0, 0, 0))

    before = (await reporter.snapshot()).operators[0]
    handles[0].counts = _counts(1_000)
    handles[1].counts = _counts(1_100)
    new_graph = _Graph([_vertex(index, handles[index]) for index in range(2)])
    final_removed_counts = {
        ExecutionVertexId(7, 2): _counts(250),
        ExecutionVertexId(7, 3): _counts(350),
    }

    await reporter.replace_execution_graph(new_graph, final_removed_counts)
    after = (await reporter.snapshot()).operators[0]

    cumulative_fields = (
        "rows_in",
        "rows_out",
        "bytes_in",
        "bytes_out",
        "busy_ns",
        "backpressure_ns",
        "backpressure_events",
        "barriers_in",
        "barriers_out",
    )
    for field_name in cumulative_fields:
        expected = sum(getattr(handles[index].counts, field_name) for index in range(2)) + sum(
            getattr(final_removed_counts[ExecutionVertexId(7, index)], field_name) for index in (2, 3)
        )
        assert getattr(after, field_name) == expected
        assert getattr(after, field_name) >= getattr(before, field_name)

    assert after.queued == sum(handles[index].counts.queued for index in range(2))
    assert after.capacity == sum(handles[index].counts.capacity for index in range(2))
    assert after.checkpoint_state_size_bytes == sum(
        handles[index].counts.checkpoint_state_size_bytes for index in range(2)
    )
    assert tuple(subtask.subtask_index for subtask in after.subtasks) == (0, 1)


@pytest.mark.asyncio
async def test_job_manager_migrates_the_existing_reporter_after_commit() -> None:
    old_graph = Mock()
    new_graph = Mock(spec=ExecutionGraph)
    new_graph.job_vertex.return_value.concurrency = 3
    logical_graph = Mock()
    reporter = Mock()
    reporter.replace_execution_graph = AsyncMock()
    manager = JobManager(Configuration(include_environment=False), namespace="job")
    manager.execution_graph = old_graph
    manager.logical_graph = Mock()
    manager.job_master = SimpleNamespace(
        rescale_operator=Mock(),
        execution_graph=new_graph,
        take_retired_rescale_counts=Mock(return_value={}),
    )
    manager._progress_reporter = reporter
    manager.progress_snapshot = AsyncMock(return_value=ProgressSnapshot())
    manager.run_exclusive = AsyncMock(return_value=None)

    error = await manager._run_operator_rescale(7, 3, logical_graph)

    assert error is None
    manager.progress_snapshot.assert_awaited_once_with()
    reporter.replace_execution_graph.assert_awaited_once_with(new_graph, {})
    assert manager._progress_reporter is reporter
    assert manager.execution_graph is new_graph
    manager._writer.shutdown(wait=False)
