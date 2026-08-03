# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace

import pytest

from ray.klein.api.job_status import JobStatus
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.job_manager import progress_reporter as progress_reporter_module
from ray.klein.runtime.job_manager.progress import OperatorProgress
from ray.klein.runtime.job_manager.progress_reporter import ProgressReporter


class _Graph:
    def __init__(self, statuses: list[ExecutionVertexStatus]) -> None:
        vertices = {
            index: SimpleNamespace(
                id=ExecutionVertexId(7, index),
                index=index,
                status=status,
                stream_task=None,
            )
            for index, status in enumerate(statuses)
        }
        self.job_vertices = {
            7: SimpleNamespace(
                id=7,
                name="map",
                concurrency=len(vertices),
                resources=SimpleNamespace(cpus=1.0, gpus=0.0),
                execution_vertices=vertices,
            )
        }

    @staticmethod
    def downstream_job_vertices(_job_vertex_id: int) -> tuple[int, ...]:
        return ()


async def _operator_for(
    statuses: list[ExecutionVertexStatus],
    job_status: JobStatus,
) -> OperatorProgress:
    reporter = ProgressReporter(
        _Graph(statuses),
        job_status=lambda: job_status,
        restart_window=lambda: (0, 0, 0),
    )
    return (await reporter.snapshot()).operators[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("parallelism", [1, 3], ids=["single", "all"])
async def test_cancelled_vertices_are_not_reported_as_pending(parallelism: int) -> None:
    operator = await _operator_for(
        [ExecutionVertexStatus.CANCELLED] * parallelism,
        JobStatus.CANCELLED,
    )

    assert operator.status == "cancelled"
    assert operator.instances.cancelled == parallelism
    assert operator.instances.pending == 0
    assert [subtask.status for subtask in operator.subtasks] == ["cancelled"] * parallelism


@pytest.mark.asyncio
async def test_explicit_cancelled_vertex_is_cancelled_while_job_is_running() -> None:
    operator = await _operator_for([ExecutionVertexStatus.CANCELLED], JobStatus.RUNNING)

    assert operator.status == "cancelled"
    assert operator.instances.cancelled == 1
    assert operator.instances.pending == 0
    assert operator.subtasks[0].status == "cancelled"


@pytest.mark.asyncio
async def test_one_cancelled_vertex_makes_a_partially_finished_operator_cancelled() -> None:
    operator = await _operator_for(
        [ExecutionVertexStatus.CANCELLED, ExecutionVertexStatus.FINISHED],
        JobStatus.CANCELLED,
    )

    assert operator.status == "cancelled"
    assert operator.instances.cancelled == 1
    assert operator.instances.finished == 1
    assert operator.instances.pending == 0
    assert [subtask.status for subtask in operator.subtasks] == ["cancelled", "finished"]


@pytest.mark.asyncio
async def test_cancelled_job_projects_unstarted_vertices_as_cancelled() -> None:
    operator = await _operator_for(
        [
            ExecutionVertexStatus.CREATED,
            ExecutionVertexStatus.DEPLOYED,
            ExecutionVertexStatus.CANCELLING,
        ],
        JobStatus.CANCELLED,
    )

    assert operator.status == "cancelled"
    assert operator.instances.cancelled == 3
    assert operator.instances.pending == 0
    assert [subtask.status for subtask in operator.subtasks] == ["cancelled"] * 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("vertex_status", "expected_status", "count_field"),
    [
        (ExecutionVertexStatus.FINISHED, "finished", "finished"),
        (ExecutionVertexStatus.FAILED, "failed", "failed"),
    ],
)
async def test_cancelled_job_preserves_terminal_vertex_outcomes(
    vertex_status: ExecutionVertexStatus,
    expected_status: str,
    count_field: str,
) -> None:
    operator = await _operator_for([vertex_status], JobStatus.CANCELLED)

    assert operator.status == expected_status
    assert getattr(operator.instances, count_field) == 1
    assert operator.instances.cancelled == 0
    assert operator.instances.pending == 0
    assert operator.subtasks[0].status == expected_status


@pytest.mark.asyncio
async def test_cancelled_job_preserves_finished_and_failed_vertex_outcomes() -> None:
    operator = await _operator_for(
        [
            ExecutionVertexStatus.FINISHED,
            ExecutionVertexStatus.FAILED,
            ExecutionVertexStatus.CREATED,
        ],
        JobStatus.CANCELLED,
    )

    assert operator.status == "failed"
    assert operator.instances.finished == 1
    assert operator.instances.failed == 1
    assert operator.instances.cancelled == 1
    assert operator.instances.pending == 0
    assert [subtask.status for subtask in operator.subtasks] == [
        "finished",
        "failed",
        "cancelled",
    ]


@pytest.mark.asyncio
async def test_running_vertex_with_failed_progress_rpc_stays_recovering(monkeypatch: pytest.MonkeyPatch) -> None:
    graph = _Graph([ExecutionVertexStatus.RUNNING])
    vertex = graph.job_vertices[7].execution_vertices[0]
    vertex.stream_task = SimpleNamespace(progress_counts=lambda: object())

    async def fail_progress_rpc(_request, timeout: float) -> None:
        raise RuntimeError("actor unavailable")

    monkeypatch.setattr(progress_reporter_module.klein, "aget", fail_progress_rpc)
    reporter = ProgressReporter(
        graph,
        job_status=lambda: JobStatus.RUNNING,
        restart_window=lambda: (0, 0, 0),
    )

    operator = (await reporter.snapshot()).operators[0]
    assert operator.status == "recovering"
    assert operator.instances.running == 0
    assert operator.instances.restarting == 1
    assert operator.instances.cancelled == 0
    assert operator.subtasks[0].status == "recovering"
