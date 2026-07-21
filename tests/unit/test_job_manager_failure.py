# SPDX-License-Identifier: Apache-2.0
import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from ray.klein.api.job_status import JobStatus
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.job_manager.job_manager import JobManager


@pytest.mark.asyncio
async def test_failure_detail_preserves_the_error_that_triggered_restart() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    vertex_id = ExecutionVertexId(2, 0)
    vertex = Mock(error_message=None)
    vertex.name = "map (1/1)"
    graph = Mock(execution_vertices=[vertex])
    graph.find_execution_vertex.return_value = vertex
    manager.execution_graph = graph
    manager.job_master = Mock()
    manager.run_exclusive = AsyncMock(return_value=(True, False, vertex.name))

    await manager.update_stream_task_status(
        vertex_id,
        ExecutionVertexStatus.FAILED,
        "ValueError: user-code failure",
    )
    # A collateral teardown error can replace the vertex's transient status,
    # but the original diagnostic remains the root cause shown to the user.
    vertex.error_message = "ActorDiedError: downstream was killed during restart"

    detail = await manager.failure_detail()

    assert "ValueError: user-code failure" in detail
    assert "ActorDiedError" not in detail
    manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_finished_status_returns_before_scheduled_teardown() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager.job_name = "orders"
    manager.job_master = Mock()
    manager._job_status = JobStatus.RUNNING
    manager.run_exclusive = AsyncMock(return_value=(True, True, "sink (1)"))
    teardown_started = asyncio.Event()
    allow_teardown = asyncio.Event()

    async def teardown() -> None:
        teardown_started.set()
        await allow_teardown.wait()

    manager._stop_job = AsyncMock(side_effect=teardown)
    try:
        await manager.update_stream_task_status(
            ExecutionVertexId(2, 0),
            ExecutionVertexStatus.FINISHED,
        )

        completion_task = manager._completion_task_obj
        assert completion_task is not None
        await teardown_started.wait()
        assert manager.job_status() is JobStatus.RUNNING

        allow_teardown.set()
        await completion_task
        assert manager.job_status() is JobStatus.FINISHED
        manager._stop_job.assert_awaited_once_with()
    finally:
        allow_teardown.set()
        if manager._completion_task_obj is not None:
            await manager._completion_task_obj
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_finished_teardown_failure_is_reported_as_job_failure() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager.job_name = "orders"
    manager.job_master = Mock()
    manager._job_status = JobStatus.RUNNING
    manager.run_exclusive = AsyncMock(return_value=(True, True, "sink (1)"))
    manager._stop_job = AsyncMock(side_effect=RuntimeError("actor survived teardown"))
    try:
        await manager.update_stream_task_status(
            ExecutionVertexId(2, 0),
            ExecutionVertexStatus.FINISHED,
        )
        completion_task = manager._completion_task_obj
        assert completion_task is not None
        await completion_task

        assert manager.job_status() is JobStatus.FAILED
        assert await manager.failure_detail() == "RuntimeError: actor survived teardown"
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_late_status_report_is_ignored_after_lifecycle_teardown_starts() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager.job_master = Mock()
    manager.run_exclusive = AsyncMock()
    manager._lifecycle_stop_requested = True
    try:
        await manager.update_stream_task_status(
            ExecutionVertexId(2, 0),
            ExecutionVertexStatus.FINISHED,
        )

        manager.run_exclusive.assert_not_awaited()
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_lifecycle_fence_stops_supervisor_instead_of_busy_looping() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager._lifecycle_stop_requested = True
    try:
        await manager._run()

        assert manager._stopping is True
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_permanent_failure_fences_status_reports_before_teardown() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager._job_status = JobStatus.RUNNING

    async def teardown(*, force: bool) -> None:
        assert manager._lifecycle_stop_requested is True
        assert manager._wake_event.is_set()

    manager._stop_job = AsyncMock(side_effect=teardown)
    try:
        await manager._fail_permanently(force=True)

        manager._stop_job.assert_awaited_once_with(force=True)
        assert manager.job_status() is JobStatus.FAILED
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_permanent_failure_records_teardown_error_and_still_becomes_failed() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager.job_name = "orders"
    manager._job_status = JobStatus.RUNNING
    manager._stop_job = AsyncMock(side_effect=RuntimeError("actor survived teardown"))
    try:
        await manager._fail_permanently(force=True)

        assert manager._lifecycle_stop_requested is True
        assert manager._wake_event.is_set()
        assert manager.job_status() is JobStatus.FAILED
        assert await manager.failure_detail() == "RuntimeError: actor survived teardown"
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_permanent_failure_retains_prior_diagnostic_when_teardown_also_fails() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager._job_status = JobStatus.RUNNING
    manager._submission_error = "ValueError: original deployment failure"
    manager._stop_job = AsyncMock(side_effect=RuntimeError("actor survived teardown"))
    try:
        await manager._fail_permanently(force=True)

        assert manager.job_status() is JobStatus.FAILED
        detail = await manager.failure_detail()
        assert detail is not None
        assert detail.startswith("ValueError: original deployment failure")
        assert "RuntimeError: actor survived teardown" in detail
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_supervisor_failure_teardown_error_still_marks_job_failed() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager.job_name = "orders"
    manager._job_status = JobStatus.RUNNING
    manager._stop_job = AsyncMock(side_effect=RuntimeError("actor survived teardown"))
    try:
        await manager._terminate_after_failure()

        manager._stop_job.assert_awaited_once_with(force=True)
        assert manager._lifecycle_stop_requested is True
        assert manager.job_status() is JobStatus.FAILED
        assert await manager.failure_detail() == "RuntimeError: actor survived teardown"
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_cancel_teardown_error_returns_false_and_marks_job_failed() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager.job_name = "orders"
    manager._job_status = JobStatus.RUNNING
    manager._stop_job = AsyncMock(side_effect=RuntimeError("actor survived teardown"))
    try:
        assert await manager.cancel(timeout=1) is False

        manager._stop_job.assert_awaited_once_with(force=True, timeout=1)
        assert manager.job_status() is JobStatus.FAILED
        assert await manager.failure_detail() == "RuntimeError: actor survived teardown"
    finally:
        manager._writer.shutdown(wait=False)


@pytest.mark.asyncio
async def test_submit_retains_deployment_error_when_cleanup_also_fails() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    manager._stop_job = AsyncMock(side_effect=RuntimeError("actor survived teardown"))
    manager.run_exclusive = AsyncMock(side_effect=ValueError("invalid placement"))
    logical_graph = Mock()

    with (
        pytest.MonkeyPatch.context() as monkeypatch,
    ):
        optimized = Mock()
        execution_graph = Mock()
        job_master = Mock()
        monkeypatch.setattr(
            "ray.klein.runtime.job_manager.job_manager.LogicalOptimizer.optimize",
            Mock(return_value=optimized),
        )
        monkeypatch.setattr(
            "ray.klein.runtime.job_manager.job_manager.ExecutionGraph.expand",
            Mock(return_value=execution_graph),
        )
        monkeypatch.setattr(
            "ray.klein.runtime.job_manager.job_manager.JobMaster",
            Mock(return_value=job_master),
        )
        try:
            assert await manager.submit("orders", logical_graph) is False

            assert manager._lifecycle_stop_requested is True
            assert manager.job_status() is JobStatus.FAILED
            detail = await manager.failure_detail()
            assert detail is not None
            assert detail.startswith("ValueError: invalid placement")
            assert "Deployment teardown failed: RuntimeError: actor survived teardown" in detail
        finally:
            manager._writer.shutdown(wait=False)
