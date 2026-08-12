# SPDX-License-Identifier: Apache-2.0
"""Failure-report lifecycle tests for the in-process debug actor runtime."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.worker.stream_task import StreamTask


class _TrackingCommittable(SinkCommittable):
    def __init__(self, *, abort_error: Exception | None = None) -> None:
        self.abort_calls = 0
        self._abort_error = abort_error

    @property
    def transaction_id(self) -> str:
        return "transaction-1"

    def commit(self) -> None:
        pass

    def abort(self) -> None:
        self.abort_calls += 1
        if self._abort_error is not None:
            raise self._abort_error


def _sink_task(committable: SinkCommittable, register: Mock) -> StreamTask:
    task = object.__new__(StreamTask)
    task._task_name = "sink (1/1)"
    task._state = SimpleNamespace(
        operator=SimpleNamespace(prepare_checkpoint=Mock(return_value=committable)),
        checkpoint_strategy=SimpleNamespace(register_sink_committable=register),
    )
    return task


def test_sink_commit_registration_exception_aborts_prepared_transaction() -> None:
    committable = _TrackingCommittable()
    registration_error = ConnectionError("coordinator unavailable")
    task = _sink_task(committable, Mock(side_effect=registration_error))

    with pytest.raises(ConnectionError) as raised:
        task.prepare_sink_commit(7)

    assert raised.value is registration_error
    assert committable.abort_calls == 1


def test_sink_commit_abort_failure_does_not_mask_registration_exception() -> None:
    committable = _TrackingCommittable(abort_error=RuntimeError("abort failed"))
    registration_error = ConnectionError("coordinator unavailable")
    task = _sink_task(committable, Mock(side_effect=registration_error))

    with pytest.raises(ConnectionError) as raised:
        task.prepare_sink_commit(7)

    assert raised.value is registration_error
    assert committable.abort_calls == 1


@pytest.mark.asyncio
async def test_fresh_bootstrap_clears_terminal_runtime_flags() -> None:
    task = object.__new__(StreamTask)
    task._running = False
    task._eof_reached = True
    task._drain_requested = True
    task._descriptor = object()
    runtime = SimpleNamespace(context=object())
    task._build_runtime = AsyncMock(return_value=runtime)
    task._install_runtime = Mock()
    task._on_setup_done = AsyncMock()
    task.start = AsyncMock()

    await task.setup_and_run()

    assert task._eof_reached is False
    assert task._drain_requested is False
    assert task._running is True
    task._build_runtime.assert_awaited_once_with(task._descriptor)
    task._install_runtime.assert_called_once_with(runtime)
    task._on_setup_done.assert_awaited_once_with(runtime.context)
    task.start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_requests_drain_before_waiting_for_input_at_initial_end_of_stream() -> None:
    task = object.__new__(StreamTask)
    task._rescale_role = None
    task._committed_event_time_controls = None
    task._drain_requested = False
    task._state = SimpleNamespace(operator=SimpleNamespace(end_of_stream=True), executor=None)
    task._check_end_of_stream = Mock(return_value=True)
    task._pump = SimpleNamespace(run_once=AsyncMock())

    await task._run()

    task._check_end_of_stream.assert_called_once_with()
    task._pump.run_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_force_stop_cancels_and_reaps_an_inflight_failure_report() -> None:
    task = object.__new__(StreamTask)
    task._task_name = "map (1/1)"
    task._task_generation = 1
    task._vertex_id = ExecutionVertexId(2, 0)
    task._job_manager = SimpleNamespace(update_stream_task_status=lambda *_args, **_kwargs: object())
    task._failure_report_task_obj = None
    task._force_stop_requested = False
    task._retired_runtimes = []
    task._active_runtime = None
    task._runtime_rescale_transaction = None
    task.stop = AsyncMock()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocked_aget(_request):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    with (
        patch("ray.klein.runtime.worker.stream_task.current_exception_diagnostic", return_value="failure"),
        patch("ray.klein.runtime.worker.stream_task.klein.aget", side_effect=blocked_aget),
    ):
        task.handle_exception(RuntimeError("operator failed"))
        report_task = task._failure_report_task_obj
        assert report_task is not None
        await asyncio.wait_for(started.wait(), timeout=1)

        task.prepare_force_stop()
        await task._settle_force_stopped_failure_report(1)

    assert report_task.done()
    assert report_task.cancelled()
    assert cancelled.is_set()
    task.stop.assert_awaited_once_with()
    assert task._failure_report_task_obj is None


@pytest.mark.asyncio
async def test_stream_task_teardown_is_single_flight_and_shielded_from_a_cancelled_caller() -> None:
    task = object.__new__(StreamTask)
    task._task_name = "source (1/1)"
    task._stream_stop_task_obj = None
    started = asyncio.Event()
    release = asyncio.Event()

    async def cleanup(_timeout: float, *, release_rescale: bool) -> None:
        assert release_rescale is True
        started.set()
        await release.wait()

    task._run_stream_task_stop = AsyncMock(side_effect=cleanup)
    first = asyncio.create_task(task._stop_stream_task(1, release_rescale=True))
    await started.wait()
    second = asyncio.create_task(task._stop_stream_task(1, release_rescale=True))

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert task._stream_stop_task_obj is not None
    assert not task._stream_stop_task_obj.cancelled()

    release.set()
    await second
    task._run_stream_task_stop.assert_awaited_once_with(1, release_rescale=True)


@pytest.mark.asyncio
async def test_external_force_stop_does_not_deadlock_with_failure_report_finalizer() -> None:
    task = object.__new__(StreamTask)
    task._task_name = "map (1/1)"
    task._vertex_id = ExecutionVertexId(2, 0)
    task._task_generation = 1
    task._job_manager = SimpleNamespace(update_stream_task_status=lambda *_args, **_kwargs: object())
    task._failure_report_task_obj = None
    task._stream_stop_task_obj = None
    task._stream_stop_initiator_task_obj = None
    task._force_stop_requested = True
    report_started = asyncio.Event()

    async def blocked_aget(_request):
        report_started.set()
        await asyncio.Event().wait()

    async def cleanup(timeout: float, *, release_rescale: bool) -> None:
        assert release_rescale is True
        report_task = task._failure_report_task_obj
        assert report_task is not None
        report_task.cancel()
        await task._settle_force_stopped_failure_report(timeout)

    task._run_stream_task_stop = AsyncMock(side_effect=cleanup)
    with patch("ray.klein.runtime.worker.stream_task.klein.aget", side_effect=blocked_aget):
        report_task = asyncio.create_task(task._report_failure("failure"))
        task._failure_report_task_obj = report_task
        await report_started.wait()

        await asyncio.wait_for(task.stop(1), timeout=1)

    assert report_task.cancelled()
    task._run_stream_task_stop.assert_awaited_once_with(1, release_rescale=True)


@pytest.mark.asyncio
async def test_failure_report_initiated_force_stop_does_not_await_its_initiator() -> None:
    task = object.__new__(StreamTask)
    task._task_name = "map (1/1)"
    task._vertex_id = ExecutionVertexId(2, 0)
    task._task_generation = 1
    task._job_manager = SimpleNamespace(update_stream_task_status=lambda *_args, **_kwargs: object())
    task._failure_report_task_obj = None
    task._stream_stop_task_obj = None
    task._stream_stop_initiator_task_obj = None
    task._force_stop_requested = True

    async def cleanup(timeout: float, *, release_rescale: bool) -> None:
        assert release_rescale is True
        await task._settle_force_stopped_failure_report(timeout)

    task._run_stream_task_stop = AsyncMock(side_effect=cleanup)
    with patch("ray.klein.runtime.worker.stream_task.klein.aget", new=AsyncMock()):
        report_task = asyncio.create_task(task._report_failure("failure"))
        task._failure_report_task_obj = report_task

        await asyncio.wait_for(report_task, timeout=1)

    assert task._stream_stop_initiator_task_obj is report_task
    task._run_stream_task_stop.assert_awaited_once_with(30.0, release_rescale=True)
