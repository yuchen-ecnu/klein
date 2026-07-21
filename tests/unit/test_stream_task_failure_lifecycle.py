# SPDX-License-Identifier: Apache-2.0
"""Failure-report lifecycle tests for the in-process debug actor runtime."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.worker.stream_task import StreamTask


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
