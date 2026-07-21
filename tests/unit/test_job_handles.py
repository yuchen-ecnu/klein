# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import threading
from collections import deque
from unittest.mock import Mock

import pytest

import ray.klein as klein
from ray.klein.api.completed_job_handle import CompletedJobHandle
from ray.klein.api.job_status import JobStatus
from ray.klein.api.live_job_handle import LiveJobHandle
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.exceptions import KleinError
from ray.klein.observability import progress_view


class _OutputQueue:
    def __init__(self, values: list[object]) -> None:
        self._values = deque(values)
        self.shutdown_args: list[bool] = []

    def qsize(self) -> int:
        return len(self._values)

    def get_nowait(self) -> object:
        return self._values.popleft()

    def shutdown(self, *, force: bool) -> None:
        self.shutdown_args.append(force)


class _JobManager:
    def __init__(self, status: JobStatus = JobStatus.FINISHED) -> None:
        self.current_status = status
        self.queue = _OutputQueue([{"id": 1}, {"id": 2}])
        self.cancel_timeouts: list[int] = []
        self.wait_error: BaseException | None = None

    def wait_until_terminal(self) -> str:
        if self.wait_error is not None:
            raise self.wait_error
        return "terminal"

    def output_queue(self) -> _OutputQueue:
        return self.queue

    def job_status(self) -> JobStatus:
        return self.current_status

    def cancel(self, timeout: int) -> bool:
        self.cancel_timeouts.append(timeout)
        return True

    def failure_detail(self) -> str:
        return "worker exploded"

    def progress_snapshot(self) -> str:
        return "snapshot"


def _live_handle(manager: _JobManager, lineage: Mock | None = None) -> tuple[LiveJobHandle, Mock]:
    tracker = lineage or Mock()
    return (
        LiveJobHandle(
            manager,
            "orders",
            RuntimeExecutionMode.STREAMING,
            "klein-orders",
            tracker,
        ),
        tracker,
    )


def test_completed_job_handle_exposes_in_memory_result() -> None:
    result = [{"id": 1}]
    handle = CompletedJobHandle(result)

    assert handle.wait() is None
    assert handle.get() is result
    assert handle.status is JobStatus.FINISHED
    assert handle.cancel(timeout=0) is True
    assert handle.namespace is None


def test_live_job_handle_drains_output_and_delegates_control(monkeypatch) -> None:
    manager = _JobManager()
    handle, _ = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    assert handle.get() == [{"id": 1}, {"id": 2}]
    assert manager.queue.shutdown_args == [True]
    assert handle.status is JobStatus.FINISHED
    assert handle.cancel(timeout=7) is True
    assert manager.cancel_timeouts == [7]
    assert handle._progress_snapshot() == "snapshot"
    assert handle.namespace == "klein-orders"


@pytest.mark.parametrize(
    ("status", "lineage_method"),
    [
        (JobStatus.FINISHED, "report_complete"),
        (JobStatus.CANCELLED, "report_cancel"),
    ],
)
def test_wait_reports_terminal_lineage(monkeypatch, status: JobStatus, lineage_method: str) -> None:
    manager = _JobManager(status)
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: False)

    handle.wait()

    getattr(lineage, lineage_method).assert_called_once()


def test_wait_raises_job_failure_with_diagnostic(monkeypatch) -> None:
    manager = _JobManager(JobStatus.FAILED)
    handle, lineage = _live_handle(manager)
    diagnostic = Mock()
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: False)
    monkeypatch.setattr("ray.klein.api.live_job_handle.report_diagnostic", diagnostic)

    with pytest.raises(KleinError, match="worker exploded"):
        handle.wait()

    diagnostic.assert_called_once()
    lineage.report_fail.assert_called_once()


def test_wait_cancels_job_when_driver_is_interrupted(monkeypatch) -> None:
    manager = _JobManager()
    manager.wait_error = KeyboardInterrupt("stop")
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: False)

    with pytest.raises(KeyboardInterrupt, match="stop"):
        handle.wait()

    assert manager.cancel_timeouts == [5]
    lineage.report_cancel.assert_called_once()


def test_wait_starts_and_stops_interactive_progress_thread(monkeypatch) -> None:
    manager = _JobManager()
    handle, lineage = _live_handle(manager)
    summary = Mock()
    created_threads: list[object] = []

    class _Thread:
        def __init__(self, *, target, args, daemon: bool) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon
            self.started = False
            self.join_timeout: int | None = None
            created_threads.append(self)

        def start(self) -> None:
            self.started = True

        def join(self, timeout: int) -> None:
            self.join_timeout = timeout

    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: True)
    monkeypatch.setattr(progress_view, "print_summary", summary)
    monkeypatch.setattr(threading, "Thread", _Thread)

    handle.wait()

    thread = created_threads[0]
    assert thread.started is True
    assert thread.join_timeout == 2
    assert thread.daemon is True
    assert thread.args[3].is_set()
    summary.assert_called_once()
    lineage.report_complete.assert_called_once()
