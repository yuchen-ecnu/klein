# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import threading
from collections import deque
from unittest.mock import Mock

import pytest
from ray.util.queue import Empty

import ray.klein as klein
from ray.klein.api.collect_function import _CollectLimitExceeded
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
        try:
            return self._values.popleft()
        except IndexError as error:
            raise Empty from error

    def get(self, *, timeout: float) -> object:
        del timeout
        return self.get_nowait()

    def shutdown(self, *, force: bool) -> None:
        self.shutdown_args.append(force)


class _BlockingOutputQueue(_OutputQueue):
    def __init__(self, values: list[object] | None = None) -> None:
        super().__init__(values or [])
        self._condition = threading.Condition()

    def put(self, value: object) -> None:
        with self._condition:
            self._values.append(value)
            self._condition.notify()

    def get(self, *, timeout: float) -> object:
        with self._condition:
            if not self._values:
                self._condition.wait(timeout)
            return self.get_nowait()


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


def _live_handle(
    manager: _JobManager,
    lineage: Mock | None = None,
    **options,
) -> tuple[LiveJobHandle, Mock]:
    tracker = lineage or Mock()
    return (
        LiveJobHandle(
            manager,
            "orders",
            RuntimeExecutionMode.STREAMING,
            "klein-orders",
            tracker,
            **options,
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
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    result = handle.get()
    assert result == [{"id": 1}, {"id": 2}]
    assert handle.get() is result
    assert manager.queue.shutdown_args == [True]
    lineage.report_complete.assert_called_once()
    assert handle.status is JobStatus.FINISHED
    assert handle.cancel(timeout=7) is True
    assert manager.cancel_timeouts == [7]
    assert handle._progress_snapshot() == "snapshot"
    assert handle.namespace == "klein-orders"


def test_live_job_handle_keeps_draining_after_ray_queue_empty(monkeypatch) -> None:
    manager = _JobManager()
    manager.queue = _BlockingOutputQueue()
    release_terminal = threading.Event()

    def wait_until_terminal() -> str:
        manager.queue.put({"id": 1})
        assert release_terminal.wait(timeout=1)
        manager.queue.put({"id": 2})
        return "terminal"

    manager.wait_until_terminal = wait_until_terminal
    handle, _ = _live_handle(manager)

    def resolve(value, **_kwargs):
        if value == "terminal":
            return value
        return value

    monkeypatch.setattr(klein, "get", resolve)
    producer = threading.Thread(
        target=lambda: (threading.Event().wait(0.15), release_terminal.set()),
        daemon=True,
    )
    producer.start()

    assert handle.get() == [{"id": 1}, {"id": 2}]
    producer.join(timeout=1)


def test_concurrent_get_calls_share_one_queue_drain(monkeypatch) -> None:
    manager = _JobManager()
    entered = threading.Event()
    release = threading.Event()

    def wait_until_terminal() -> str:
        entered.set()
        assert release.wait(timeout=1)
        return "terminal"

    manager.wait_until_terminal = wait_until_terminal
    handle, _ = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    results: list[object] = []
    errors: list[BaseException] = []

    def get_result() -> None:
        try:
            results.append(handle.get())
        except BaseException as error:
            errors.append(error)

    first = threading.Thread(target=get_result)
    second = threading.Thread(target=get_result)
    first.start()
    assert entered.wait(timeout=1)
    second.start()
    release.set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert errors == []
    assert len(results) == 2
    assert results[0] is results[1]
    assert manager.queue.shutdown_args == [True]


def test_live_job_handle_cleanup_failure_does_not_hide_completed_result(monkeypatch) -> None:
    manager = _JobManager()
    manager.queue.shutdown = Mock(side_effect=RuntimeError("queue unavailable"))
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    result = handle.get()

    assert result == [{"id": 1}, {"id": 2}]
    assert handle.get() is result
    manager.queue.shutdown.assert_called_once_with(force=True)
    lineage.report_complete.assert_called_once()


def test_live_job_handle_reports_take_all_safety_limit_after_graceful_drain(monkeypatch) -> None:
    manager = _JobManager()
    manager.queue = _OutputQueue([{"id": 1}, _CollectLimitExceeded(1)])
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    with pytest.raises(ValueError, match="exceeds limit 1") as first_error:
        handle.get()
    with pytest.raises(ValueError) as repeated_error:
        handle.get()

    assert repeated_error.value is first_error.value
    assert manager.queue.shutdown_args == [True]
    lineage.report_complete.assert_called_once()


def test_live_job_handle_enforces_take_all_limit_across_restarted_collectors(monkeypatch) -> None:
    manager = _JobManager()
    manager.queue = _OutputQueue([{"id": 1}, {"id": 2}, {"id": 3}])
    handle, _ = _live_handle(manager, collect_limit=2)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    with pytest.raises(ValueError, match="exceeds limit 2"):
        handle.get()

    assert handle._drained_output == []


def test_live_job_handle_caps_take_result_across_restarted_collectors(monkeypatch) -> None:
    manager = _JobManager()
    manager.queue = _OutputQueue([{"id": 1}, {"id": 2}, {"id": 3}])
    handle, _ = _live_handle(manager, collect_limit=2, collect_truncate=True)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    assert handle.get() == [{"id": 1}, {"id": 2}]


@pytest.mark.parametrize(
    ("status", "message", "lineage_method"),
    [
        (JobStatus.FAILED, "worker exploded", "report_fail"),
        (JobStatus.CANCELLED, "Job was cancelled", "report_cancel"),
    ],
)
def test_live_job_handle_get_rejects_partial_terminal_results(
    monkeypatch,
    status: JobStatus,
    message: str,
    lineage_method: str,
) -> None:
    manager = _JobManager(status)
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    with pytest.raises(KleinError, match=message):
        handle.get()

    assert manager.queue.qsize() == 0
    assert manager.queue.shutdown_args == [True]
    getattr(lineage, lineage_method).assert_called_once()


@pytest.mark.parametrize("method", ["get", "wait"])
def test_terminal_job_failure_wins_over_output_drain_failure(monkeypatch, method: str) -> None:
    manager = _JobManager(JobStatus.FAILED)
    manager.queue.get = Mock(side_effect=RuntimeError("queue unavailable"))
    handle, _ = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: False)

    with pytest.raises(KleinError, match="worker exploded"):
        getattr(handle, method)()


def test_successful_job_reports_and_caches_output_drain_failure(monkeypatch) -> None:
    manager = _JobManager()
    manager.queue.get = Mock(side_effect=RuntimeError("queue unavailable"))
    handle, _ = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    with pytest.raises(KleinError, match=r"Failed to drain.*queue unavailable") as first:
        handle.get()
    with pytest.raises(KleinError) as repeated:
        handle.get()

    assert repeated.value is first.value


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


@pytest.mark.parametrize(
    ("status", "lineage_method"),
    [
        (JobStatus.FINISHED, "report_complete"),
        (JobStatus.CANCELLED, "report_cancel"),
    ],
)
def test_wait_then_get_reports_terminal_lineage_once(
    monkeypatch,
    status: JobStatus,
    lineage_method: str,
) -> None:
    manager = _JobManager(status)
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: False)

    handle.wait()
    if status is JobStatus.CANCELLED:
        with pytest.raises(KleinError, match="cancelled"):
            handle.get()
    else:
        result = handle.get()
        assert result == [{"id": 1}, {"id": 2}]
        assert handle.get() is result

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


def test_failed_wait_then_get_reuses_terminal_error_and_reports_once(monkeypatch) -> None:
    manager = _JobManager(JobStatus.FAILED)
    handle, lineage = _live_handle(manager)
    diagnostic = Mock()
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr(progress_view, "is_interactive", lambda: False)
    monkeypatch.setattr("ray.klein.api.live_job_handle.report_diagnostic", diagnostic)

    with pytest.raises(KleinError, match="worker exploded") as wait_error:
        handle.wait()
    with pytest.raises(KleinError, match="worker exploded") as get_error:
        handle.get()

    assert get_error.value is wait_error.value
    assert manager.queue.shutdown_args == [True]
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


def test_get_cancels_job_when_driver_is_interrupted(monkeypatch) -> None:
    manager = _JobManager()
    manager.wait_error = KeyboardInterrupt("stop")
    handle, lineage = _live_handle(manager)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)

    with pytest.raises(KeyboardInterrupt, match="stop"):
        handle.get()

    assert manager.cancel_timeouts == [5]
    assert manager.queue.shutdown_args == [True]
    lineage.report_cancel.assert_called_once()


def test_wait_starts_and_stops_interactive_progress_thread(monkeypatch) -> None:
    manager = _JobManager()
    handle, lineage = _live_handle(manager)
    summary = Mock()
    created_threads: list[object] = []

    class _Thread:
        def __init__(self, *, target, args=(), name=None, daemon: bool) -> None:
            self.target = target
            self.args = args
            self.name = name
            self.daemon = daemon
            self.started = False
            self.join_timeout: int | None = None
            created_threads.append(self)

        def start(self) -> None:
            self.started = True

        def join(self, timeout: int) -> None:
            self.join_timeout = timeout

        def is_alive(self) -> bool:
            return False

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
