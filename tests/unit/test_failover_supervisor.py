# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from collections import deque
from unittest.mock import AsyncMock, Mock

import pytest

from ray.klein.runtime.job_manager import failover_supervisor as failover_module
from ray.klein.runtime.job_manager.failover_supervisor import FailoverSupervisor
from ray.klein.runtime.scheduler.restart_result import RestartResult, RestartStatus


class _JobMaster:
    def __init__(self) -> None:
        self.coordinator_error: Exception | None = None
        self.recovery_error: Exception | None = None
        self.recovery_result = True
        self.restart_results = deque([RestartResult(RestartStatus.SUCCESS, "restarted")])
        self.coordinator_recoveries = 0
        self.task_recoveries = 0
        self.restart_forces = []

    def recover_coordinator_if_needed(self) -> bool:
        self.coordinator_recoveries += 1
        if self.coordinator_error is not None:
            raise self.coordinator_error
        return False

    def try_recover_tasks(self) -> bool:
        self.task_recoveries += 1
        if self.recovery_error is not None:
            raise self.recovery_error
        return self.recovery_result

    def restart(self, force: bool) -> RestartResult:
        self.restart_forces.append(force)
        result = self.restart_results.popleft()
        if isinstance(result, Exception):
            raise result
        return result


class _Report:
    def __init__(
        self,
        *,
        healthy: bool,
        tasks_not_running: list[str] | None = None,
        coordinator_healthy: bool = True,
    ) -> None:
        self.healthy = healthy
        self.tasks_not_running = tasks_not_running or []
        self.coordinator_healthy = coordinator_healthy

    def summary(self) -> str:
        return (
            f"healthy={self.healthy} coordinator_healthy={self.coordinator_healthy} "
            f"unhealthy_tasks={self.tasks_not_running}"
        )


def _build_supervisor(
    job_master: _JobMaster | None = None,
    *,
    wake_event: asyncio.Event | None = None,
    health_check_interval: float = 10,
    restart_delay: float = 0,
    stop_requested: Mock | None = None,
):
    master = job_master or _JobMaster()
    graph = object()
    wake = wake_event or asyncio.Event()
    permanent_failure = AsyncMock()
    stop = stop_requested or Mock(return_value=False)

    async def run_exclusive(function, *args):
        return function(*args)

    supervisor = FailoverSupervisor(
        job_master_provider=lambda: master,
        execution_graph_provider=lambda: graph,
        run_exclusive=run_exclusive,
        wake_event_provider=lambda: wake,
        health_check_interval=health_check_interval,
        restart_delay_provider=lambda: restart_delay,
        on_permanent_failure=permanent_failure,
        stop_requested_provider=stop,
        health_probe_timeout=2,
    )
    return supervisor, master, graph, wake, permanent_failure


@pytest.mark.asyncio
async def test_healthy_tick_checks_coordinator_then_sleeps(monkeypatch) -> None:
    supervisor, master, graph, _wake, _permanent = _build_supervisor()
    report = _Report(healthy=True)
    report_factory = Mock(return_value=report)
    sleep = AsyncMock()
    monkeypatch.setattr(failover_module, "JobHealthReport", report_factory)
    monkeypatch.setattr(supervisor, "_sleep_until_next_tick", sleep)

    await supervisor.tick()

    assert master.coordinator_recoveries == 1
    assert master.task_recoveries == 0
    report_factory.assert_called_once_with(graph, 2)
    sleep.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_healthy_tick_without_job_master_skips_coordinator_probe(monkeypatch) -> None:
    sleep = AsyncMock()
    supervisor = FailoverSupervisor(
        job_master_provider=lambda: None,
        execution_graph_provider=object,
        run_exclusive=AsyncMock(),
        wake_event_provider=asyncio.Event,
        health_check_interval=10,
        restart_delay_provider=lambda: 0,
        on_permanent_failure=AsyncMock(),
        stop_requested_provider=lambda: False,
        health_probe_timeout=2,
    )
    monkeypatch.setattr(failover_module, "JobHealthReport", Mock(return_value=_Report(healthy=True)))
    monkeypatch.setattr(supervisor, "_sleep_until_next_tick", sleep)

    await supervisor.tick()

    supervisor._run_exclusive.assert_not_awaited()
    sleep.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_coordinator_recovery_failure_does_not_abort_healthy_tick(monkeypatch) -> None:
    supervisor, master, _graph, _wake, _permanent = _build_supervisor()
    master.coordinator_error = RuntimeError("coordinator unavailable")
    events = []
    sleep = AsyncMock()
    monkeypatch.setattr(failover_module, "JobHealthReport", Mock(return_value=_Report(healthy=True)))
    monkeypatch.setattr(supervisor, "_sleep_until_next_tick", sleep)
    monkeypatch.setattr(failover_module, "log_event", lambda *args, **_kwargs: events.append(args[2]))

    await supervisor.tick()

    assert master.coordinator_recoveries == 1
    assert master.task_recoveries == 0
    assert "failover.coordinator.probe_failed" in events
    sleep.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_unhealthy_tick_stays_local_when_task_recovery_succeeds(monkeypatch) -> None:
    supervisor, master, _graph, _wake, _permanent = _build_supervisor()
    report = _Report(healthy=False, tasks_not_running=["map-0"], coordinator_healthy=False)
    sleep = AsyncMock()
    global_restart = AsyncMock()
    monkeypatch.setattr(failover_module, "JobHealthReport", Mock(return_value=report))
    monkeypatch.setattr(supervisor, "_sleep_until_next_tick", sleep)
    monkeypatch.setattr(supervisor, "restart", global_restart)

    await supervisor.tick()

    assert master.task_recoveries == 1
    sleep.assert_awaited_once_with()
    global_restart.assert_not_awaited()


@pytest.mark.asyncio
async def test_unrecoverable_tick_reports_diagnostic_and_forces_global_restart(monkeypatch) -> None:
    supervisor, master, _graph, _wake, _permanent = _build_supervisor()
    master.recovery_result = False
    report = _Report(healthy=False, tasks_not_running=["sink-0"])
    diagnostic = Mock()
    global_restart = AsyncMock()
    monkeypatch.setattr(failover_module, "JobHealthReport", Mock(return_value=report))
    monkeypatch.setattr(failover_module, "report_diagnostic", diagnostic)
    monkeypatch.setattr(supervisor, "restart", global_restart)

    await supervisor.tick()

    global_restart.assert_awaited_once_with(force=True)
    diagnostic.assert_called_once()
    assert report.summary() in diagnostic.call_args.args[1]


@pytest.mark.asyncio
async def test_probe_and_local_recovery_exceptions_escalate_to_global_restart(monkeypatch) -> None:
    supervisor, master, _graph, _wake, _permanent = _build_supervisor()
    master.recovery_error = RuntimeError("task recovery failed")
    diagnostic = Mock()
    global_restart = AsyncMock()
    events = []
    monkeypatch.setattr(failover_module, "JobHealthReport", Mock(side_effect=RuntimeError("probe failed")))
    monkeypatch.setattr(failover_module, "report_diagnostic", diagnostic)
    monkeypatch.setattr(failover_module, "log_event", lambda *args, **_kwargs: events.append(args[2]))
    monkeypatch.setattr(supervisor, "restart", global_restart)

    await supervisor.tick()

    assert master.task_recoveries == 1
    assert "failover.health.probe_failed" in events
    assert "failover.health.unavailable" in events
    assert "failover.task.recovery_failed" in events
    global_restart.assert_awaited_once_with(force=True)
    assert "health probe failed" in diagnostic.call_args.args[1]


@pytest.mark.asyncio
async def test_restart_retries_exceptions_and_failed_results_until_success() -> None:
    master = _JobMaster()
    master.restart_results = deque(
        [
            RuntimeError("writer crashed"),
            RestartResult(RestartStatus.FAILED, "workers unavailable"),
            RestartResult(RestartStatus.SUCCESS, "restarted"),
        ]
    )
    supervisor, _master, _graph, _wake, permanent = _build_supervisor(master)

    await supervisor.restart(force=True)

    assert master.restart_forces == [True, True, True]
    permanent.assert_not_awaited()


@pytest.mark.asyncio
async def test_restart_timeout_is_converted_to_a_retry(monkeypatch) -> None:
    supervisor, master, _graph, _wake, _permanent = _build_supervisor()
    wait_calls = 0

    async def timeout_once(awaitable, timeout):
        nonlocal wait_calls
        del timeout
        wait_calls += 1
        if wait_calls == 1:
            awaitable.close()
            raise asyncio.TimeoutError
        return await awaitable

    monkeypatch.setattr(failover_module.asyncio, "wait_for", timeout_once)

    await supervisor.restart()

    assert wait_calls == 2
    assert master.restart_forces == [False]


@pytest.mark.asyncio
async def test_suppressed_restart_fails_job_permanently() -> None:
    master = _JobMaster()
    master.restart_results = deque([RestartResult(RestartStatus.SUPPRESSED, "failure-rate exceeded")])
    supervisor, _master, _graph, _wake, permanent = _build_supervisor(master)

    await supervisor.restart(force=True)

    assert master.restart_forces == [True]
    permanent.assert_awaited_once_with(True)


@pytest.mark.asyncio
async def test_restart_returns_without_attempt_when_stop_was_requested() -> None:
    stop_requested = Mock(return_value=True)
    supervisor, master, _graph, _wake, permanent = _build_supervisor(stop_requested=stop_requested)

    await supervisor.restart()

    assert master.restart_forces == []
    permanent.assert_not_awaited()


@pytest.mark.asyncio
async def test_wake_event_interrupts_backoff_and_stop_is_rechecked() -> None:
    master = _JobMaster()
    master.restart_results = deque([RestartResult(RestartStatus.FAILED, "retry later")])
    wake = asyncio.Event()
    wake.set()
    stop_requested = Mock(side_effect=[False, True])
    supervisor, _master, _graph, _wake, _permanent = _build_supervisor(
        master,
        wake_event=wake,
        restart_delay=60,
        stop_requested=stop_requested,
    )

    await supervisor.restart()

    assert master.restart_forces == [False]
    assert wake.is_set() is False
    assert stop_requested.call_count == 2


@pytest.mark.asyncio
async def test_restart_retries_after_backoff_timeout() -> None:
    master = _JobMaster()
    master.restart_results = deque(
        [
            RestartResult(RestartStatus.FAILED, "retry later"),
            RestartResult(RestartStatus.SUCCESS, "restarted"),
        ]
    )
    supervisor, _master, _graph, _wake, _permanent = _build_supervisor(master, restart_delay=0.001)

    await supervisor.restart()

    assert master.restart_forces == [False, False]


@pytest.mark.asyncio
@pytest.mark.parametrize("wake_immediately", [False, True])
async def test_inter_tick_sleep_is_bounded_and_clears_wake_event(wake_immediately: bool) -> None:
    wake = asyncio.Event()
    if wake_immediately:
        wake.set()
    supervisor, _master, _graph, _wake, _permanent = _build_supervisor(
        wake_event=wake,
        health_check_interval=0.001,
    )

    await supervisor._sleep_until_next_tick()

    assert wake.is_set() is False
