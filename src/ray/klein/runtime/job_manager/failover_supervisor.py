# SPDX-License-Identifier: Apache-2.0
"""Failover policy: one supervisor tick + global-restart escalation.

Single-writer invariant: every execution-graph mutation runs through the
JobManager's ``run_exclusive`` (its single-writer executor, borrowed never owned
here), so it stays serialized with submit/cancel/stop on the one writer thread.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from ray.klein._internal.logging import get_logger, log_event
from ray.klein.observability.diagnostics import DiagnosticLevel, report_diagnostic
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.job_manager.liveness_report import JobHealthReport
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.scheduler.restart_result import RestartResult, RestartStatus

logger = get_logger(__name__)

# Safety net so a stuck coordinator/worker can't block the restart loop forever.
_RESTART_ATTEMPT_TIMEOUT = 180.0


class FailoverSupervisor:
    """Health-loop tick + self-healing global-restart escalation for one job."""

    def __init__(
        self,
        *,
        job_master_provider: Callable[[], JobMaster],
        execution_graph_provider: Callable[[], ExecutionGraph],
        run_exclusive: Callable[..., Awaitable[Any]],
        wake_event_provider: Callable[[], asyncio.Event],
        health_check_interval: float,
        restart_delay_provider: Callable[[], float],
        on_permanent_failure: Callable[[bool], Awaitable[None]],
        stop_requested_provider: Callable[[], bool],
        health_probe_timeout: float,
    ) -> None:
        self._job_master_provider = job_master_provider
        self._execution_graph_provider = execution_graph_provider
        # The JobManager's single-writer executor entry point: await
        # run_exclusive(fn, *args) to run a blocking execution-graph mutation
        # serialized with every other writer.
        self._run_exclusive = run_exclusive
        self._wake_event_provider = wake_event_provider
        self._health_check_interval = health_check_interval
        self._restart_delay_provider = restart_delay_provider
        self._on_permanent_failure = on_permanent_failure
        self._stop_requested_provider = stop_requested_provider
        self._health_probe_timeout = health_probe_timeout

    async def tick(self) -> None:
        """coordinator recovery → health probe → single-point recovery → global restart."""
        job_master = self._job_master_provider()

        if job_master is not None:
            try:
                await self._run_exclusive(job_master.recover_coordinator_if_needed)
            except Exception:
                log_event(
                    logger,
                    logging.ERROR,
                    "failover.coordinator.probe_failed",
                    "Checkpoint coordinator recovery check failed and will be retried",
                    exc_info=True,
                )

        try:
            report = await asyncio.wait_for(
                asyncio.to_thread(
                    JobHealthReport,
                    self._execution_graph_provider(),
                    self._health_probe_timeout,
                ),
                timeout=self._health_probe_timeout + 1,
            )
        except Exception:
            log_event(
                logger,
                logging.ERROR,
                "failover.health.probe_failed",
                "Job health probe failed; treating the job as unhealthy",
                exc_info=True,
            )
            report = None

        if report is not None and report.healthy:
            logger.debug(report.summary())
            await self._sleep_until_next_tick()
            return

        self._log_unhealthy(report)

        try:
            recovered = await self._run_exclusive(job_master.try_recover_tasks)
        except Exception:
            log_event(
                logger,
                logging.ERROR,
                "failover.task.recovery_failed",
                "Task-level recovery failed; escalating to a global restart",
                exc_info=True,
            )
            recovered = False

        if recovered:
            log_event(
                logger,
                logging.INFO,
                "failover.task.recovery_succeeded",
                "Task-level recovery succeeded",
            )
            await self._sleep_until_next_tick()
            return

        summary = report.summary() if report is not None else "(health probe failed)"
        error_message = (
            f"Task-level recovery could not restore the job; escalating to a global restart. Health summary: {summary}"
        )
        log_event(
            logger,
            logging.WARNING,
            "failover.global.escalated",
            "%s",
            error_message,
        )
        report_diagnostic(DiagnosticLevel.WARN, error_message)
        await self.restart(force=True)

    async def restart(self, force: bool = False) -> None:
        """Self-healing global-restart retry loop. Exits on SUCCESS, SUPPRESSED
        (failure-rate exceeded → fail permanently), or external cancel/stop."""
        job_master = self._job_master_provider()
        wake_event = self._wake_event_provider()
        restart_delay = self._restart_delay_provider()
        while True:
            if self._stop_requested_provider():
                return
            # Only the scheduler call goes on the writer: the SUPPRESSED branch's
            # stop (below, outside this block) also needs the writer, and the
            # backoff wait must not occupy it — so we await the single restart here
            # and let it complete before stop/backoff can be enqueued.
            try:
                result = await asyncio.wait_for(
                    self._run_exclusive(job_master.restart, force),
                    timeout=_RESTART_ATTEMPT_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log_event(
                    logger,
                    logging.ERROR,
                    "failover.global.timed_out",
                    "Global restart timed out after %.0f seconds and will be retried",
                    _RESTART_ATTEMPT_TIMEOUT,
                    timeout_seconds=_RESTART_ATTEMPT_TIMEOUT,
                )
                result = RestartResult(
                    RestartStatus.FAILED,
                    f"restart timed out after {_RESTART_ATTEMPT_TIMEOUT}s",
                )
            except Exception as error:
                log_event(
                    logger,
                    logging.ERROR,
                    "failover.global.attempt_failed",
                    "Global restart raised an exception and will be retried: %s",
                    error,
                    exc_info=True,
                    reason=str(error),
                )
                result = RestartResult(
                    RestartStatus.FAILED,
                    f"{type(error).__name__}: {error}",
                )
            if result.status == RestartStatus.SUCCESS:
                log_event(
                    logger,
                    logging.INFO,
                    "failover.global.completed",
                    "Global restart completed",
                )
                return
            if result.status == RestartStatus.SUPPRESSED:
                log_event(
                    logger,
                    logging.ERROR,
                    "failover.global.suppressed",
                    "Global restart was suppressed; failing the job permanently: %s",
                    result.message,
                    reason=result.message,
                )
                await self._on_permanent_failure(force)
                return
            log_event(
                logger,
                logging.WARNING,
                "failover.global.retry_scheduled",
                "Global restart failed and will be retried after %g seconds: %s",
                restart_delay,
                result.message,
                retry_delay_seconds=restart_delay,
                reason=result.message,
            )
            # Interruptible backoff: a stop/cancel sets the wake event to break out.
            if restart_delay > 0:
                try:
                    await asyncio.wait_for(wake_event.wait(), timeout=restart_delay)
                except asyncio.TimeoutError:
                    pass
                finally:
                    wake_event.clear()

    @staticmethod
    def _log_unhealthy(report: JobHealthReport | None) -> None:
        if report is not None:
            unhealthy_tasks = report.tasks_not_running
            log_event(
                logger,
                logging.WARNING,
                "failover.health.unhealthy",
                "Job health check failed; unhealthy tasks: %s; checkpoint coordinator healthy: %s",
                unhealthy_tasks or "none",
                report.coordinator_healthy,
                unhealthy_tasks=unhealthy_tasks,
                coordinator_healthy=report.coordinator_healthy,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "failover.health.unavailable",
                "Job health data is unavailable; triggering recovery",
            )

    async def _sleep_until_next_tick(self) -> None:
        """Interruptible inter-tick wait — a stop/cancel wakes it immediately."""
        wake_event = self._wake_event_provider()
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=self._health_check_interval)
        except asyncio.TimeoutError:
            pass
        finally:
            wake_event.clear()
