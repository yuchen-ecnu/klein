# SPDX-License-Identifier: Apache-2.0

from contextlib import suppress
from typing import Any, cast

from ray.util.queue import Queue

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.job_status import JobStatus
from ray.klein.config.execution_options import (
    RuntimeExecutionMode,
)
from ray.klein.exceptions import KleinError
from ray.klein.observability.diagnostics import DiagnosticLevel, report_diagnostic
from ray.klein.observability.lineage.tracker import KleinLineageTracker

logger = get_logger(__name__)
_RESULT_UNSET = object()


class LiveJobHandle(JobHandle):
    """Handle to a submitted streaming job, backed by a remote JobManager.

    Owns the job's observable runtime surface: terminal-state waiting (driven by
    the JobManager's ``asyncio.Event``, no polling), result draining, status,
    cancellation, the live progress view and lineage reporting.
    """

    def __init__(
        self,
        jobmanager: Any,
        job_name: str,
        runtime_mode: RuntimeExecutionMode,
        namespace: str,
        lineage_tracker: KleinLineageTracker,
    ) -> None:
        self._jobmanager = jobmanager
        self._job_name = job_name
        self._runtime_mode = runtime_mode
        self._namespace = namespace
        self._lineage_tracker = lineage_tracker
        self._reported_terminal_status: JobStatus | None = None
        self._reported_terminal_error: KleinError | None = None
        self._completed_result: Any = _RESULT_UNSET

    def wait(self) -> None:
        """Block until the job reaches a terminal state.

        Blocks on a single ``wait_until_terminal`` RPC — the JobManager sets an
        internal ``asyncio.Event`` on every terminal transition, so this wakes on
        the real transition with no polling.
        """
        import threading
        import time as _time

        from ray.klein.observability import progress_view as _progress_view

        render_thread: threading.Thread | None = None
        stop_event: threading.Event | None = None
        progress_result: dict = {"rows": 0}
        started = _time.monotonic()
        if _progress_view.is_interactive():
            stop_event = threading.Event()
            render_thread = threading.Thread(
                target=_progress_view.render_until_terminal,
                args=(
                    self._progress_snapshot,
                    self._job_name,
                    self._runtime_mode.value,
                    stop_event,
                    progress_result,
                ),
                daemon=True,
            )
            render_thread.start()

        try:
            self._wait_until_terminal()
        finally:
            if stop_event is not None:
                stop_event.set()
            if render_thread is not None:
                render_thread.join(timeout=2)
        status = self.status
        if render_thread is not None:
            _progress_view.print_summary(
                self._job_name,
                status.name,
                _time.monotonic() - started,
                progress_result["rows"],
            )
        terminal_error = self._report_terminal_status(status)
        if status == JobStatus.FAILED:
            assert terminal_error is not None
            raise terminal_error

    def get(self) -> Any:
        """Block until the job is terminal, then drain the output queue.

        Waits on the same event-driven ``wait_until_terminal`` block used by
        :meth:`wait` (no polling), then drains. The output queue is unbounded so
        producers never stall on a slow consumer — by the time the job is
        terminal every emitted record is already enqueued, so a single
        non-blocking drain is complete and race-free.
        """
        if self._completed_result is not _RESULT_UNSET:
            return self._completed_result
        self._wait_until_terminal()
        terminal_error = self._report_terminal_status(self.status)
        if terminal_error is not None:
            self._discard_partial_output()
            raise terminal_error
        output_queue: Queue = klein.get(self._jobmanager.output_queue())
        try:
            result = [output_queue.get_nowait() for _ in range(output_queue.qsize())]
        finally:
            self._shutdown_output_queue(output_queue)
        self._completed_result = result
        return result

    def _wait_until_terminal(self) -> None:
        """Wait for the terminal event and avoid orphaning on interruption."""

        try:
            klein.get(self._jobmanager.wait_until_terminal())
        except (SystemExit, KeyboardInterrupt) as error:
            # SIGTERM from `ray job stop` raises SystemExit via Ray's signal
            # handler. Cancel before re-raising so the cluster job does not
            # outlive an interrupted wait/get caller.
            with suppress(Exception):
                self.cancel(timeout=5)
            self._lineage_tracker.report_cancel(KleinError(f"Job was terminated by external signal: {error}"))
            raise

    def _discard_partial_output(self) -> None:
        """Best-effort release of a failed collecting job's detached queue."""

        with suppress(Exception):
            output_queue: Queue = klein.get(self._jobmanager.output_queue())
            output_queue.shutdown(force=True)

    @staticmethod
    def _shutdown_output_queue(output_queue: Queue) -> None:
        """Do not let best-effort queue cleanup hide a completed result."""

        try:
            output_queue.shutdown(force=True)
        except Exception:
            logger.warning("Failed to release completed job output queue", exc_info=True)

    def _report_terminal_status(self, status: JobStatus) -> KleinError | None:
        if self._reported_terminal_status is status:
            return self._reported_terminal_error
        if status == JobStatus.FAILED:
            failed_detail = klein.get(self._jobmanager.failure_detail())
            error_message = f"Job failed due to fatal error, detail:\n {failed_detail}"
            error = KleinError(error_message)
            report_diagnostic(DiagnosticLevel.ERROR, error_message)
            self._lineage_tracker.report_fail(error)
        elif status == JobStatus.CANCELLED:
            error = KleinError("Job was cancelled")
            self._lineage_tracker.report_cancel(error)
        else:
            error = None
            self._lineage_tracker.report_complete()
        self._reported_terminal_status = status
        self._reported_terminal_error = error
        return error

    @property
    def status(self) -> JobStatus:
        return klein.get(self._jobmanager.job_status())

    def cancel(self, timeout: int = 60) -> bool:
        return cast(bool, klein.get(self._jobmanager.cancel(timeout)))

    def _progress_snapshot(self) -> list[Any]:
        """One per-operator progress snapshot (used by the live CLI view)."""
        return cast(list[Any], klein.get(self._jobmanager.progress_snapshot()))

    @property
    def namespace(self) -> str:
        """Per-job Ray namespace.

        Exposed so tests (and ops tooling that attaches to a running job's named
        actors via ``ray.get_actor(name, namespace=...)``) can read what was
        picked.
        """
        return self._namespace
