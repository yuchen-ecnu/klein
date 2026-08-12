# SPDX-License-Identifier: Apache-2.0

from contextlib import suppress
from threading import Event, Lock, Thread
from typing import Any, cast

from ray.util.queue import Empty, Queue

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.api.collect_function import _CollectLimitExceeded
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
_OUTPUT_DRAIN_POLL_SECONDS = 0.1
_OUTPUT_DRAIN_JOIN_SECONDS = 2.0


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
        collecting: bool = True,
        collect_limit: int | None = None,
        collect_truncate: bool = False,
    ) -> None:
        self._jobmanager = jobmanager
        self._job_name = job_name
        self._runtime_mode = runtime_mode
        self._namespace = namespace
        self._lineage_tracker = lineage_tracker
        self._collecting = collecting
        self._collect_limit = collect_limit
        self._collect_truncate = collect_truncate
        self._reported_terminal_status: JobStatus | None = None
        self._reported_terminal_error: KleinError | None = None
        self._completed_result: Any = _RESULT_UNSET
        self._completed_result_error: Exception | None = None
        self._output_drain_error: KleinError | None = None
        self._output_queue_drained = False
        self._drained_output: list[Any] = []
        self._drained_output_rows = 0
        self._collect_limit_exceeded = False
        self._completion_lock = Lock()

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
            with self._completion_lock:
                self._wait_and_drain_output()
                status = self.status
                terminal_error = self._report_terminal_status(status)
                self._finalize_drained_output(success=terminal_error is None and self._output_drain_error is None)
        finally:
            if stop_event is not None:
                stop_event.set()
            if render_thread is not None:
                render_thread.join(timeout=2)
        if render_thread is not None:
            _progress_view.print_summary(
                self._job_name,
                status.name,
                _time.monotonic() - started,
                progress_result["rows"],
            )
        if status == JobStatus.FAILED:
            assert terminal_error is not None
            raise terminal_error
        if terminal_error is None and self._output_drain_error is not None:
            raise self._output_drain_error

    def get(self) -> Any:
        """Block until the job is terminal, then drain the output queue.

        Waits on the same event-driven ``wait_until_terminal`` block used by
        :meth:`wait` while concurrently draining the bounded output queue. This
        keeps producer backpressure without deadlocking terminal completion.
        """
        with self._completion_lock:
            if self._completed_result is not _RESULT_UNSET:
                return self._completed_result
            if self._completed_result_error is not None:
                raise self._completed_result_error
            if not self._collecting:
                raise ValueError("result() is only available for take() or take_all() terminals")
            self._wait_and_drain_output()
            terminal_error = self._report_terminal_status(self.status)
            if terminal_error is not None:
                self._finalize_drained_output(success=False)
                raise terminal_error
            if self._output_drain_error is not None:
                self._finalize_drained_output(success=False)
                self._completed_result_error = self._output_drain_error
                raise self._output_drain_error
            self._finalize_drained_output(success=True)
            if self._completed_result_error is not None:
                raise self._completed_result_error
            return self._completed_result

    def _finalize_drained_output(self, *, success: bool) -> None:
        if not self._collecting or self._completed_result is not _RESULT_UNSET:
            return
        if self._completed_result_error is not None:
            return
        if not success:
            self._drained_output.clear()
            return
        exceeded = next((item for item in self._drained_output if isinstance(item, _CollectLimitExceeded)), None)
        if exceeded is not None:
            self._completed_result_error = ValueError(f"take_all() result exceeds limit {exceeded.limit}")
            self._drained_output.clear()
            return
        self._completed_result = self._drained_output

    def _wait_and_drain_output(self) -> None:
        """Drain a collecting queue concurrently so its hard bound can backpressure."""

        if not self._collecting or self._output_queue_drained:
            self._wait_until_terminal()
            return

        try:
            output_queue: Queue = klein.get(self._jobmanager.output_queue())
        except Exception as error:
            self._remember_output_drain_error("Failed to access the collecting output queue", error)
            self._output_queue_drained = True
            self._wait_until_terminal()
            return
        terminal = Event()
        drain_errors: list[BaseException] = []
        drain_thread = Thread(
            target=self._drain_output_queue,
            args=(output_queue, terminal, drain_errors),
            name=f"klein-output-{self._namespace}",
            daemon=True,
        )
        drain_thread.start()
        try:
            self._wait_until_terminal()
        finally:
            terminal.set()
            drain_thread.join(timeout=_OUTPUT_DRAIN_JOIN_SECONDS)
            if drain_thread.is_alive():
                self._shutdown_output_queue(output_queue)
                drain_thread.join(timeout=_OUTPUT_DRAIN_JOIN_SECONDS)
            else:
                self._shutdown_output_queue(output_queue)
            self._output_queue_drained = True
            if drain_thread.is_alive():
                self._output_drain_error = KleinError("Timed out while draining the collecting output queue")
            elif drain_errors:
                self._remember_output_drain_error("Failed to drain the collecting output queue", drain_errors[0])

    def _drain_output_queue(
        self,
        output_queue: Queue,
        terminal: Event,
        drain_errors: list[BaseException],
    ) -> None:
        try:
            while not terminal.is_set():
                with suppress(Empty):
                    self._append_drained_output(output_queue.get(timeout=_OUTPUT_DRAIN_POLL_SECONDS))
            while True:
                try:
                    self._append_drained_output(output_queue.get_nowait())
                except Empty:
                    return
        except BaseException as error:
            drain_errors.append(error)

    def _append_drained_output(self, item: Any) -> None:
        if isinstance(item, _CollectLimitExceeded):
            if not self._collect_limit_exceeded:
                self._drained_output.append(item)
                self._collect_limit_exceeded = True
            return
        limit = self._collect_limit
        if limit is not None and self._drained_output_rows >= limit:
            if not self._collect_truncate and not self._collect_limit_exceeded:
                self._append_drained_output(_CollectLimitExceeded(limit))
            return
        self._drained_output.append(item)
        self._drained_output_rows += 1

    def _remember_output_drain_error(self, context: str, error: BaseException) -> None:
        if self._output_drain_error is None:
            self._output_drain_error = KleinError(f"{context}: {error}")
            self._output_drain_error.__cause__ = error

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
