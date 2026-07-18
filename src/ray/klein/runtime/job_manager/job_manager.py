# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, TypeVar

from ray.util.queue import Queue

import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.deadline import Deadline
from ray.klein._internal.logging import get_logger, log_event
from ray.klein._internal.validation import is_blank
from ray.klein.api.job_status import JobStatus
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.observability.dashboard.serialization import dashboard_value, safe_configuration
from ray.klein.observability.diagnostics import truncate_diagnostic
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.actor import KleinActorHandle, create_remote_actor
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer
from ray.klein.runtime.job_manager.failover_supervisor import FailoverSupervisor
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.worker.async_worker import AsyncWorker

if TYPE_CHECKING:
    from ray.klein.runtime.job_manager.progress import ProgressSnapshot

logger = get_logger(__name__)

_T = TypeVar("_T")


class JobManager(AsyncWorker):
    """Async Ray actor that supervises a single Klein job.

    All public RPC methods are ``async def`` so this is registered as a Ray
    AsyncActor (Ray treats any actor with at least one ``async def`` method as
    AsyncActor). Internally:

    * The supervisor loop runs as an :class:`asyncio.Task` (via
      :class:`AsyncWorker`) — one task instead of one OS thread.
    * The retry/backoff sleep uses an :class:`asyncio.Event` that
      :meth:`_stop_job` sets to wake the supervisor and let cancel propagate
      immediately.
    * Every blocking execution-graph mutation runs through :meth:`run_exclusive`, a
      single-worker :class:`ThreadPoolExecutor`. One worker serializes all writes
      by construction, so the lock-free ExecutionGraph always has exactly one
      writer. Cheap read RPCs stay on the event loop and interleave freely.

    """

    def __init__(self, config: Configuration, namespace: str) -> None:
        super().__init__()
        self.config = config
        # Per-job Ray namespace, propagated to JobMaster/Coordinator/StreamTask so
        # named-actor lookups stay scoped to this job (else a sibling job's actor
        # with the same short name could be returned).
        self.namespace = namespace
        self.job_master: JobMaster | None = None
        self.logical_graph: LogicalGraph | None = None
        self.execution_graph: ExecutionGraph | None = None
        self.job_name: str | None = None
        self._job_status = JobStatus.CREATED
        # Built lazily via the properties below: eager construction here binds the
        # Event to __init__'s loop (debug mode: not the actor's own loop) and
        # raises "got Future attached to a different loop".
        self._wake_event_obj: asyncio.Event | None = None
        self._terminal_event_obj: asyncio.Event | None = None
        # Single-writer executor (see run_exclusive). Plain threads aren't
        # loop-bound, so unlike the Events above this is safe to build eagerly.
        self._writer = ThreadPoolExecutor(max_workers=1, thread_name_prefix="klein-jm-writer")
        self.health_check_interval = self.config.get(JobManagerOptions.HEALTH_CHECK_INTERVAL)
        self._progress_reporter = None
        self._supervisor: FailoverSupervisor | None = None
        self._submission_error: str | None = None
        # Keep the first diagnostic reported by every failed task across global
        # restarts. ExecutionVertex.reset() intentionally clears live status,
        # but a later teardown failure must not erase the user-code exception
        # that initiated recovery.
        self._task_failure_details: dict[str, str] = {}
        self._job_config: Configuration | None = None
        now_ms = int(time.time() * 1000)
        self._created_at_ms = now_ms
        self._started_at_ms: int | None = None
        self._ended_at_ms: int | None = None
        self._status_updated_at_ms = now_ms
        self._status_history: list[dict[str, object]] = [{"status": JobStatus.CREATED.name, "timestamp_ms": now_ms}]

    @property
    def _wake_event(self) -> asyncio.Event:
        if self._wake_event_obj is None:
            self._wake_event_obj = asyncio.Event()
        return self._wake_event_obj

    @property
    def _terminal_event(self) -> asyncio.Event:
        if self._terminal_event_obj is None:
            self._terminal_event_obj = asyncio.Event()
        return self._terminal_event_obj

    async def run_exclusive(self, fn: Callable[..., _T], *args) -> _T:
        """Run a blocking execution-graph mutation serialized with every other writer.

        The single worker is what serializes writes: a call submitted while one
        is in flight queues behind it instead of racing it on another thread.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._writer, lambda: fn(*args))

    @staticmethod
    def create(config: Configuration, namespace: str) -> KleinActorHandle | None:
        # Look-up + create both go through the per-job namespace so two
        # JobClients in the same cluster (each with their own namespace) each
        # land on their own JobManager — get_actor_by_name with the right
        # namespace returns None for the sibling job's "JobManager" actor, and
        # the subsequent create registers a fresh one in this job's namespace.
        jobmanager = klein.get_actor_by_name(ComponentName.KLEIN_JOB_MANAGER, namespace=namespace)
        if jobmanager is not None:
            return jobmanager
        ray_remote_args = {
            "name": ComponentName.KLEIN_JOB_MANAGER,
            "num_cpus": 0,
            # Keep the streaming control plane alive when its submitting
            # driver exits or crashes. Child actors are owned transitively by
            # this actor, so detaching the JobManager removes driver fate sharing.
            "lifetime": "detached",
            "max_restarts": -1,
            "max_task_retries": -1,
            # max_concurrency lets short RPCs (job_status, cancel,
            # failure_detail) interleave with long-running submit()/
            # restart() work without blocking.
            "max_concurrency": 8,
        }
        ray_remote_args["namespace"] = namespace
        return create_remote_actor(
            JobManager,
            construct_args={"config": config, "namespace": namespace},
            ray_remote_args=ray_remote_args,
        )

    async def submit(
        self,
        job_name: str,
        stream_graph: StreamGraph,
        config: Configuration = None,
    ) -> bool:
        job_config = Configuration()
        job_config.update(self.config)
        if config is not None:
            job_config.update(config)
        self._job_config = job_config
        self.job_name = job_name
        self._submission_error = None
        self._task_failure_details.clear()
        self._update_job_status(JobStatus.SUBMITTING)
        log_event(
            logger,
            logging.INFO,
            "job.submission.received",
            "Received job submission %s",
            job_name,
            job_id=self.namespace,
            job_name=job_name,
        )
        logger.debug("Submitted stream graph:\n%s", stream_graph)

        self.logical_graph = LogicalOptimizer(job_config).optimize(stream_graph)

        job_metric_group = JobMetricGroup(job_name=job_name, job_id=self.namespace)
        self.execution_graph = ExecutionGraph.expand(
            self.logical_graph,
            job_config,
            job_metric_group,
            self.namespace,
        )
        self.job_master = JobMaster(self.execution_graph, job_config, job_metric_group)

        restore_path = job_config.get(CheckpointOptions.RESTORE_PATH)
        try:
            await self.run_exclusive(self.job_master.schedule, restore_path)
        except Exception as error:
            self._submission_error = f"{type(error).__name__}: {error}"
            log_event(
                logger,
                logging.ERROR,
                "job.deployment.failed",
                "Failed to deploy job %s",
                job_name,
                exc_info=True,
                job_id=self.namespace,
                job_name=job_name,
            )
            await self._stop_job()
            self._update_job_status(JobStatus.FAILED)
            return False
        await self.start_job_supervisor()
        self._update_job_status(JobStatus.RUNNING)
        return True

    async def cancel(self, timeout: int | None = None) -> bool:
        if self.job_status().is_terminal:
            return False
        await self._stop_job(force=True, timeout=timeout)
        self._update_job_status(JobStatus.CANCELLED)
        return True

    async def drain(self) -> None:
        """Graceful completion: ask every source to stop producing.

        Used by bounded sinks (take(n)) when they hit their limit. Each source is
        asked to stop and then emits EndOfData; the alignment chain flushes and
        commits, and every task reports FINISHED. cancel() is reserved for
        genuine user-initiated abort.
        """
        if self.job_status().is_terminal:
            return
        if self.job_master is None:
            return
        # JobMaster owns the execution graph; it hands back source handles, we gather here.
        source_tasks = self.job_master.list_source_task_handles()
        try:
            await klein.aget([task.request_drain() for task in source_tasks])
        except Exception as error:
            log_event(
                logger,
                logging.WARNING,
                "job.drain.failed",
                "Failed to request graceful source drain: %s",
                error,
                job_id=self.namespace,
                job_name=self.job_name,
            )

    async def update_stream_task_status(
        self,
        vertex_id: ExecutionVertexId,
        status: ExecutionVertexStatus,
        error_message: str | None = None,
    ) -> None:
        # The status update + all-sinks-finished scan are atomic inside
        # on_task_status_report (one run_exclusive call); we only react here.
        if self.job_master is None:
            return
        if status == ExecutionVertexStatus.FAILED and not is_blank(error_message):
            vertex = self.execution_graph.execution_vertex(vertex_id) if self.execution_graph is not None else None
            task_name = vertex.name if vertex is not None else str(vertex_id)
            self._task_failure_details.setdefault(task_name, error_message)
        all_finished = await self.run_exclusive(
            self.job_master.on_task_status_report,
            vertex_id,
            status,
            error_message,
        )
        if not all_finished:
            return
        await self._stop_job()
        self._update_job_status(JobStatus.FINISHED)

    async def _stop_job(self, force: bool = False, timeout: int | None = None) -> None:
        # One Deadline bounds the whole teardown (supervisor stop + writer queue +
        # stop_job) so it can't hang. None => the configured job.stop.timeout.
        budget = timeout if timeout is not None else self.config.get(JobManagerOptions.STOP_TIMEOUT)
        deadline = Deadline(budget)

        await self.stop(timeout=deadline.remaining())
        # Wake any inner backoff in restart() so it observes the stop request.
        self._wake_event.set()
        # On timeout, skip the graceful stop (a stuck in-flight writer still holds
        # the worker) rather than block forever — the next force teardown cleans up.
        if self.job_master is not None:
            try:
                await asyncio.wait_for(
                    self.run_exclusive(self.job_master.stop_job, force, deadline),
                    timeout=deadline.remaining(),
                )
            except asyncio.TimeoutError:
                log_event(
                    logger,
                    logging.WARNING,
                    "job.stop.timed_out",
                    "Job stop exceeded its %.0fs budget while waiting for the writer queue",
                    budget,
                    job_id=self.namespace,
                    job_name=self.job_name,
                )

    async def output_queue(self) -> Queue:
        take_list = self.logical_graph.take_vertices
        if len(take_list) != 1:
            raise ValueError("`take` can be used if and only if exist one take operator")
        queue = self.execution_graph.job_vertex(take_list[0].index).output_queue
        if queue is None:
            raise ValueError("Output queue is not initialized. This is a bug.")
        return queue

    def job_status(self) -> JobStatus:
        # Intentionally synchronous: cheap status read, no I/O. Ray async actors
        # are happy to expose sync methods alongside async ones.
        return self._job_status

    async def failure_detail(self) -> str | None:
        if self._submission_error:
            return self._submission_error
        if self.execution_graph is None:
            return None
        failed_msgs = dict(self._task_failure_details)
        for vertex in self.execution_graph.execution_vertices:
            error_message = vertex.error_message
            if not is_blank(error_message):
                failed_msgs.setdefault(vertex.name, error_message)
        if len(failed_msgs) <= 0:
            return ""

        delimiter = "-" * 10 + "\n"
        detail = delimiter.join(f"[{k}] failed reason:\n {v}" for k, v in failed_msgs.items())
        detail = truncate_diagnostic(detail)
        return f"The failed reason of tasks: {'=' * 20} \n{detail}[END] {'=' * 20} \n"

    async def progress_snapshot(self) -> "ProgressSnapshot":
        """Per-operator progress + failover state for the CLI live view.

        Delegated to a read-only ProgressReporter (built lazily once the graph
        exists). Returns an empty snapshot before submit so a pre-submit poll is
        harmless.
        """
        from ray.klein.runtime.job_manager.progress import ProgressSnapshot
        from ray.klein.runtime.job_manager.progress_reporter import ProgressReporter

        if self.execution_graph is None:
            return ProgressSnapshot()
        if self._progress_reporter is None:
            self._progress_reporter = ProgressReporter(
                self.execution_graph,
                is_job_running=lambda: self._job_status == JobStatus.RUNNING,
                restart_window=self._restart_window,
            )
        return await self._progress_reporter.snapshot()

    async def dashboard_snapshot(self) -> dict:
        """Return one immutable, redacted, Flink-style job snapshot."""

        now_ms = int(time.time() * 1000)
        progress = await self.progress_snapshot()
        operators = [dashboard_value(operator) for operator in progress.operators]
        checkpoint = await self._checkpoint_dashboard_snapshot()
        status = self._job_status.name
        return {
            "job_id": self.namespace or self.job_name or "unknown",
            "job_name": self.job_name or "Unnamed Klein job",
            "namespace": self.namespace,
            "status": status,
            "created_at_ms": self._created_at_ms,
            "started_at_ms": self._started_at_ms,
            "ended_at_ms": self._ended_at_ms,
            "updated_at_ms": self._status_updated_at_ms,
            "duration_ms": (self._ended_at_ms or now_ms) - (self._started_at_ms or self._created_at_ms),
            "status_history": list(self._status_history),
            "operators": operators,
            "edges": [
                {"source": operator["op_id"], "target": target}
                for operator in operators
                for target in operator.get("downstream", [])
            ],
            "overview": {
                "operators": len(operators),
                "task_instances": sum(operator["parallelism"] for operator in operators),
                "rows_in": sum(operator["rows_in"] for operator in operators),
                "rows_out": sum(operator["rows_out"] for operator in operators),
                "restarts": progress.restarts,
                "max_restarts": progress.max_restarts,
                "restart_window_seconds": progress.window_seconds,
            },
            "checkpoints": checkpoint,
            "configuration": safe_configuration(self._job_config),
            "failure": await self.failure_detail() if status == JobStatus.FAILED.name else None,
        }

    async def _checkpoint_dashboard_snapshot(self) -> dict:
        if self.job_master is None or self.job_master.coordinator is None:
            return {
                "summary": {"total": 0, "completed": 0, "failed": 0, "in_progress": 0},
                "history": [],
                "latest_path": None,
            }
        try:
            return await klein.aget(self.job_master.coordinator.dashboard_snapshot())
        except Exception as error:
            logger.debug("Checkpoint dashboard snapshot unavailable: %s", error)
            return {
                "summary": {"total": 0, "completed": 0, "failed": 0, "in_progress": 0},
                "history": [],
                "latest_path": None,
                "error": f"{type(error).__name__}: {error}",
            }

    def _restart_window(self) -> tuple[int, int, int]:
        if self.job_master is None:
            return 0, 0, 0
        return self.job_master.restart_window()

    async def wait_until_terminal(self) -> JobStatus:
        """Block (async-wise) until the job reaches a terminal state.

        Enables JobClient.wait() to ``await`` instead of polling. The terminal
        event is set inside ``_update_job_status`` whenever we transition to
        FAILED / CANCELLED / FINISHED.
        """
        if self._job_status.is_terminal:
            return self._job_status
        await self._terminal_event.wait()
        return self._job_status

    def _update_job_status(self, status: JobStatus) -> None:
        log_event(
            logger,
            logging.INFO,
            "job.status.changed",
            "Job %s status changed from %s to %s",
            self.job_name,
            self._job_status.name,
            status.name,
            job_id=self.namespace,
            job_name=self.job_name,
            previous_status=self._job_status.name,
            status=status.name,
        )
        now_ms = int(time.time() * 1000)
        previous = self._job_status
        self._job_status = status
        self._status_updated_at_ms = now_ms
        self._status_history.append({"status": status.name, "previous_status": previous.name, "timestamp_ms": now_ms})
        if status == JobStatus.RUNNING and self._started_at_ms is None:
            self._started_at_ms = now_ms
        if status.is_terminal:
            self._ended_at_ms = now_ms
            self._terminal_event.set()

    def _failover_supervisor(self) -> FailoverSupervisor:
        # Borrows run_exclusive + the wake event so the supervisor owns only the
        # failover POLICY; the JobManager stays the single owner of write serialization.
        if self._supervisor is None:
            self._supervisor = FailoverSupervisor(
                job_master_provider=lambda: self.job_master,
                execution_graph_provider=lambda: self.execution_graph,
                run_exclusive=self.run_exclusive,
                wake_event_provider=lambda: self._wake_event,
                health_check_interval=self.health_check_interval,
                restart_delay_provider=self._restart_delay,
                on_permanent_failure=self._fail_permanently,
            )
        return self._supervisor

    def _restart_delay(self) -> float:
        if self.job_master is None:
            raise RuntimeError("restart delay is unavailable before job submission")
        return self.job_master.restart_delay()

    async def _fail_permanently(self, force: bool) -> None:
        """Tear the job down and mark it FAILED — the supervisor's SUPPRESSED path."""
        await self._stop_job(force=force)
        self._update_job_status(JobStatus.FAILED)

    async def restart(self, force: bool = False) -> None:
        if self.job_master is None:
            raise RuntimeError("the job must be submitted before it can be restarted")
        await self._failover_supervisor().restart(force=force)

    async def _run(self) -> None:
        # One supervisor tick. The FailoverSupervisor decides WHAT recovery to
        # attempt; the JobMaster owns HOW (the execution-graph writes).
        await self._failover_supervisor().tick()

    def _get_name(self) -> str:
        return "JobManager"

    def handle_exception(self, exc: Exception) -> None:
        log_event(
            logger,
            logging.ERROR,
            "job.supervisor.failed",
            "The job supervisor failed; terminating the job",
            exc_info=exc,
            job_id=self.namespace,
            job_name=self.job_name,
        )
        # Schedule the actual teardown as a separate task — handle_exception runs
        # in the loop and we can't `await` here.
        asyncio.get_running_loop().create_task(self._terminate_after_failure())

    async def _terminate_after_failure(self) -> None:
        await self._stop_job(force=True)
        self._update_job_status(JobStatus.FAILED)

    async def start_job_supervisor(self) -> None:
        log_event(
            logger,
            logging.INFO,
            "job.supervisor.started",
            "Started the job supervisor",
            job_id=self.namespace,
            job_name=self.job_name,
        )
        await self.start()
