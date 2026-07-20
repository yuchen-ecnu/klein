# SPDX-License-Identifier: Apache-2.0
import asyncio
import logging
import re
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, TypeVar

from ray.exceptions import RayTaskError
from ray.util.queue import Queue

import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.deadline import Deadline
from ray.klein._internal.logging import get_logger, log_event
from ray.klein._internal.validation import is_blank
from ray.klein.api.job_status import JobStatus
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.state_options import StateOptions
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
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.job_manager.failover_supervisor import FailoverSupervisor
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.worker.async_worker import AsyncWorker

if TYPE_CHECKING:
    from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
    from ray.klein.runtime.job_manager.progress import ProgressSnapshot

logger = get_logger(__name__)

_T = TypeVar("_T")

_ANSI_ESCAPE_RE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|[@-_])")
_RESCALE_OPERATION_HISTORY_LIMIT = 20
_RESCALE_STABILIZATION_POLL_SECONDS = 0.25
_TERMINAL_RESCALE_STATUSES = frozenset({"COMPLETED", "NOOP", "REJECTED", "FAILED"})


def _format_rescale_error(error: BaseException) -> str:
    """Return the innermost useful exception without Ray's remote traceback."""

    cause = error
    seen: set[int] = set()
    while id(cause) not in seen:
        seen.add(id(cause))
        nested = None
        for attribute in ("cause", "__cause__", "__context__"):
            candidate = getattr(cause, attribute, None)
            if isinstance(candidate, BaseException) and id(candidate) not in seen:
                nested = candidate
                break
        if nested is None:
            break
        cause = nested
    if isinstance(cause, RayTaskError):
        return "RayTaskError: remote task failed"
    message = _ANSI_ESCAPE_RE.sub("", str(cause)).strip()
    return f"{type(cause).__name__}: {message}"


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
        self._rescale_in_progress = False
        self._rescale_done_obj: asyncio.Event | None = None
        self._rescale_operations: dict[str, dict[str, object]] = {}
        self._active_rescale_operation_id: str | None = None
        self._rescale_task_obj: asyncio.Task | None = None
        self._completion_task_obj: asyncio.Task[None] | None = None
        # Serialize whole lifecycle transactions (local rescale, cancellation
        # and failover), not only their individual execution-graph writes.
        self._lifecycle_lock_obj: asyncio.Lock | None = None
        self._lifecycle_stop_requested = False

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

    @property
    def _lifecycle_lock(self) -> asyncio.Lock:
        if self._lifecycle_lock_obj is None:
            self._lifecycle_lock_obj = asyncio.Lock()
        return self._lifecycle_lock_obj

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
        logical_graph: LogicalGraph,
        config: Configuration = None,
    ) -> bool:
        job_config = Configuration()
        job_config.update(self.config)
        if config is not None:
            job_config.update(config)
        self._job_config = job_config
        self.job_name = job_name
        self._submission_error = None
        self._lifecycle_stop_requested = False
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
        logger.debug("Submitted logical graph:\n%s", logical_graph)

        self.logical_graph = LogicalOptimizer(job_config).optimize(logical_graph)

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
            self._lifecycle_stop_requested = True
            self._wake_event.set()
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
            try:
                await self._stop_job()
            except Exception as teardown_error:
                self._record_teardown_failure("Deployment teardown failed", teardown_error)
                log_event(
                    logger,
                    logging.ERROR,
                    "job.deployment.teardown_failed",
                    "Failed to tear down partially deployed job %s",
                    job_name,
                    exc_info=True,
                    job_id=self.namespace,
                    job_name=job_name,
                )
            finally:
                self._update_job_status(JobStatus.FAILED)
            return False
        await self.start_job_supervisor()
        self._update_job_status(JobStatus.RUNNING)
        return True

    async def cancel(self, timeout: int | None = None) -> bool:
        if self.job_status().is_terminal:
            return False
        budget = timeout if timeout is not None else self.config.get(JobManagerOptions.STOP_TIMEOUT)
        self._lifecycle_stop_requested = True
        self._wake_event.set()
        try:
            await asyncio.wait_for(self._lifecycle_lock.acquire(), timeout=budget)
        except asyncio.TimeoutError:
            self._lifecycle_stop_requested = False
            return False
        try:
            if self.job_status().is_terminal:
                return False
            try:
                await self._stop_job(force=True, timeout=timeout)
            except Exception as error:
                self._record_teardown_failure("Cancellation teardown failed", error)
                log_event(
                    logger,
                    logging.ERROR,
                    "job.cancellation.teardown_failed",
                    "Failed to tear down cancelled job %s",
                    self.job_name,
                    exc_info=True,
                    job_id=self.namespace,
                    job_name=self.job_name,
                )
                self._update_job_status(JobStatus.FAILED)
                return False
            self._update_job_status(JobStatus.CANCELLED)
            return True
        finally:
            self._lifecycle_lock.release()

    @property
    def _rescale_done(self) -> asyncio.Event:
        if self._rescale_done_obj is None:
            self._rescale_done_obj = asyncio.Event()
            self._rescale_done_obj.set()
        return self._rescale_done_obj

    async def rescale_operator(self, operator_id: int | str, parallelism: int) -> dict:
        """Change one operator and retain the historical synchronous API."""

        while True:
            operation = await self.submit_operator_rescale(operator_id, parallelism)
            active_operation_id = operation.get("active_operation_id")
            if operation["status"] != "REJECTED" or active_operation_id is None:
                break
            # The old synchronous entry point serialized callers behind the
            # lifecycle lock. Keep that behavior even though the new admission
            # RPC correctly rejects a concurrent HTTP request immediately.
            await self._wait_for_rescale_operation_terminal(str(active_operation_id))
        if operation["status"] != "ACCEPTED":
            return operation
        return await self._wait_for_rescale_operation(str(operation["operation_id"]))

    async def submit_operator_rescale(self, operator_id: int | str, parallelism: int) -> dict:
        """Admit one rescale and return before its topology transaction runs.

        There is deliberately no ``await`` before the active slot is reserved.
        Ray async-actor calls can interleave at await points, so this makes the
        single-rescale admission decision atomic on the actor event loop.
        """

        active_operation_id = self._active_rescale_operation_id
        if active_operation_id is not None or self._rescale_in_progress:
            return self._rejected_rescale_submission(
                operator_id,
                parallelism,
                active_operation_id=active_operation_id,
                error="another operator rescale is already in progress",
            )

        # Validation is intentionally synchronous and precedes the first await,
        # so an unknown/unsupported operator never produces an orphan ACCEPTED
        # record that no operator row can display.  Graph construction and the
        # authoritative revalidation still happen under the lifecycle lock.
        try:
            vertex_id, logical_vertex = self._resolve_rescale_request(operator_id, parallelism)
        except (KeyError, TypeError, ValueError, RuntimeError) as error:
            return self._rejected_rescale_submission(
                operator_id,
                parallelism,
                active_operation_id=None,
                error=str(error),
                remember=True,
            )

        previous_parallelism = logical_vertex.concurrency
        if previous_parallelism == parallelism:
            return self._terminal_rescale_submission(
                vertex_id.index,
                parallelism,
                status="NOOP",
                operator_name=logical_vertex.name,
                previous_parallelism=previous_parallelism,
            )

        operation_id = uuid.uuid4().hex
        now_ms = int(time.time() * 1000)
        operation: dict[str, object] = {
            "operation_id": operation_id,
            "job_id": self.namespace,
            "operator_id": vertex_id.index,
            "operator_name": logical_vertex.name,
            "previous_parallelism": previous_parallelism,
            "parallelism": parallelism,
            "target_parallelism": parallelism,
            "status": "ACCEPTED",
            "phase": "QUEUED",
            "accepted_at_ms": now_ms,
            "started_at_ms": None,
            "updated_at_ms": now_ms,
            "ended_at_ms": None,
            "error": None,
        }
        self._remember_rescale_operation(operation)
        self._active_rescale_operation_id = operation_id
        self._rescale_in_progress = True
        self._rescale_done.clear()
        self._wake_event.set()
        self._rescale_task_obj = asyncio.create_task(
            self._execute_rescale_operation(operation_id, vertex_id.index, parallelism),
            name=f"klein-rescale-{operation_id[:8]}",
        )
        # Return an immutable point-in-time response.  The background task may
        # advance the authoritative record as soon as this actor method yields.
        return dict(operation)

    async def _wait_for_rescale_operation(self, operation_id: str) -> dict:
        """Wait for a submitted operation without coupling it to its caller."""

        while True:
            operation = self._rescale_operations.get(operation_id)
            if operation is None:
                raise KeyError(f"unknown operator rescale operation: {operation_id}")
            if operation["status"] == "STABILIZING":
                # Preserve the public synchronous API's historical contract:
                # COMPLETED means the topology commit finished.  The durable
                # operation record remains STABILIZING until its checkpoint.
                result = dict(operation)
                result["status"] = "COMPLETED"
                result["ended_at_ms"] = int(time.time() * 1000)
                return result
            if operation["status"] in _TERMINAL_RESCALE_STATUSES:
                return dict(operation)
            await self._rescale_done.wait()

    async def _wait_for_rescale_operation_terminal(self, operation_id: str) -> dict:
        """Wait through stabilization for synchronous request serialization."""

        while True:
            operation = self._rescale_operations.get(operation_id)
            if operation is None:
                raise KeyError(f"unknown operator rescale operation: {operation_id}")
            if operation["status"] in _TERMINAL_RESCALE_STATUSES:
                return dict(operation)
            await asyncio.sleep(_RESCALE_STABILIZATION_POLL_SECONDS)

    async def _execute_rescale_operation(
        self,
        operation_id: str,
        operator_id: int | str,
        parallelism: int,
    ) -> None:
        """Run an admitted rescale independently of the submitting client."""

        try:
            async with self._lifecycle_lock:
                self._update_rescale_operation(
                    operation_id,
                    status="RUNNING",
                    phase="COORDINATING",
                    started_at_ms=int(time.time() * 1000),
                )
                result = await self._rescale_operator_locked(
                    operator_id,
                    parallelism,
                    operation_id=operation_id,
                )
            self._merge_rescale_result(operation_id, result)
            if result["status"] == "COMPLETED":
                # The topology is live, but recovery must remain fenced until
                # its first checkpoint is durable.  Observe that fence outside
                # the lifecycle lock so cancellation and failover can proceed.
                self._update_rescale_operation(
                    operation_id,
                    status="STABILIZING",
                    phase="STABILIZING",
                )
                # Wake compatibility callers at topology commit. Coordinator
                # health or a slow stabilization checkpoint must not extend
                # their historical synchronous response latency.
                self._rescale_done.set()
                stabilization_error = await self._wait_for_rescale_stabilization(operation_id)
                if stabilization_error is None:
                    self._update_rescale_operation(
                        operation_id,
                        status="COMPLETED",
                        phase="COMPLETED",
                    )
                else:
                    self._update_rescale_operation(
                        operation_id,
                        status="FAILED",
                        phase="COMPLETED",
                        error=stabilization_error,
                    )
            else:
                self._update_rescale_operation(
                    operation_id,
                    status=str(result["status"]),
                    phase="COMPLETED",
                    error=result.get("error"),
                )
        except asyncio.CancelledError:
            self._update_rescale_operation(
                operation_id,
                status="FAILED",
                phase="COMPLETED",
                error="operator rescale task was cancelled",
            )
            raise
        except Exception as error:
            self._update_rescale_operation(
                operation_id,
                status="FAILED",
                phase="COMPLETED",
                error=_format_rescale_error(error),
            )
        finally:
            if self._active_rescale_operation_id == operation_id:
                self._active_rescale_operation_id = None
            self._rescale_in_progress = False
            self._rescale_done.set()
            if self._rescale_task_obj is asyncio.current_task():
                self._rescale_task_obj = None

    async def _wait_for_rescale_stabilization(self, operation_id: str) -> str | None:
        while self._active_rescale_operation_id == operation_id:
            fenced = await self._rescale_recovery_fenced()
            if fenced is False:
                return None
            # Probe the fence first so a job that finishes immediately after a
            # durable checkpoint is not reported as a failed rescale.
            if self._job_status.is_terminal:
                if await self._rescale_recovery_fenced() is False:
                    return None
                return f"job became {self._job_status.name} before the stabilization checkpoint completed"
            await asyncio.sleep(1.0 if fenced is None else _RESCALE_STABILIZATION_POLL_SECONDS)
        return "operator rescale was superseded before stabilization completed"

    async def _rescale_recovery_fenced(self) -> bool | None:
        coordinator = None if self.job_master is None else getattr(self.job_master, "coordinator", None)
        if coordinator is None:
            return False
        try:
            result = await klein.aget(
                coordinator.operator_rescale_recovery_fenced(),
                timeout=self.config.get(JobManagerOptions.COORDINATOR_RPC_TIMEOUT),
            )
        except Exception:
            logger.debug("Unable to inspect operator rescale stabilization fence", exc_info=True)
            return None
        # Test doubles and a mismatched coordinator must not create an endless
        # stabilization wait.  The real coordinator contract returns bool.
        return result if type(result) is bool else False

    def _remember_rescale_operation(self, operation: dict[str, object]) -> None:
        operation_id = str(operation["operation_id"])
        self._rescale_operations[operation_id] = operation
        while len(self._rescale_operations) > _RESCALE_OPERATION_HISTORY_LIMIT:
            oldest_operation_id = next(iter(self._rescale_operations))
            if oldest_operation_id == self._active_rescale_operation_id:
                break
            self._rescale_operations.pop(oldest_operation_id)

    def _update_rescale_operation(self, operation_id: str, **updates: object) -> None:
        operation = self._rescale_operations[operation_id]
        operation.update(updates)
        now_ms = int(time.time() * 1000)
        operation["updated_at_ms"] = now_ms
        if operation["status"] in _TERMINAL_RESCALE_STATUSES:
            operation["ended_at_ms"] = now_ms

    def _merge_rescale_result(self, operation_id: str, result: dict) -> None:
        operation = self._rescale_operations[operation_id]
        for key in (
            "operator_id",
            "operator_name",
            "previous_parallelism",
            "parallelism",
            "target_parallelism",
            "error",
        ):
            if key in result:
                operation[key] = result[key]
        operation["updated_at_ms"] = int(time.time() * 1000)

    def _rejected_rescale_submission(
        self,
        operator_id: int | str,
        parallelism: int,
        *,
        active_operation_id: str | None,
        error: str,
        remember: bool = False,
    ) -> dict:
        operation = self._terminal_rescale_submission(
            operator_id,
            parallelism,
            status="REJECTED",
            error=error,
            remember=remember,
        )
        if active_operation_id is not None:
            operation["active_operation_id"] = active_operation_id
        return operation

    def _terminal_rescale_submission(
        self,
        operator_id: int | str,
        parallelism: int,
        *,
        status: str,
        operator_name: str | None = None,
        previous_parallelism: int | None = None,
        error: str | None = None,
        remember: bool = True,
    ) -> dict:
        now_ms = int(time.time() * 1000)
        operation: dict[str, object] = {
            "operation_id": uuid.uuid4().hex,
            "job_id": self.namespace,
            "operator_id": operator_id,
            "operator_name": operator_name,
            "previous_parallelism": previous_parallelism,
            "parallelism": parallelism,
            "target_parallelism": parallelism,
            "status": status,
            "phase": "COMPLETED",
            "accepted_at_ms": now_ms,
            "started_at_ms": now_ms,
            "updated_at_ms": now_ms,
            "ended_at_ms": now_ms,
            "error": error,
        }
        if remember:
            self._remember_rescale_operation(operation)
        return dict(operation)

    async def _rescale_operator_locked(
        self,
        operator_id: int | str,
        parallelism: int,
        *,
        operation_id: str,
    ) -> dict:
        """Run one local rescale while holding the lifecycle transaction gate."""

        started_at_ms = int(self._rescale_operations[operation_id]["started_at_ms"] or time.time() * 1000)
        try:
            vertex_id, logical_vertex = self._resolve_rescale_request(operator_id, parallelism)
        except (KeyError, TypeError, ValueError, RuntimeError) as error:
            return self._rescale_result(started_at_ms, operator_id, parallelism, "REJECTED", error=str(error))
        previous = logical_vertex.concurrency
        if previous == parallelism:
            return self._rescale_result(
                started_at_ms,
                vertex_id.index,
                parallelism,
                "NOOP",
                logical_vertex.name,
                previous,
            )
        try:
            resized_logical_graph = self.logical_graph.rescale_operator(vertex_id.index, parallelism)
        except Exception as error:
            return self._rescale_result(
                started_at_ms,
                vertex_id.index,
                parallelism,
                "REJECTED",
                logical_vertex.name,
                previous,
                f"{type(error).__name__}: {error}",
            )

        error = await self._run_operator_rescale(
            vertex_id.index,
            parallelism,
            resized_logical_graph,
            operation_id,
        )
        return self._rescale_result(
            started_at_ms,
            vertex_id.index,
            parallelism,
            "COMPLETED" if error is None else "FAILED",
            logical_vertex.name,
            previous,
            error,
        )

    def _resolve_rescale_request(
        self,
        operator_id: int | str,
        parallelism: int,
    ) -> tuple[VertexId, VertexSpec]:
        if self._job_status != JobStatus.RUNNING:
            raise RuntimeError("operator rescale requires a RUNNING job")
        if isinstance(parallelism, bool) or not isinstance(parallelism, int) or parallelism <= 0:
            raise ValueError("parallelism must be a positive integer")
        if self.logical_graph is None or self.execution_graph is None or self.job_master is None:
            raise RuntimeError("job topology is unavailable")
        vertex_id = self.logical_graph.resolve_operator(operator_id)
        unavailable = [
            vertex
            for vertex in self._rescale_domain_vertices(vertex_id.index)
            if vertex.status != ExecutionVertexStatus.RUNNING
        ]
        if unavailable:
            names = ", ".join(vertex.name for vertex in unavailable)
            raise RuntimeError(
                "operator rescale requires every task instance in the target checkpoint domain "
                f"to be RUNNING; unavailable: {names}"
            )
        logical_vertex = self.logical_graph.get(vertex_id)
        if logical_vertex.concurrency != parallelism:
            error = self._unsupported_rescale_reason(logical_vertex, parallelism)
            if error is not None:
                raise ValueError(error)
        return vertex_id, logical_vertex

    def _rescale_domain_vertices(self, operator_id: int) -> tuple["ExecutionVertex", ...]:
        """Return the physical checkpoint scope that must be live for rescale."""

        if self.execution_graph is None:
            return ()
        vertex_ids = {
            execution_vertex_id
            for domain in self.execution_graph.checkpoint_domains_for_job_vertex(operator_id)
            for execution_vertex_id in domain.vertex_ids
        }
        return tuple(
            self.execution_graph.execution_vertex(execution_vertex_id)
            for execution_vertex_id in sorted(
                vertex_ids,
                key=lambda item: (item.job_vertex_id, item.index),
            )
        )

    def _unsupported_rescale_reason(self, logical_vertex, parallelism: int) -> str | None:
        max_parallelism = (self._job_config or self.config).get(StateOptions.MAX_PARALLELISM)
        if logical_vertex.operator.stateful and parallelism > max_parallelism:
            return f"parallelism {parallelism} exceeds state.keyed.max-parallelism={max_parallelism}"
        if logical_vertex.operator.source:
            return "source operators cannot be locally rescaled"
        if logical_vertex.operator.transactional_sink:
            return "transactional sink operators cannot be locally rescaled"
        if logical_vertex.operator.collecting:
            return "collecting sink operators cannot be locally rescaled"
        if not logical_vertex.operator.supports_concurrent_rescale:
            return (
                "operator lifecycle does not allow an old and pending runtime to overlap; "
                "set supports_concurrent_rescale=True only when its external resources support handoff"
            )
        return None

    async def _run_operator_rescale(
        self,
        target_id: int,
        parallelism: int,
        logical_graph: LogicalGraph,
        operation_id: str | None = None,
    ) -> str | None:
        operation_id = operation_id or uuid.uuid4().hex
        # Observability is best-effort and must never reject a valid topology
        # change.  Actor RPC failures are already represented by cached/zero
        # counts inside ProgressReporter; guard structural failures as well.
        try:
            await self.progress_snapshot()
        except Exception:
            logger.debug("Unable to capture progress before operator rescale", exc_info=True)
        try:
            await self.run_exclusive(
                self.job_master.rescale_operator,
                target_id,
                parallelism,
                operation_id,
            )
        except Exception as error:
            # The topology commit precedes checkpoint-gate release. If that
            # post-commit RPC fails, JobMaster deliberately retains the new
            # graph and forces global recovery; keep health reporting and the
            # Dashboard on that same committed graph even though the request
            # itself reports FAILED.
            committed_graph = self._authoritative_rescale_graph(target_id, parallelism)
            if committed_graph is not None:
                self.logical_graph = logical_graph
                self.execution_graph = committed_graph
                await self._replace_progress_execution_graph(committed_graph)
            return _format_rescale_error(error)
        else:
            committed_graph = self._authoritative_rescale_graph(target_id, parallelism)
            if committed_graph is None:
                # Lightweight unit/debug doubles do not own a real graph. The
                # production JobMaster always returns the writer-owned graph.
                committed_graph = self.execution_graph.resize_operator(target_id, parallelism)
            self.logical_graph = logical_graph
            self.execution_graph = committed_graph
            await self._replace_progress_execution_graph(committed_graph)
            return None

    def _authoritative_rescale_graph(
        self,
        target_id: int,
        parallelism: int,
    ) -> ExecutionGraph | None:
        graph = getattr(self.job_master, "execution_graph", None)
        if not isinstance(graph, ExecutionGraph):
            return None
        try:
            return graph if graph.job_vertex(target_id).concurrency == parallelism else None
        except KeyError:
            return None

    async def _replace_progress_execution_graph(self, execution_graph: ExecutionGraph) -> None:
        retired_counts = self.job_master.take_retired_rescale_counts()
        if self._progress_reporter is None:
            return
        try:
            await self._progress_reporter.replace_execution_graph(
                execution_graph,
                retired_counts,
            )
        except Exception:
            # Keep a committed data-plane transaction successful even if the
            # best-effort view cannot migrate its cache.
            logger.warning("Unable to retain progress counters across operator rescale", exc_info=True)
            self._progress_reporter = None

    def _rescale_result(
        self,
        started_at_ms: int,
        operator_id: int | str,
        parallelism: int,
        status: str,
        operator_name: str | None = None,
        previous_parallelism: int | None = None,
        error: str | None = None,
    ) -> dict:
        return {
            "job_id": self.namespace,
            "operator_id": operator_id,
            "operator_name": operator_name,
            "previous_parallelism": previous_parallelism,
            "parallelism": parallelism,
            "target_parallelism": parallelism,
            "status": status,
            "started_at_ms": started_at_ms,
            "ended_at_ms": int(time.time() * 1000),
            "error": error,
        }

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
        task_name: str | None = None,
        task_generation: str | None = None,
    ) -> None:
        # The status update + all-sinks-finished scan are atomic inside
        # on_task_status_report (one run_exclusive call); we only react here.
        # Once lifecycle teardown starts, its writer transaction owns the graph
        # until every worker is gone. A late FINISHED/FAILED report must neither
        # mutate that graph nor retain an actor RPC while the actor is closing.
        if self.job_master is None or self._lifecycle_stop_requested:
            return
        accepted, all_finished, resolved_name = await self.run_exclusive(
            self.job_master.apply_task_status_report,
            vertex_id,
            status,
            error_message,
            task_name,
            task_generation,
        )
        if not accepted:
            return
        if status == ExecutionVertexStatus.FAILED and not is_blank(error_message):
            self._task_failure_details.setdefault(resolved_name or task_name or str(vertex_id), error_message)
        if not all_finished:
            return
        self._lifecycle_stop_requested = True
        self._wake_event.set()
        completion_task = self._completion_task_obj
        if completion_task is None or completion_task.done():
            self._completion_task_obj = asyncio.create_task(
                self._complete_finished_job(),
                name=f"{self.namespace}-complete-job",
            )

    async def _complete_finished_job(self) -> None:
        """Tear down a naturally finished job outside the reporting worker RPC.

        A terminal worker reports its status while still unwinding its pump and
        executor.  Running the survivor sweep inline in that status RPC can
        synchronously re-enter ``worker.stop()`` (notably for debug actors),
        deadlocking the worker that is waiting for the RPC response.  Deferring
        the lifecycle transaction lets the report return before actor cleanup.
        """

        try:
            async with self._lifecycle_lock:
                if self._job_status.is_terminal:
                    return
                try:
                    await self._stop_job()
                except Exception as error:
                    self._submission_error = f"{type(error).__name__}: {error}"
                    log_event(
                        logger,
                        logging.ERROR,
                        "job.completion.teardown_failed",
                        "Failed to tear down naturally completed job %s",
                        self.job_name,
                        exc_info=True,
                        job_id=self.namespace,
                        job_name=self.job_name,
                    )
                    self._update_job_status(JobStatus.FAILED)
                else:
                    self._update_job_status(JobStatus.FINISHED)
        finally:
            if self._completion_task_obj is asyncio.current_task():
                self._completion_task_obj = None

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
        checkpoint = await self._checkpoint_dashboard_snapshot()
        rescale_stabilizing = bool(checkpoint.get("rescale_recovery_fenced", False))
        rescale_operations = [
            dashboard_value(dict(operation)) for operation in reversed(tuple(self._rescale_operations.values()))
        ]
        operators = [dashboard_value(operator) for operator in progress.operators]
        for operator in operators:
            job_vertex = (
                None if self.execution_graph is None else self.execution_graph.find_job_vertex(operator["op_id"])
            )
            domain_rescalable = job_vertex is not None and all(
                vertex.status == ExecutionVertexStatus.RUNNING
                for vertex in self._rescale_domain_vertices(operator["op_id"])
            )
            reason = None
            if job_vertex is None:
                reason = "Operator topology is unavailable."
            elif not domain_rescalable:
                reason = "Every task instance in this operator's CheckpointDomain must be RUNNING."
            elif rescale_stabilizing:
                reason = "The previous operator rescale is waiting for its stabilization checkpoint."
            elif job_vertex.operator_spec.source:
                reason = "Source operators cannot be locally rescaled."
            elif job_vertex.operator_spec.transactional_sink:
                reason = "Transactional sink operators cannot be locally rescaled."
            elif job_vertex.operator_spec.collecting:
                reason = "Collecting sink operators cannot be locally rescaled."
            elif not job_vertex.operator_spec.supports_concurrent_rescale:
                reason = "The operator lifecycle has not opted into concurrent runtime handoff."
            elif self._rescale_in_progress:
                reason = "Another operator rescale is already in progress."
            operator["can_rescale"] = reason is None and self._job_status == JobStatus.RUNNING
            operator["rescale_disabled_reason"] = (
                reason
                if reason is not None
                else (None if self._job_status == JobStatus.RUNNING else "The job is not running.")
            )
            operator["rescale_operation"] = next(
                (
                    operation
                    for operation in rescale_operations
                    if str(operation.get("operator_id")) == str(operator["op_id"])
                ),
                None,
            )
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
            "rescale_operations": rescale_operations,
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
                "bytes_in": sum(operator["bytes_in"] for operator in operators),
                "bytes_out": sum(operator["bytes_out"] for operator in operators),
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
                stop_requested_provider=lambda: self._lifecycle_stop_requested,
                health_probe_timeout=self.config.get(JobManagerOptions.COORDINATOR_RPC_TIMEOUT),
            )
        return self._supervisor

    def _restart_delay(self) -> float:
        if self.job_master is None:
            raise RuntimeError("restart delay is unavailable before job submission")
        return self.job_master.restart_delay()

    async def _fail_permanently(self, force: bool) -> None:
        """Tear the job down and mark it FAILED — the supervisor's SUPPRESSED path."""
        self._lifecycle_stop_requested = True
        self._wake_event.set()
        try:
            await self._stop_job(force=force)
        except Exception as error:
            self._record_teardown_failure("Permanent-failure teardown failed", error)
            log_event(
                logger,
                logging.ERROR,
                "job.permanent_failure.teardown_failed",
                "Failed to tear down permanently failed job %s",
                self.job_name,
                exc_info=True,
                job_id=self.namespace,
                job_name=self.job_name,
            )
        finally:
            # Teardown errors must not strand a fenced job in RUNNING forever.
            self._update_job_status(JobStatus.FAILED)

    async def restart(self, force: bool = False) -> None:
        if self.job_master is None:
            raise RuntimeError("the job must be submitted before it can be restarted")
        self._wake_event.set()
        async with self._lifecycle_lock:
            await self._failover_supervisor().restart(force=force)

    async def _run(self) -> None:
        # One supervisor tick. The FailoverSupervisor decides WHAT recovery to
        # attempt; the JobMaster owns HOW (the execution-graph writes).
        async with self._lifecycle_lock:
            if self._lifecycle_stop_requested:
                return
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
        self._lifecycle_stop_requested = True
        self._wake_event.set()
        async with self._lifecycle_lock:
            try:
                await self._stop_job(force=True)
            except Exception as error:
                self._record_teardown_failure("Supervisor-failure teardown failed", error)
                log_event(
                    logger,
                    logging.ERROR,
                    "job.supervisor.teardown_failed",
                    "Failed to tear down job %s after a supervisor failure",
                    self.job_name,
                    exc_info=True,
                    job_id=self.namespace,
                    job_name=self.job_name,
                )
            finally:
                self._update_job_status(JobStatus.FAILED)

    def _record_teardown_failure(self, context: str, error: Exception) -> None:
        """Preserve the root cause while retaining cleanup diagnostics."""

        diagnostic = f"{type(error).__name__}: {error}"
        if self._submission_error:
            self._submission_error = f"{self._submission_error}\n{context}: {diagnostic}"
        else:
            self._submission_error = diagnostic

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
