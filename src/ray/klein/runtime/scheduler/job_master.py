# SPDX-License-Identifier: Apache-2.0

import logging

import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.deadline import Deadline
from ray.klein._internal.logging import get_logger, log_event
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.config.configuration import Configuration
from ray.klein.config.deployment_mode import DeploymentMode
from ray.klein.config.deployment_options import DeploymentOptions
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.observability.metrics.metrics import Counter
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.scheduler import task_deployer, task_terminator
from ray.klein.runtime.scheduler.errors import DeploymentError, PlacementError
from ray.klein.runtime.scheduler.placement import (
    NativeStrategy,
    PlacementGroupStrategy,
    PlacementStrategy,
    RoundRobinStrategy,
)
from ray.klein.runtime.scheduler.recovery_manager import RecoveryManager
from ray.klein.runtime.scheduler.restart_result import RestartResult, RestartStatus
from ray.klein.runtime.scheduler.restart_strategy import (
    create_restart_strategy,
    now_seconds,
)

logger = get_logger(__name__)


class JobMaster:
    """
    JobMaster — the single owner/writer of the job's ExecutionGraph.

    Drives deploy/stop/restart/recover, owns the coordinator handle + restart
    policy and placement-group lifecycle.
    """

    def __init__(
        self,
        execution_graph: ExecutionGraph,
        config: Configuration | None = None,
        metrics_group: JobMetricGroup | None = None,
    ) -> None:
        self.config = config if config is not None else Configuration()
        self.execution_graph = execution_graph
        self.namespace = execution_graph.namespace
        self.metrics_group = metrics_group
        self.coordinator: KleinActorHandle | None = None
        # Job-wide PlacementGroup; lifecycle tied to the job (removed on
        # stop/restart, rebuilt next schedule()). None when PG is off or fell back.
        self.placement_group = None
        self.failover_counter: Counter | None = (
            None if metrics_group is None else metrics_group.builtin_counter(KleinMetrics.JOB_RESTARTS)
        )
        self._restart_strategy = create_restart_strategy(self.config)
        self._schedule_start_timeout = self.config.get(JobManagerOptions.SCHEDULER_START_TIMEOUT)
        # Lightweight coordinator RPCs (read attr, flush, cancel) use this; the
        # heavy open/start go through _schedule_start_timeout (model load may
        # take minutes).
        self._coordinator_rpc_timeout = self.config.get(JobManagerOptions.COORDINATOR_RPC_TIMEOUT)
        # Whole-operation budgets: each step draws min(remaining, per-step cap)
        # from a Deadline, so total deploy/stop is bounded, not the sum of steps.
        self._deploy_timeout = self.config.get(JobManagerOptions.DEPLOY_TIMEOUT)
        self._stop_timeout = self.config.get(JobManagerOptions.STOP_TIMEOUT)
        self._recovery = RecoveryManager(
            execution_graph,
            coordinator_provider=lambda: self.coordinator,
            rpc_timeout=self._coordinator_rpc_timeout,
            start_timeout=self._schedule_start_timeout,
        )

    def schedule(self, restore_path: str | None = None) -> None:
        """Deploy the job as an ordered sequence of named stages.

        Every stage runs under one shared budget: a single ``Deadline`` built
        from ``job.deploy.timeout`` bounds the whole deploy, while each stage is
        additionally capped at ``_schedule_start_timeout`` (a single heavy step
        — model load, checkpoint restore — may legitimately take minutes). The
        stages run in order; the FIRST stage to fail raises ``DeploymentError``
        (wrapping any non-control-plane exception) and aborts the rest. This is
        the single catch site for the whole deploy path — the stage functions all
        ``raise`` rather than returning a (bool, err) tuple.
        """
        deadline = Deadline(self._deploy_timeout)
        stages = (
            ("create workers", self._create_workers),
            ("open coordinator", lambda: self._open_coordinator(restore_path, deadline)),
            ("deploy workers", lambda: task_deployer.deploy_workers(self.execution_graph)),
            (
                "start workers",
                lambda: task_deployer.start_workers(
                    self.execution_graph,
                    timeout=deadline.step(self._schedule_start_timeout),
                ),
            ),
            ("start coordinator", lambda: self._start_coordinator(deadline)),
        )
        for name, run_stage in stages:
            try:
                run_stage()
            except DeploymentError:
                raise
            except Exception as error:
                raise DeploymentError(name, error) from error

    def _open_coordinator(self, restore_path: str | None, deadline: Deadline) -> None:
        # namespace scopes the lookup to this job — without it a second job in the
        # cluster would attach to the first job's coordinator and corrupt its state.
        if self.coordinator is None or not self._coordinator_alive():
            self.coordinator = CheckpointCoordinator.open_or_create(
                self.config,
                namespace=self.namespace,
                job_name=None if self.metrics_group is None else self.metrics_group.job_name,
            )
        klein.get(
            self.coordinator.open(self.execution_graph, restore_path),
            timeout=deadline.step(self._schedule_start_timeout),
        )

    def _start_coordinator(self, deadline: Deadline) -> None:
        klein.get(self.coordinator.start(), timeout=deadline.step(self._schedule_start_timeout))

    def _coordinator_alive(self) -> bool:
        return (
            klein.get_actor_status(ComponentName.KLEIN_CHECKPOINT_COORDINATOR, namespace=self.namespace)
            == StreamTaskStatus.ALIVE
        )

    def recover_coordinator_if_needed(self) -> bool:
        """REGIONAL failover: re-open a Ray-rebuilt coordinator (single-writer
        thread). Delegates to the RecoveryManager; returns True if a recovery ran."""
        return self._recovery.recover_coordinator_if_needed()

    def stop_job(self, force: bool = False, deadline: Deadline | None = None) -> None:
        # None => default budget (internal callers like restart()).
        if deadline is None:
            deadline = Deadline(self._stop_timeout)
        log_event(
            logger,
            logging.INFO,
            "job.workers.stop_started",
            "Stopping all job workers",
            force=force,
        )
        # stop_workers force-kills survivors internally, so anything it still
        # raises is a genuine teardown failure restart() relies on to return FAILED.
        task_terminator.stop_workers(self.execution_graph, deadline.step(self._stop_timeout), force)
        # Release the PG so its bundles return to the cluster (rebuilt next schedule()).
        self._remove_placement_group()
        if self.coordinator is not None and self._coordinator_alive():
            # Terminal flush: persist progress since the last periodic snapshot
            # before teardown. Best-effort — a force kill may have removed sources.
            try:
                klein.get(
                    self.coordinator.persist_now(
                        notify_sources=False,
                        abort_inflight_sinks=True,
                    ),
                    timeout=deadline.step(self._coordinator_rpc_timeout),
                )
            except Exception as error:
                log_event(
                    logger,
                    logging.WARNING,
                    "checkpoint.terminal_flush.failed",
                    "Terminal checkpoint flush failed: %s",
                    error,
                    exc_info=True,
                )
            logger.info("Stopping the checkpoint coordinator")
            rpc_timeout = deadline.step(self._coordinator_rpc_timeout)
            klein.get(
                self.coordinator.stop(timeout=rpc_timeout),
                timeout=rpc_timeout,
            )

    def restart(self, force: bool = False) -> RestartResult:
        # The strategy's suppression window is NOT reset on a successful reschedule,
        # so a deterministic poison-pill can't restart forever — see RestartStrategy.
        now = now_seconds()
        should_suppress, attempts = self._restart_strategy.record_and_should_suppress(now)
        if self.failover_counter is not None:
            self.failover_counter.inc()
        _, max_attempts, window_s = self._restart_strategy.window_view(now)
        if should_suppress:
            reason = (
                f"Restart was suppressed: {attempts} restarts within the last {window_s}s exceeds limit {max_attempts}"
            )
            log_event(
                logger,
                logging.ERROR,
                "failover.global.suppressed",
                "Global restart was suppressed because the job is failing too quickly: %s",
                reason,
                attempts=attempts,
                max_attempts=max_attempts,
                window_seconds=window_s,
            )
            return RestartResult(RestartStatus.SUPPRESSED, reason)
        try:
            log_event(
                logger,
                logging.WARNING,
                "failover.global.started",
                "Starting global restart %d within the last %d seconds (limit %d)",
                attempts,
                window_s,
                max_attempts,
                attempts=attempts,
                max_attempts=max_attempts,
                window_seconds=window_s,
            )
            self.stop_job(force=force)
            if self.coordinator is not None:
                restore_path = klein.get(
                    self.coordinator.latest_checkpoint_path(),
                    timeout=self._coordinator_rpc_timeout,
                )
            else:
                restore_path = None
            # Backoff is the caller's responsibility (supervisor loop), so we don't
            # block the JobManager actor thread here.
            log_event(
                logger,
                logging.INFO,
                "failover.global.rescheduling",
                "Rescheduling the job from checkpoint %s",
                restore_path,
                checkpoint_path=restore_path,
            )
            self.schedule(restore_path)
            return RestartResult(RestartStatus.SUCCESS, "")
        except Exception as error:
            log_event(
                logger,
                logging.ERROR,
                "failover.global.failed",
                "Global restart failed: %s",
                error,
                exc_info=True,
            )
            return RestartResult(RestartStatus.FAILED, str(error))

    def try_recover_tasks(self) -> bool:
        """Tier-0 single-point task recovery (single-writer thread).

        Delegates to the RecoveryManager. Returns True if every non-terminal
        vertex is running or being rebuilt; False to escalate to a global restart
        (a genuinely unrecoverable vertex). See RecoveryManager.try_recover_tasks.
        """
        return self._recovery.try_recover_tasks()

    def list_source_task_handles(self) -> list[KleinActorHandle]:
        """Live StreamTask handles of all source subtasks (read-only).

        Lets the JobManager drive ``drain`` without walking execution-graph internals
        itself — the scheduler owns the graph, the JobManager only does the async
        gather on the returned handles.
        """
        return [
            vertex.stream_task
            for source_job_vertex_id in self.execution_graph.source_job_vertices
            for vertex in self.execution_graph.job_vertex(source_job_vertex_id).execution_vertices.values()
            if vertex.stream_task is not None
        ]

    def on_task_status_report(
        self,
        vertex_id: ExecutionVertexId,
        status: ExecutionVertexStatus,
        error_message: str | None = None,
    ) -> bool:
        """Apply a StreamTask's status report and report whether the job finished.

        The single-writer entry point for the status-report write. The JobManager
        calls it under _scheduler_lock, the same gate as schedule/restart/stop/
        recover, so this update + the all-sinks-finished scan can't interleave
        with any other execution-graph writer. Returns True iff every sink is FINISHED.
        """
        vertex = self.execution_graph.execution_vertex(vertex_id)
        if vertex is None:
            return False
        vertex.transition_to(status, error_message)
        if status != ExecutionVertexStatus.FINISHED:
            return False
        return all(
            sink.status == ExecutionVertexStatus.FINISHED for sink in self.execution_graph.sink_execution_vertices
        )

    def restart_delay(self) -> float:
        """Fixed-delay (seconds) between two restart attempts.

        Exposed so the caller can apply the backoff in an interruptible way instead of
        having the scheduler `time.sleep` inside the JobManager actor thread.
        """
        return self._restart_strategy.delay

    def restart_window(self) -> tuple[int, int, int]:
        """(restarts_in_window, max_attempts, window_seconds) for the CLI view.

        Delegates to the restart strategy, which prunes its window to *now*
        before counting so a poll between restart attempts reflects only
        restarts still inside the window.
        """
        return self._restart_strategy.window_view(now_seconds())

    def _create_workers(self) -> None:
        """Create workers by trying the configured placement strategies in order.

        Each strategy's ``plan`` raises ``PlacementError`` when infeasible; we log
        and fall through to the next. The last strategy (NativeStrategy) never
        raises PlacementError, so the cascade always terminates — any other
        failure surfaces as ``DeploymentError`` to ``schedule()``. The job's
        PlacementGroup (if one strategy created it) is held for lifecycle.
        """
        task_deployer.validate_vertex_statuses(self.execution_graph)
        # Reset any stale PG from a previous schedule before (re)placing.
        self._remove_placement_group()

        strategies = self._placement_strategies()
        last_error = None
        for strategy in strategies:
            try:
                plan = task_deployer.place_workers(self.execution_graph, strategy)
                self.placement_group = plan.placement_group
                return
            except PlacementError as error:
                last_error = error
                logger.warning("Placement strategy '%s' infeasible (%s); trying next.", strategy.name, error)
        # Unreachable: NativeStrategy is always last and never raises PlacementError.
        raise DeploymentError("create workers", last_error or "no placement strategy succeeded")

    def _placement_strategies(self) -> list[PlacementStrategy]:
        """The ordered placement cascade for this job.

        Debug mode (local objects, no cluster) or a head-only cluster (no
        schedulable worker nodes) → native only. Otherwise PlacementGroup (unless
        disabled or BALANCED mode) → Round-Robin → native.
        """
        if klein.is_debug_mode() or not task_deployer.has_schedulable_worker_nodes():
            return [NativeStrategy()]

        deploy_mode = self.config.get(DeploymentOptions.MODE)
        strategies = []
        if self.config.get(PipelineOptions.PLACEMENT_GROUP_ENABLED) and deploy_mode != DeploymentMode.BALANCED:
            strategies.append(
                PlacementGroupStrategy(
                    strategy=self.config.get(PipelineOptions.PLACEMENT_GROUP_STRATEGY),
                    ready_timeout=self.config.get(PipelineOptions.PLACEMENT_GROUP_READY_TIMEOUT).total_seconds(),
                )
            )
        strategies.extend((RoundRobinStrategy(), NativeStrategy()))
        return strategies

    def _remove_placement_group(self) -> None:
        """Remove the job's PlacementGroup if one is held (stop / re-place)."""
        if self.placement_group is None:
            return
        try:
            from ray.util.placement_group import remove_placement_group

            remove_placement_group(self.placement_group)
        except Exception as error:
            logger.warning("Failed to remove placement group: %s", error)
        finally:
            self.placement_group = None
