# SPDX-License-Identifier: Apache-2.0

import logging
from typing import TYPE_CHECKING, Any

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
from ray.klein.runtime.execution_graph.checkpoint_domain import CheckpointDomain
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.job_manager.progress import SubtaskCounts
from ray.klein.runtime.scheduler import task_deployer, task_terminator
from ray.klein.runtime.scheduler.errors import (
    DeploymentError,
    PlacementCleanupError,
    PlacementError,
)
from ray.klein.runtime.scheduler.placement import (
    NativeStrategy,
    PlacementGroupStrategy,
    PlacementPlan,
    PlacementStrategy,
    PlacementTransition,
    RoundRobinStrategy,
)
from ray.klein.runtime.scheduler.recovery_manager import RecoveryManager
from ray.klein.runtime.scheduler.rescale_plan import (
    RescalePhase,
    RescalePlan,
    RescaleTransaction,
)
from ray.klein.runtime.scheduler.restart_result import RestartResult, RestartStatus
from ray.klein.runtime.scheduler.restart_strategy import (
    create_restart_strategy,
    now_seconds,
)
from ray.klein.state.state_snapshot_reference import StateSnapshotReference

if TYPE_CHECKING:
    from ray.klein.runtime.scheduler.task_deployment_descriptor import (
        TaskDeploymentDescriptor,
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
        # The resolved placement owns every external reservation for this job.
        # Built-in PG placement uses independently releasable actor groups so
        # local scale-in can return resources without moving retained actors.
        self.placement_plan: PlacementPlan | None = None
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
        # A failed Ray kill must not make a rescale-only vertex unreachable
        # after its candidate/old graph is discarded. Keep the physical wrapper
        # until a later rescale or whole-job stop confirms the name is gone.
        self._pending_rescale_actor_cleanup: dict[str, tuple[ExecutionJobVertex, ExecutionVertex]] = {}
        self._pending_placement_cleanup: list[PlacementTransition | PlacementPlan] = []
        self._pending_rescale_state_cleanup: set[tuple[str, int]] = set()
        self._last_rescale_retired_counts: dict[ExecutionVertexId, SubtaskCounts] = {}

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
        teardown_errors = self._stop_workers_and_placement(force, deadline)
        coordinator_error = self._stop_coordinator(deadline)
        if coordinator_error is not None:
            teardown_errors.append(coordinator_error)
        if teardown_errors:
            summary = "; ".join(f"{type(error).__name__}: {error}" for error in teardown_errors)
            raise RuntimeError(f"job teardown was incomplete: {summary}") from teardown_errors[0]

    def _stop_workers_and_placement(self, force: bool, deadline: Deadline) -> list[Exception]:
        worker_error: Exception | None = None
        teardown_errors: list[Exception] = []
        try:
            task_terminator.stop_workers(self.execution_graph, deadline.step(self._stop_timeout), force)
        except Exception as error:
            # Releasing an elastic actor PG can finish terminating a Ray actor
            # whose explicit kill was temporarily unconfirmed. Reconcile once
            # more after reservation teardown before declaring stop failure.
            worker_error = error
        try:
            self._cleanup_pending_rescale_actors(deadline.step(self._stop_timeout))
        except Exception as error:
            teardown_errors.append(error)
        try:
            self._reconcile_pending_placement_cleanup()
        except Exception as error:
            teardown_errors.append(error)
        try:
            self._close_placement_plan()
        except Exception as error:
            teardown_errors.append(error)
        if worker_error is not None:
            try:
                task_terminator.force_kill_survivors(
                    self.execution_graph,
                    deadline.step(self._stop_timeout),
                )
            except Exception as error:
                teardown_errors.append(error)
        return teardown_errors

    def _stop_coordinator(self, deadline: Deadline) -> Exception | None:
        if self.coordinator is None or not self._coordinator_alive():
            return None
        # Terminal flush is best-effort because force kill may have removed sources.
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
        try:
            klein.get(self.coordinator.stop(timeout=rpc_timeout), timeout=rpc_timeout)
        except Exception as error:
            return error
        return None

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
            # A successful whole-job restore establishes one consistent graph
            # and checkpoint epoch. Retire any one-shot force-global policy
            # installed for an uncertain local-rescale commit/rollback; keeping
            # it would disable Tier-0 recovery forever after the job is healthy.
            self._replace_recovery_graph(self.execution_graph)
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
        self.reconcile_rescale_cleanup()
        return self._recovery.try_recover_tasks()

    def reconcile_rescale_cleanup(self) -> bool:
        """Best-effort background reconciliation run by every health tick."""

        clean = True
        try:
            self._cleanup_pending_rescale_actors(self._coordinator_rpc_timeout)
        except Exception:
            clean = False
            logger.warning("Pending rescale actor cleanup is still incomplete", exc_info=True)
        try:
            self._reconcile_pending_placement_cleanup()
        except Exception:
            clean = False
        try:
            self._reconcile_pending_rescale_state_cleanup(self._coordinator_rpc_timeout)
        except Exception:
            clean = False
            logger.warning("Transient rescale-state cleanup is still incomplete", exc_info=True)
        return clean

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

    def rescale_operator(
        self,
        target_id: int,
        parallelism: int,
        operation_id: str,
    ) -> None:
        """Resize one operator while preserving every overlapping actor.

        Scale-out creates only the added physical subtasks; scale-in retires
        only the surplus subtasks.  Direct upstream tasks insert an ordered
        local fence and pause.  The old target aligns those fences, snapshots
        managed state and forwards a fence downstream.  Retained actors prepare
        a second runtime under the new parallelism, then atomically swap it at
        the topology commit while keeping their Ray actor identity.
        """

        forward_budget, compensation_budget = self._rescale_timeout_budgets()
        deadline = Deadline(forward_budget)
        self._last_rescale_retired_counts = {}
        self._cleanup_pending_rescale_actors(deadline.step(self._stop_timeout))
        self._reconcile_pending_placement_cleanup()
        self._reconcile_pending_rescale_state_cleanup(deadline.step(self._coordinator_rpc_timeout))
        plan = RescalePlan.build(self.execution_graph, target_id, parallelism, operation_id)
        self._validate_local_rescale(plan)
        transaction = RescaleTransaction(plan)
        placement_transition = self._begin_rescale_placement(
            plan,
            deadline.step(self._schedule_start_timeout),
        )
        try:
            self._apply_local_rescale(
                plan,
                deadline,
                transaction,
                placement_transition,
                compensation_budget,
            )
        except Exception:
            if transaction.committed:
                logger.exception(
                    "Local rescale of operator %s failed after its commit point; retaining the new topology",
                    plan.old_target.name,
                )
                raise
            logger.exception(
                "Local rescale of operator %s failed; restoring the old topology",
                plan.old_target.name,
            )
            self._rollback_local_rescale(
                plan,
                Deadline(compensation_budget + deadline.remaining()),
                transaction,
                placement_transition,
            )
            raise

    def _rescale_timeout_budgets(self) -> tuple[float, float]:
        """Split one hard rescale timeout and reserve bounded compensation."""

        total = max(0.0, float(self._schedule_start_timeout))
        reserve_target = max(
            float(self._stop_timeout),
            float(self._coordinator_rpc_timeout),
            total * 0.2,
        )
        compensation = min(total / 2.0, reserve_target)
        return total - compensation, compensation

    def _validate_local_rescale(self, plan: RescalePlan) -> None:
        old_target = plan.old_target
        if old_target.operator_spec.source:
            raise ValueError("source operators cannot be locally rescaled")
        if old_target.operator_spec.transactional_sink:
            raise ValueError("transactional sink operators cannot be locally rescaled")
        if old_target.operator_spec.collecting:
            raise ValueError("collecting sink operators cannot be locally rescaled")
        if not old_target.operator_spec.supports_concurrent_rescale:
            raise ValueError(
                "operator lifecycle does not allow an old and pending runtime to overlap; "
                "set supports_concurrent_rescale=True only when its external resources support handoff"
            )
        unavailable_sources = [
            source
            for source in self._rescale_stabilization_sources(plan.old_graph, plan.target_id)
            if source.stream_task is None or source.status != ExecutionVertexStatus.RUNNING
        ]
        if unavailable_sources:
            names = ", ".join(source.name for source in unavailable_sources)
            raise RuntimeError(
                "local rescale requires every source subtask in the target dataflow component "
                "to be running for the shared "
                f"stabilization checkpoint; unavailable: {names}"
            )
        if self.coordinator is None or not self._coordinator_alive():
            raise RuntimeError("checkpoint coordinator is unavailable")

    def _apply_local_rescale(
        self,
        plan: RescalePlan,
        deadline: Deadline,
        transaction: RescaleTransaction,
        placement_transition: PlacementTransition,
        compensation_budget: float | None = None,
    ) -> None:
        execution_graph = plan.new_graph
        operation_id = plan.operation_id
        target_id = plan.target_id
        old_graph = plan.old_graph
        old_target = plan.old_target
        new_target = plan.new_target
        delta = plan.delta
        execution_graph.mark_rescale_epoch(target_id, operation_id)
        for vertex in new_target.execution_vertices.values():
            # The committed graph must retain this identity until its first
            # durable checkpoint, including for actor wrappers that are reused.
            vertex.restore_operation_id = operation_id

        # Allocate and confirm only the scale-out delta before inserting the
        # data-plane fence.  Ray actor creation is asynchronous, so ping is the
        # point at which resource placement and construction are known to have
        # completed.  Stateful setup still waits for the local-cut snapshots.
        transaction.enter(RescalePhase.CANDIDATES)
        if delta.added:
            task_deployer.instantiate_job_vertex(
                execution_graph,
                new_target,
                placement_transition.candidate_plan,
                restore_operation_id=operation_id,
                vertices=delta.added,
            )
            task_deployer.deploy_job_vertex(new_target, vertices=delta.added)
            task_deployer.wait_job_vertex_created(
                new_target,
                deadline.step(self._schedule_start_timeout),
                vertices=delta.added,
            )

        transaction.enter(RescalePhase.CHECKPOINT_GATE)
        timeout = deadline.step(self._coordinator_rpc_timeout)
        gate_acquired = klein.get(
            self.coordinator.begin_operator_rescale(operation_id, timeout),
            timeout=timeout,
        )
        if gate_acquired is not True:
            raise RuntimeError(f"checkpoint coordinator rejected operator rescale {operation_id}")
        if not self._recovery.clear_stable_rescale_metadata(deadline.step(self._coordinator_rpc_timeout)):
            raise RuntimeError("could not retire the previous durable rescale identity")
        transaction.enter(RescalePhase.PARTICIPANTS)
        participants = self._prepare_local_rescale(
            old_graph,
            target_id,
            operation_id,
            deadline.step(self._schedule_start_timeout),
        )
        snapshots = self._await_local_rescale_cut(
            old_target,
            participants,
            operation_id,
            deadline.step(self._schedule_start_timeout),
        )
        self._capture_retired_progress(plan, deadline.step(self._coordinator_rpc_timeout))
        self._stage_local_rescale_state(
            plan,
            snapshots,
            deadline.step(self._coordinator_rpc_timeout),
        )
        if delta.added:
            task_deployer.start_job_vertex(
                new_target,
                deadline.step(self._schedule_start_timeout),
                paused_operation_id=operation_id,
                vertices=delta.added,
            )
        transaction.enter(RescalePhase.RUNTIMES)
        self._prepare_retained_target_runtimes(
            execution_graph,
            new_target,
            delta.retained,
            operation_id,
            deadline.step(self._schedule_start_timeout),
        )
        # From prepare through coordinator activation every task remains fenced,
        # and old EdgeOutput objects remain available for an exact rollback.
        transaction.enter(RescalePhase.ROUTES)
        live_handles = self._prepare_live_task_topologies(
            execution_graph,
            target_id,
            operation_id,
            deadline.step(self._schedule_start_timeout),
            live_graph=old_graph,
        )
        self._activate_live_task_topologies(
            live_handles,
            operation_id,
            deadline.step(self._schedule_start_timeout),
        )
        transaction.enter(RescalePhase.COORDINATOR)
        klein.get(
            self.coordinator.reconfigure_execution_graph(execution_graph),
            timeout=deadline.step(self._coordinator_rpc_timeout),
        )

        # A live-task commit RPC may succeed on only a subset before its batch
        # acknowledgement is lost. Select the roll-forward graph and recovery
        # policy *before* attempting that irreversible call, so every uncertain
        # result is recovered against the new topology.
        self.execution_graph = execution_graph
        self._replace_recovery_graph(execution_graph)
        placement_transition.commit()
        transaction.enter(RescalePhase.COMMITTED)
        try:
            try:
                self._commit_live_task_topologies(
                    live_handles,
                    operation_id,
                    deadline.step(self._schedule_start_timeout),
                )
                self._commit_retained_target_runtimes(
                    delta.retained,
                    operation_id,
                    deadline.step(self._schedule_start_timeout),
                )
                # Install the recovery fence and reopen checkpoint admission
                # before any participant can resume on the committed routes.
                self._finish_local_rescale_gate(
                    operation_id,
                    committed=True,
                    timeout=deadline.step(self._coordinator_rpc_timeout),
                    target_job_vertex_id=target_id,
                )
                # Arm every source while the direct rescale participants are
                # still paused.  Their next ordered boundary joins one shared
                # checkpoint epoch; releasing the new topology then lets that
                # barrier traverse without a post-commit data window.
                self._request_rescale_stabilization_checkpoint(
                    execution_graph,
                    target_id,
                    deadline.step(self._coordinator_rpc_timeout),
                )
                self._release_committed_rescale(
                    old_graph,
                    new_target,
                    target_id,
                    operation_id,
                    deadline.step(self._schedule_start_timeout),
                )
                transaction.enter(RescalePhase.RELEASED)
            except Exception:
                self._recovery.require_global_recovery(
                    "the committed operator topology could not be fully activated or fenced for recovery"
                )
                try:
                    self._finish_local_rescale_gate(
                        operation_id,
                        committed=True,
                        timeout=deadline.step(self._coordinator_rpc_timeout),
                        target_job_vertex_id=target_id,
                    )
                except Exception:
                    logger.warning("Failed to install the committed rescale recovery fence", exc_info=True)
                raise
        finally:
            if delta.removed:
                self._retire_removed_actors(
                    plan,
                    deadline if compensation_budget is None else Deadline(compensation_budget + deadline.remaining()),
                    placement_transition,
                )

    def _retire_removed_actors(
        self,
        plan: RescalePlan,
        deadline: Deadline,
        placement_transition: PlacementTransition,
    ) -> None:
        try:
            task_terminator.stop_job_vertex(
                plan.old_target,
                plan.old_graph.namespace,
                deadline.step(self._stop_timeout),
                force=False,
                vertices=plan.delta.removed,
                rescale_operation_id=plan.operation_id,
            )
        except Exception:
            logger.warning(
                "Graceful cleanup of retired rescaled actors failed; force-killing the same delta",
                exc_info=True,
            )
            try:
                task_terminator.stop_job_vertex(
                    plan.old_target,
                    plan.old_graph.namespace,
                    deadline.step(self._stop_timeout),
                    force=True,
                    vertices=plan.delta.removed,
                )
            except Exception:
                self._remember_pending_rescale_actors(plan.old_target, plan.delta.removed)
                self._remember_pending_placement_cleanup(placement_transition)
                self._recovery.require_global_recovery(
                    "one or more retired operator actors survived post-rescale cleanup"
                )
                logger.exception("Failed to force-clean retired rescaled operator actors")
                raise
        try:
            placement_transition.release_retired()
        except Exception:
            self._remember_pending_placement_cleanup(placement_transition)
            logger.exception("Failed to release retired actor placement reservations")
            raise

    def _stage_local_rescale_state(
        self,
        plan: RescalePlan,
        snapshots: list[StateSnapshotReference | None],
        timeout: float,
    ) -> None:
        if not plan.old_target.operator_spec.stateful:
            return
        if len(snapshots) != plan.old_target.concurrency or any(snapshot is None for snapshot in snapshots):
            raise RuntimeError("managed-state rescale did not capture every old subtask")
        klein.get(
            self.coordinator.stage_operator_rescale_state(
                plan.operation_id,
                plan.target_id,
                plan.new_target.concurrency,
                tuple(snapshots),
            ),
            timeout=timeout,
        )

    def _capture_retired_progress(self, plan: RescalePlan, timeout: float) -> None:
        """Read scale-in counters at the aligned cut, where they are final."""

        removed = plan.delta.removed
        if not removed:
            return
        try:
            results = klein.get(
                [vertex.stream_task.progress_counts() for vertex in removed],
                timeout=timeout,
            )
        except Exception:
            logger.warning("Unable to capture final counters for retired rescale actors", exc_info=True)
            return
        self._last_rescale_retired_counts = {
            vertex.id: counts
            for vertex, counts in zip(removed, results, strict=True)
            if isinstance(counts, SubtaskCounts)
        }

    def take_retired_rescale_counts(self) -> dict[ExecutionVertexId, SubtaskCounts]:
        """Return and clear final counters from the latest committed scale-in."""

        counts = self._last_rescale_retired_counts
        self._last_rescale_retired_counts = {}
        return counts

    @staticmethod
    def _prepare_retained_target_runtimes(
        execution_graph: ExecutionGraph,
        new_target: ExecutionJobVertex,
        retained: tuple[ExecutionVertex, ...],
        operation_id: str,
        timeout: float,
    ) -> None:
        """Build the new runtime beside each retained actor's paused runtime."""

        calls = [
            vertex.stream_task.prepare_runtime_rescale(
                operation_id,
                task_deployer.build_descriptor(
                    execution_graph,
                    new_target,
                    vertex,
                    restore_operation_id=operation_id,
                ),
            )
            for vertex in retained
        ]
        if calls:
            results = klein.get(calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more retained target runtimes rejected rescale preparation")

    @staticmethod
    def _commit_retained_target_runtimes(
        retained: tuple[ExecutionVertex, ...],
        operation_id: str,
        timeout: float,
    ) -> None:
        calls = [vertex.stream_task.commit_runtime_rescale(operation_id) for vertex in retained]
        if calls:
            results = klein.get(calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more retained target runtimes rejected the rescale commit")

    @staticmethod
    def _rollback_retained_target_runtimes(
        retained: tuple[ExecutionVertex, ...],
        operation_id: str,
        timeout: float,
    ) -> bool:
        calls = [vertex.stream_task.rollback_runtime_rescale(operation_id) for vertex in retained]
        if not calls:
            return True
        try:
            return all(result is True for result in klein.get(calls, timeout=timeout))
        except Exception:
            logger.warning("Failed to roll back retained target runtimes", exc_info=True)
            return False

    def _rollback_local_rescale(
        self,
        plan: RescalePlan,
        deadline: Deadline,
        transaction: RescaleTransaction,
        placement_transition: PlacementTransition,
    ) -> None:
        rollback_safe = self._restore_precommit_topologies(
            plan,
            deadline,
            transaction,
        )
        try:
            rollback_safe = (
                self._restore_precommit_runtime(
                    plan,
                    deadline,
                    transaction,
                    placement_transition,
                )
                and rollback_safe
            )
        finally:
            if rollback_safe:
                if transaction.attempted(RescalePhase.PARTICIPANTS):
                    try:
                        self._release_rescale_participants(
                            plan.old_graph,
                            plan.target_id,
                            plan.operation_id,
                            deadline.step(self._schedule_start_timeout),
                            include_target=True,
                        )
                    except Exception:
                        rollback_safe = False
                        logger.warning("Failed to resume every participant after rescale rollback", exc_info=True)
                if rollback_safe and transaction.attempted(RescalePhase.CHECKPOINT_GATE):
                    try:
                        self._finish_local_rescale_gate(
                            plan.operation_id,
                            committed=False,
                            timeout=deadline.step(self._coordinator_rpc_timeout),
                        )
                    except Exception:
                        rollback_safe = False
                        logger.warning("Failed to release the checkpoint gate after rescale rollback", exc_info=True)
            if not rollback_safe:
                # Do not resume a possibly split topology. Fenced tasks make the
                # health probe fail, and this explicit policy flag guarantees
                # the supervisor skips Tier-0 and performs a global restore.
                self._recovery.require_global_recovery("operator topology rollback was incomplete")
                try:
                    self._finish_local_rescale_gate(
                        plan.operation_id,
                        committed=True,
                        timeout=deadline.step(self._coordinator_rpc_timeout),
                        target_job_vertex_id=plan.target_id,
                    )
                except Exception:
                    logger.warning("Failed to install the rollback recovery fence", exc_info=True)
        if not rollback_safe:
            raise RuntimeError("operator rescale rollback was incomplete; global recovery is required")

    def _restore_precommit_runtime(
        self,
        plan: RescalePlan,
        deadline: Deadline,
        transaction: RescaleTransaction,
        placement_transition: PlacementTransition,
    ) -> bool:
        restored = True
        if transaction.attempted(RescalePhase.RUNTIMES):
            restored = (
                self._rollback_retained_target_runtimes(
                    plan.delta.retained,
                    plan.operation_id,
                    deadline.step(self._schedule_start_timeout),
                )
                and restored
            )
        if transaction.attempted(RescalePhase.CANDIDATES) and plan.delta.added:
            try:
                task_terminator.stop_job_vertex(
                    plan.new_target,
                    plan.new_graph.namespace,
                    deadline.step(self._stop_timeout),
                    force=True,
                    vertices=plan.delta.added,
                )
            except Exception:
                restored = False
                self._remember_pending_rescale_actors(plan.new_target, plan.delta.added)
                logger.warning("Failed to stop replacement actors during rescale rollback", exc_info=True)
        try:
            placement_transition.rollback()
        except Exception:
            restored = False
            self._remember_pending_placement_cleanup(placement_transition)
            logger.warning("Failed to release candidate placement reservations", exc_info=True)
        self.execution_graph = plan.old_graph
        try:
            self._replace_recovery_graph(plan.old_graph)
        except Exception:
            restored = False
            logger.warning("Failed to restore the recovery graph during rescale rollback", exc_info=True)
        try:
            self._discard_local_rescale_state(
                plan.operation_id,
                plan.target_id,
                deadline.step(self._coordinator_rpc_timeout),
            )
        except Exception:
            logger.warning("Failed to discard local state during rescale rollback", exc_info=True)
        return restored

    def _restore_precommit_topologies(
        self,
        plan: RescalePlan,
        deadline: Deadline,
        transaction: RescaleTransaction,
    ) -> bool:
        restored = True
        if transaction.attempted(RescalePhase.COORDINATOR):
            try:
                klein.get(
                    self.coordinator.reconfigure_execution_graph(plan.old_graph),
                    timeout=deadline.step(self._coordinator_rpc_timeout),
                )
            except Exception:
                restored = False
                logger.warning("Failed to restore checkpoint topology during rescale rollback", exc_info=True)
        if transaction.attempted(RescalePhase.ROUTES):
            try:
                restored = (
                    self._rollback_live_task_topologies(
                        plan.new_graph,
                        plan.target_id,
                        plan.operation_id,
                        deadline.step(self._schedule_start_timeout),
                        live_graph=plan.old_graph,
                    )
                    and restored
                )
            except Exception:
                restored = False
                logger.warning("Failed to restore live task routes during rescale rollback", exc_info=True)
        return restored

    def _discard_local_rescale_state(
        self,
        operation_id: str,
        target_id: int,
        timeout: float,
    ) -> None:
        cleanup = (operation_id, target_id)
        try:
            klein.get(
                self.coordinator.discard_operator_rescale_state(operation_id, target_id),
                timeout=timeout,
            )
        except Exception:
            self._pending_rescale_state_cleanup.add(cleanup)
            raise
        self._pending_rescale_state_cleanup.discard(cleanup)

    def _reconcile_pending_rescale_state_cleanup(self, timeout: float) -> None:
        if not self._pending_rescale_state_cleanup:
            return
        deadline = Deadline(timeout)
        for operation_id, target_id in tuple(self._pending_rescale_state_cleanup):
            if deadline.expired():
                break
            try:
                self._discard_local_rescale_state(
                    operation_id,
                    target_id,
                    deadline.step(self._coordinator_rpc_timeout),
                )
            except Exception:
                logger.warning(
                    "Failed to reconcile transient rescale state %s for operator %s",
                    operation_id,
                    target_id,
                    exc_info=True,
                )
        if self._pending_rescale_state_cleanup:
            raise RuntimeError("transient operator-rescale state cleanup remains incomplete")

    def _finish_local_rescale_gate(
        self,
        operation_id: str,
        *,
        committed: bool,
        timeout: float,
        target_job_vertex_id: int | None = None,
    ) -> None:
        retry_deadline = Deadline(timeout)
        last_error: Exception | None = None
        for _attempt in range(3):
            attempt_timeout = retry_deadline.step(self._coordinator_rpc_timeout)
            if attempt_timeout <= 0:
                break
            try:
                released = klein.get(
                    self.coordinator.finish_operator_rescale(
                        operation_id,
                        committed,
                        target_job_vertex_id,
                    ),
                    timeout=attempt_timeout,
                )
                if released is True:
                    return
                last_error = RuntimeError(
                    f"checkpoint coordinator rejected completion of operator rescale {operation_id}"
                )
            except Exception as error:
                last_error = error
        raise RuntimeError(f"failed to release checkpoint gate for operator rescale {operation_id}") from last_error

    def _request_rescale_stabilization_checkpoint(
        self,
        execution_graph: ExecutionGraph,
        target_job_vertex_id: int,
        timeout: float,
    ) -> None:
        """Prompt the job source to checkpoint the committed topology.

        The RPC only sets a source-thread flag; the barrier itself is emitted at
        the next record or idle callback, preserving record-boundary ordering.
        Every source in the component must accept before rescale participants
        resume; a partial arm is recovered globally because that epoch could
        never align.
        """

        sources = self._rescale_stabilization_sources(execution_graph, target_job_vertex_id)
        unavailable = [
            vertex for vertex in sources if vertex.stream_task is None or vertex.status != ExecutionVertexStatus.RUNNING
        ]
        if unavailable:
            raise RuntimeError("one or more sources became unavailable before stabilization was armed")
        calls = [vertex.stream_task.request_checkpoint() for vertex in sources]
        try:
            accepted = klein.get(calls, timeout=timeout)
        except Exception as error:
            raise RuntimeError("failed to arm every source for rescale stabilization") from error
        if not all(result is True for result in accepted):
            raise RuntimeError("one or more sources rejected rescale stabilization")

    @staticmethod
    def _rescale_stabilization_sources(
        execution_graph: ExecutionGraph,
        target_job_vertex_id: int,
    ) -> tuple[ExecutionVertex, ...]:
        domains = execution_graph.checkpoint_domains_for_job_vertex(target_job_vertex_id)
        source_ids = {
            source_id
            for domain in domains
            if isinstance(domain, CheckpointDomain)
            for source_id in domain.source_vertex_ids
        }
        return tuple(source for source in execution_graph.source_execution_vertices if source.id in source_ids)

    def _prepare_local_rescale(
        self,
        graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
    ) -> tuple[list[Any], list[Any]]:
        target = graph.job_vertex(target_id)
        prepare_calls = [
            vertex.stream_task.prepare_rescale_target(operation_id) for vertex in target.execution_vertices.values()
        ]
        downstream_vertices: list[ExecutionVertex] = []
        for downstream_id in graph.downstream_job_vertices(target_id):
            job_vertex = graph.job_vertex(downstream_id)
            for vertex in job_vertex.execution_vertices.values():
                descriptor = task_deployer.build_descriptor(graph, job_vertex, vertex)
                expected = tuple(sender for sender in descriptor.input_vertex_ids if sender.job_vertex_id == target_id)
                prepare_calls.append(vertex.stream_task.prepare_rescale_downstream(operation_id, expected))
                downstream_vertices.append(vertex)
        if prepare_calls:
            klein.get(prepare_calls, timeout=timeout)
        downstream_waits = [
            vertex.stream_task.await_rescale_ready(operation_id, timeout) for vertex in downstream_vertices
        ]

        upstream_waits: list[Any] = []
        input_ids = {edge.source for edge in graph.input_job_edges(target_id)}
        for upstream_id in input_ids:
            job_vertex = graph.job_vertex(upstream_id)
            edge_indices = tuple(
                index for index, edge in enumerate(graph.output_job_edges(upstream_id)) if edge.target == target_id
            )
            upstream_waits.extend(
                vertex.stream_task.prepare_rescale_upstream(
                    operation_id,
                    target_id,
                    edge_indices,
                    timeout,
                )
                for vertex in job_vertex.execution_vertices.values()
            )
        return upstream_waits, downstream_waits

    @staticmethod
    def _await_local_rescale_cut(
        old_target: ExecutionJobVertex,
        participants: tuple[list[Any], list[Any]],
        operation_id: str,
        timeout: float,
    ) -> list[StateSnapshotReference | None]:
        upstream_waits, downstream_waits = participants
        target_waits = [
            vertex.stream_task.await_rescale_ready(operation_id, timeout)
            for vertex in old_target.execution_vertices.values()
        ]
        all_waits = [*upstream_waits, *target_waits, *downstream_waits]
        results = klein.get(all_waits, timeout=timeout) if all_waits else []
        target_start = len(upstream_waits)
        return list(results[target_start : target_start + len(target_waits)])

    def _prepare_live_task_topologies(
        self,
        descriptor_graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
        *,
        live_graph: ExecutionGraph | None = None,
    ) -> list[KleinActorHandle]:
        handles = self._changed_live_task_handles(
            descriptor_graph,
            target_id,
            live_graph=live_graph,
        )
        calls = [
            handle.stream_task.prepare_topology_reconfiguration(
                operation_id,
                descriptor,
                timeout,
            )
            for handle, descriptor in handles
        ]
        if calls:
            results = klein.get(calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more live tasks rejected topology preparation")
        return [vertex.stream_task for vertex, _descriptor in handles]

    def _changed_live_task_handles(
        self,
        descriptor_graph: ExecutionGraph,
        target_id: int,
        *,
        live_graph: ExecutionGraph | None = None,
    ) -> list[tuple[ExecutionVertex, "TaskDeploymentDescriptor"]]:
        live_graph = descriptor_graph if live_graph is None else live_graph
        handles = []
        for job_vertex_id, descriptor_job_vertex in descriptor_graph.job_vertices.items():
            if job_vertex_id == target_id:
                continue
            live_job_vertex = live_graph.job_vertex(job_vertex_id)
            for index, live_vertex in live_job_vertex.execution_vertices.items():
                descriptor_vertex = descriptor_job_vertex.execution_vertex(index)
                descriptor = task_deployer.build_descriptor(
                    descriptor_graph,
                    descriptor_job_vertex,
                    descriptor_vertex,
                )
                current_descriptor = task_deployer.build_descriptor(
                    live_graph,
                    live_job_vertex,
                    live_vertex,
                )
                if descriptor == current_descriptor:
                    continue
                handles.append((live_vertex, descriptor))
        return handles

    @staticmethod
    def _activate_live_task_topologies(
        handles: list[KleinActorHandle],
        operation_id: str,
        timeout: float,
    ) -> None:
        if handles:
            results = klein.get(
                [handle.activate_topology_reconfiguration(operation_id) for handle in handles],
                timeout=timeout,
            )
            if not all(result is True for result in results):
                raise RuntimeError("one or more live tasks rejected topology activation")

    @staticmethod
    def _commit_live_task_topologies(
        handles: list[KleinActorHandle],
        operation_id: str,
        timeout: float,
    ) -> None:
        if handles:
            # Commit is actor-idempotent through StreamTask tombstones.  Do not
            # hide an uncertain acknowledgement: leaving one actor's topology
            # transaction open would reject every later rescale on that actor.
            # The caller fences recovery and escalates this post-commit failure.
            results = klein.get(
                [handle.commit_topology_reconfiguration(operation_id) for handle in handles],
                timeout=timeout,
            )
            if not all(result is True for result in results):
                raise RuntimeError("one or more live tasks rejected topology commit")

    def _rollback_live_task_topologies(
        self,
        descriptor_graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
        *,
        live_graph: ExecutionGraph,
    ) -> bool:
        vertices = [
            vertex
            for vertex, _descriptor in self._changed_live_task_handles(
                descriptor_graph,
                target_id,
                live_graph=live_graph,
            )
        ]
        if not vertices:
            return True
        try:
            results = klein.get(
                [vertex.stream_task.rollback_topology_reconfiguration(operation_id) for vertex in vertices],
                timeout=timeout,
            )
            return all(result is True for result in results)
        except Exception:
            logger.warning("Failed to roll back live task topologies", exc_info=True)
            return False

    @classmethod
    def _release_committed_rescale(
        cls,
        old_graph: ExecutionGraph,
        new_target: ExecutionJobVertex,
        target_id: int,
        operation_id: str,
        timeout: float,
    ) -> None:
        calls = [
            vertex.stream_task.resume_rescale(operation_id)
            for vertex in new_target.execution_vertices.values()
            if vertex.stream_task is not None
        ]
        calls.extend(
            cls._rescale_participant_calls(
                old_graph,
                target_id,
                operation_id,
                include_target=False,
            )
        )
        if calls:
            results = klein.get(calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more rescale participants did not resume")

    @staticmethod
    def _release_rescale_participants(
        graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
        *,
        include_target: bool,
    ) -> None:
        calls = JobMaster._rescale_participant_calls(
            graph,
            target_id,
            operation_id,
            include_target=include_target,
        )
        if calls:
            results = klein.get(calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more rescale participants did not resume")

    @staticmethod
    def _rescale_participant_calls(
        graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        *,
        include_target: bool,
    ) -> list[Any]:
        participant_ids = set(graph.downstream_job_vertices(target_id))
        participant_ids.update(edge.source for edge in graph.input_job_edges(target_id))
        if include_target:
            participant_ids.add(target_id)
        return [
            vertex.stream_task.resume_rescale(operation_id)
            for job_vertex_id in participant_ids
            for vertex in graph.job_vertex(job_vertex_id).execution_vertices.values()
            if vertex.stream_task is not None
        ]

    def _replace_recovery_graph(self, execution_graph: ExecutionGraph) -> None:
        self._recovery = RecoveryManager(
            execution_graph,
            coordinator_provider=lambda: self.coordinator,
            rpc_timeout=self._coordinator_rpc_timeout,
            start_timeout=self._schedule_start_timeout,
        )

    def _begin_rescale_placement(
        self,
        plan: RescalePlan,
        timeout: float,
    ) -> PlacementTransition:
        placement_plan = self.placement_plan
        if placement_plan is None:
            raise RuntimeError("job placement is unavailable")
        try:
            return placement_plan.begin_rescale(
                plan.new_graph,
                added=plan.delta.added,
                removed=plan.delta.removed,
                timeout=timeout,
            )
        except PlacementCleanupError as error:
            self._remember_pending_placement_cleanup(error.plan)
            raise

    def _remember_pending_placement_cleanup(
        self,
        cleanup: PlacementTransition | PlacementPlan,
    ) -> None:
        if not any(pending is cleanup for pending in self._pending_placement_cleanup):
            self._pending_placement_cleanup.append(cleanup)

    def _reconcile_pending_placement_cleanup(self) -> None:
        remaining: list[PlacementTransition | PlacementPlan] = []
        for transition in self._pending_placement_cleanup:
            try:
                transition.reconcile()
            except Exception:
                remaining.append(transition)
                logger.warning("Placement cleanup reconciliation failed", exc_info=True)
        self._pending_placement_cleanup = remaining
        if remaining:
            raise RuntimeError("previous operator rescale still has unreleased placement reservations")

    def _remember_pending_rescale_actors(
        self,
        job_vertex: ExecutionJobVertex,
        vertices: tuple[ExecutionVertex, ...],
    ) -> None:
        for vertex in vertices:
            self._pending_rescale_actor_cleanup[vertex.name] = (job_vertex, vertex)

    def _cleanup_pending_rescale_actors(self, timeout: float) -> None:
        if not self._pending_rescale_actor_cleanup:
            return
        groups: dict[int, tuple[ExecutionJobVertex, list[ExecutionVertex]]] = {}
        for job_vertex, vertex in self._pending_rescale_actor_cleanup.values():
            group = groups.setdefault(id(job_vertex), (job_vertex, []))
            group[1].append(vertex)
        deadline = Deadline(timeout)
        for job_vertex, vertices in groups.values():
            task_terminator.stop_job_vertex(
                job_vertex,
                self.namespace,
                deadline.step(self._stop_timeout),
                force=True,
                vertices=tuple(vertices),
            )
            for vertex in vertices:
                self._pending_rescale_actor_cleanup.pop(vertex.name, None)

    def on_task_status_report(
        self,
        vertex_id: ExecutionVertexId,
        status: ExecutionVertexStatus,
        error_message: str | None = None,
        task_name: str | None = None,
        task_generation: str | None = None,
    ) -> bool:
        """Apply a StreamTask's status report and report whether the job finished.

        The single-writer entry point for the status-report write. The JobManager
        calls it under _scheduler_lock, the same gate as schedule/restart/stop/
        recover, so this update + the all-sinks-finished scan can't interleave
        with any other execution-graph writer. Returns True iff every sink is FINISHED.
        """
        _accepted, all_finished, _resolved_name = self.apply_task_status_report(
            vertex_id,
            status,
            error_message,
            task_name,
            task_generation,
        )
        return all_finished

    def apply_task_status_report(
        self,
        vertex_id: ExecutionVertexId,
        status: ExecutionVertexStatus,
        error_message: str | None = None,
        task_name: str | None = None,
        task_generation: str | None = None,
    ) -> tuple[bool, bool, str | None]:
        """Validate and apply a report against the writer-owned current graph."""

        vertex = self.execution_graph.find_execution_vertex(vertex_id)
        if vertex is None:
            return False, False, None
        if task_name is not None and vertex.name != task_name:
            return False, False, vertex.name
        if task_generation is not None and vertex.task_generation != task_generation:
            return False, False, vertex.name
        vertex.transition_to(status, error_message)
        if status != ExecutionVertexStatus.FINISHED:
            return True, False, vertex.name
        all_finished = all(
            sink.status == ExecutionVertexStatus.FINISHED for sink in self.execution_graph.sink_execution_vertices
        )
        return True, all_finished, vertex.name

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
        resolved placement plan is held for lifecycle and local elasticity.
        """
        task_deployer.validate_vertex_statuses(self.execution_graph)
        self._reconcile_pending_placement_cleanup()
        # Reset any stale reservation from a previous schedule before replacing.
        self._close_placement_plan()

        strategies = self._placement_strategies()
        last_error = None
        for strategy in strategies:
            try:
                plan = task_deployer.place_workers(self.execution_graph, strategy)
                self.placement_plan = plan
                return
            except PlacementCleanupError as error:
                self._remember_pending_placement_cleanup(error.plan)
                raise
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

    def _close_placement_plan(self) -> None:
        """Release all reservations held by the active placement plan."""

        placement_plan = self.placement_plan
        if placement_plan is None:
            return
        try:
            placement_plan.close()
        except Exception as error:
            logger.warning("Failed to release placement reservations: %s", error)
            raise
        else:
            self.placement_plan = None
