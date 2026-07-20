# SPDX-License-Identifier: Apache-2.0

import logging
from dataclasses import dataclass

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


@dataclass(slots=True)
class _LocalRescaleAttempt:
    candidate_created: bool = False
    checkpoint_gate_attempted: bool = False
    participants_prepare_attempted: bool = False
    runtime_prepare_attempted: bool = False
    routes_prepared: bool = False
    coordinator_reconfiguration_attempted: bool = False
    committed: bool = False


@dataclass(frozen=True, slots=True)
class _LocalRescaleDelta:
    """Physical actor sets for one target-parallelism change."""

    retained: tuple[object, ...]
    added: tuple[object, ...]
    removed: tuple[object, ...]

    @classmethod
    def between(cls, old_target, new_target) -> "_LocalRescaleDelta":
        old_indices = set(old_target.execution_vertices)
        new_indices = set(new_target.execution_vertices)
        return cls(
            retained=tuple(new_target.execution_vertex(index) for index in sorted(old_indices & new_indices)),
            added=tuple(new_target.execution_vertex(index) for index in sorted(new_indices - old_indices)),
            removed=tuple(old_target.execution_vertex(index) for index in sorted(old_indices - new_indices)),
        )


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
        # A failed Ray kill must not make a rescale-only vertex unreachable
        # after its candidate/old graph is discarded. Keep the physical wrapper
        # until a later rescale or whole-job stop confirms the name is gone.
        self._pending_rescale_actor_cleanup: dict[str, tuple[object, object]] = {}

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
        self._cleanup_pending_rescale_actors(deadline.step(self._stop_timeout))
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

    def rescale_operator(
        self,
        execution_graph: ExecutionGraph,
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

        timeout = self._schedule_start_timeout
        self._cleanup_pending_rescale_actors(timeout)
        self._recovery.clear_stable_rescale_metadata()
        target_id, old_graph, old_target, new_target = self._validate_local_rescale(execution_graph)
        attempt = _LocalRescaleAttempt()
        try:
            self._apply_local_rescale(
                execution_graph,
                operation_id,
                target_id,
                old_graph,
                old_target,
                new_target,
                timeout,
                attempt,
            )
        except Exception:
            if attempt.committed:
                logger.exception(
                    "Local rescale of operator %s failed after its commit point; retaining the new topology",
                    old_target.name,
                )
                raise
            logger.exception("Local rescale of operator %s failed; restoring the old topology", old_target.name)
            self._rollback_local_rescale(
                execution_graph,
                operation_id,
                target_id,
                old_graph,
                new_target,
                timeout,
                attempt,
            )
            raise

    def _validate_local_rescale(self, execution_graph: ExecutionGraph) -> tuple[int, ExecutionGraph, object, object]:
        changed = [
            vertex_id
            for vertex_id, old in self.execution_graph.job_vertices.items()
            if old.concurrency != execution_graph.job_vertex(vertex_id).concurrency
        ]
        if len(changed) != 1:
            raise ValueError("local rescale must change exactly one operator")
        target_id = changed[0]
        old_graph = self.execution_graph
        old_target = old_graph.job_vertex(target_id)
        new_target = execution_graph.job_vertex(target_id)
        if old_target.operator_spec != new_target.operator_spec:
            raise ValueError("local rescale cannot replace the target operator")
        if old_target.operator_spec.source:
            raise ValueError("source operators cannot be locally rescaled")
        if old_target.operator_spec.transactional_sink:
            raise ValueError("transactional sink operators cannot be locally rescaled")
        if old_target.operator_spec.collecting:
            raise ValueError("collecting sink operators cannot be locally rescaled")
        source_count = len(old_graph.source_execution_vertices)
        if source_count != 1:
            raise ValueError(
                "local rescaling currently requires the job to have exactly one physical source "
                f"task; found {source_count}"
            )
        if self.coordinator is None or not self._coordinator_alive():
            raise RuntimeError("checkpoint coordinator is unavailable")
        return target_id, old_graph, old_target, new_target

    def _apply_local_rescale(
        self,
        execution_graph: ExecutionGraph,
        operation_id: str,
        target_id: int,
        old_graph: ExecutionGraph,
        old_target,
        new_target,
        timeout: float,
        attempt: _LocalRescaleAttempt,
    ) -> None:
        execution_graph.mark_rescale_epoch(target_id, operation_id)
        delta = _LocalRescaleDelta.between(old_target, new_target)
        for vertex in new_target.execution_vertices.values():
            # The committed graph must retain this identity until its first
            # durable checkpoint, including for actor wrappers that are reused.
            vertex.restore_operation_id = operation_id

        # Allocate and confirm only the scale-out delta before inserting the
        # data-plane fence.  Ray actor creation is asynchronous, so ping is the
        # point at which resource placement and construction are known to have
        # completed.  Stateful setup still waits for the local-cut snapshots.
        if delta.added:
            attempt.candidate_created = True
            task_deployer.instantiate_job_vertex(
                execution_graph,
                new_target,
                NativeStrategy().plan(execution_graph),
                restore_operation_id=operation_id,
                vertices=delta.added,
            )
            task_deployer.deploy_job_vertex(new_target, vertices=delta.added)
            task_deployer.wait_job_vertex_created(new_target, timeout, vertices=delta.added)

        attempt.checkpoint_gate_attempted = True
        klein.get(self.coordinator.begin_operator_rescale(operation_id, timeout), timeout=timeout)
        if not self._recovery.clear_stable_rescale_metadata():
            raise RuntimeError("could not retire the previous durable rescale identity")
        attempt.participants_prepare_attempted = True
        participants = self._prepare_local_rescale(old_graph, target_id, operation_id, timeout)
        snapshots = self._await_local_rescale_cut(old_target, participants, operation_id, timeout)
        self._stage_local_rescale_state(old_target, snapshots, operation_id, target_id, timeout)
        if delta.added:
            task_deployer.start_job_vertex(
                new_target,
                timeout,
                paused_operation_id=operation_id,
                vertices=delta.added,
            )
        attempt.runtime_prepare_attempted = True
        self._prepare_retained_target_runtimes(
            execution_graph,
            new_target,
            delta.retained,
            operation_id,
            timeout,
        )
        # From prepare through coordinator activation every task remains fenced,
        # and old EdgeOutput objects remain available for an exact rollback.
        attempt.routes_prepared = True
        live_handles = self._prepare_live_task_topologies(
            execution_graph,
            target_id,
            operation_id,
            timeout,
            live_graph=old_graph,
        )
        self._activate_live_task_topologies(live_handles, operation_id, timeout)
        attempt.coordinator_reconfiguration_attempted = True
        klein.get(self.coordinator.reconfigure_execution_graph(execution_graph), timeout=timeout)

        # The first retained-journal commit is irreversible. Every operation
        # below is idempotent/best-effort and rolls forward on the new graph.
        attempt.committed = True
        try:
            try:
                self._commit_live_task_topologies(live_handles, operation_id, timeout)
                self.execution_graph = execution_graph
                self._replace_recovery_graph(execution_graph)
                self._commit_retained_target_runtimes(delta.retained, operation_id, timeout)
                # Install the recovery fence and reopen checkpoint admission
                # before any participant can resume on the committed routes.
                self._finish_local_rescale_gate(operation_id, committed=True)
                self._release_committed_rescale(
                    old_graph,
                    new_target,
                    target_id,
                    operation_id,
                    timeout,
                )
            except Exception:
                self._recovery.require_global_recovery(
                    "the committed operator topology could not be fully activated or fenced for recovery"
                )
                try:
                    self._finish_local_rescale_gate(operation_id, committed=True)
                except Exception:
                    logger.warning("Failed to install the committed rescale recovery fence", exc_info=True)
                raise
            self._request_rescale_stabilization_checkpoint(execution_graph)
        finally:
            if delta.removed:
                try:
                    task_terminator.stop_job_vertex(
                        old_target,
                        old_graph.namespace,
                        timeout,
                        force=False,
                        vertices=delta.removed,
                    )
                except Exception:
                    logger.warning(
                        "Graceful cleanup of retired rescaled actors failed; force-killing the same delta",
                        exc_info=True,
                    )
                    try:
                        task_terminator.stop_job_vertex(
                            old_target,
                            old_graph.namespace,
                            timeout,
                            force=True,
                            vertices=delta.removed,
                        )
                    except Exception:
                        self._remember_pending_rescale_actors(old_target, delta.removed)
                        self._recovery.require_global_recovery(
                            "one or more retired operator actors survived post-rescale cleanup"
                        )
                        logger.exception("Failed to force-clean retired rescaled operator actors")
                        raise

    def _stage_local_rescale_state(
        self,
        old_target,
        snapshots: list,
        operation_id: str,
        target_id: int,
        timeout: float,
    ) -> None:
        if not old_target.operator_spec.stateful:
            return
        if len(snapshots) != old_target.concurrency or any(snapshot is None for snapshot in snapshots):
            raise RuntimeError("managed-state rescale did not capture every old subtask")
        klein.get(
            self.coordinator.stage_operator_rescale_state(operation_id, target_id, tuple(snapshots)),
            timeout=timeout,
        )

    @staticmethod
    def _prepare_retained_target_runtimes(
        execution_graph: ExecutionGraph,
        new_target,
        retained: tuple[object, ...],
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
        retained: tuple[object, ...],
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
        retained: tuple[object, ...],
        operation_id: str,
        timeout: float,
    ) -> bool:
        restored = True
        for vertex in retained:
            try:
                result = klein.get(
                    vertex.stream_task.rollback_runtime_rescale(operation_id),
                    timeout=timeout,
                )
                if result is not True:
                    restored = False
            except Exception:
                restored = False
                logger.warning("Failed to roll back one retained target runtime", exc_info=True)
        return restored

    def _rollback_local_rescale(
        self,
        execution_graph: ExecutionGraph,
        operation_id: str,
        target_id: int,
        old_graph: ExecutionGraph,
        new_target,
        timeout: float,
        attempt: _LocalRescaleAttempt,
    ) -> None:
        rollback_safe = self._restore_precommit_topologies(
            execution_graph,
            operation_id,
            target_id,
            old_graph,
            timeout,
            attempt,
        )
        try:
            rollback_safe = (
                self._restore_precommit_runtime(
                    execution_graph,
                    operation_id,
                    target_id,
                    old_graph,
                    new_target,
                    timeout,
                    attempt,
                )
                and rollback_safe
            )
        finally:
            if rollback_safe:
                if attempt.participants_prepare_attempted:
                    try:
                        self._release_rescale_participants(
                            old_graph,
                            target_id,
                            operation_id,
                            timeout,
                            include_target=True,
                        )
                    except Exception:
                        rollback_safe = False
                        logger.warning("Failed to resume every participant after rescale rollback", exc_info=True)
                if rollback_safe and attempt.checkpoint_gate_attempted:
                    try:
                        self._finish_local_rescale_gate(operation_id, committed=False)
                    except Exception:
                        rollback_safe = False
                        logger.warning("Failed to release the checkpoint gate after rescale rollback", exc_info=True)
            if not rollback_safe:
                # Do not resume a possibly split topology. Fenced tasks make the
                # health probe fail, and this explicit policy flag guarantees
                # the supervisor skips Tier-0 and performs a global restore.
                self._recovery.require_global_recovery("operator topology rollback was incomplete")
                try:
                    self._finish_local_rescale_gate(operation_id, committed=True)
                except Exception:
                    logger.warning("Failed to install the rollback recovery fence", exc_info=True)
        if not rollback_safe:
            raise RuntimeError("operator rescale rollback was incomplete; global recovery is required")

    def _restore_precommit_runtime(
        self,
        execution_graph: ExecutionGraph,
        operation_id: str,
        target_id: int,
        old_graph: ExecutionGraph,
        new_target,
        timeout: float,
        attempt: _LocalRescaleAttempt,
    ) -> bool:
        restored = True
        delta = _LocalRescaleDelta.between(old_graph.job_vertex(target_id), new_target)
        if attempt.runtime_prepare_attempted:
            restored = (
                self._rollback_retained_target_runtimes(
                    delta.retained,
                    operation_id,
                    timeout,
                )
                and restored
            )
        if attempt.candidate_created:
            try:
                task_terminator.stop_job_vertex(
                    new_target,
                    execution_graph.namespace,
                    timeout,
                    force=True,
                    vertices=delta.added,
                )
            except Exception:
                restored = False
                self._remember_pending_rescale_actors(new_target, delta.added)
                logger.warning("Failed to stop replacement actors during rescale rollback", exc_info=True)
        self.execution_graph = old_graph
        try:
            self._replace_recovery_graph(old_graph)
        except Exception:
            restored = False
            logger.warning("Failed to restore the recovery graph during rescale rollback", exc_info=True)
        try:
            self._discard_local_rescale_state(operation_id, target_id)
        except Exception:
            logger.warning("Failed to discard local state during rescale rollback", exc_info=True)
        return restored

    def _restore_precommit_topologies(
        self,
        execution_graph: ExecutionGraph,
        operation_id: str,
        target_id: int,
        old_graph: ExecutionGraph,
        timeout: float,
        attempt: _LocalRescaleAttempt,
    ) -> bool:
        restored = True
        if attempt.coordinator_reconfiguration_attempted:
            try:
                klein.get(self.coordinator.reconfigure_execution_graph(old_graph), timeout=timeout)
            except Exception:
                restored = False
                logger.warning("Failed to restore checkpoint topology during rescale rollback", exc_info=True)
        if attempt.routes_prepared:
            try:
                restored = (
                    self._rollback_live_task_topologies(
                        execution_graph,
                        target_id,
                        operation_id,
                        timeout,
                        live_graph=old_graph,
                    )
                    and restored
                )
            except Exception:
                restored = False
                logger.warning("Failed to restore live task routes during rescale rollback", exc_info=True)
        return restored

    def _discard_local_rescale_state(self, operation_id: str, target_id: int) -> None:
        try:
            klein.get(
                self.coordinator.discard_operator_rescale_state(operation_id, target_id),
                timeout=self._coordinator_rpc_timeout,
            )
        except Exception:
            logger.warning("Failed to discard transient rescale state", exc_info=True)

    def _finish_local_rescale_gate(self, operation_id: str, *, committed: bool) -> None:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                released = klein.get(
                    self.coordinator.finish_operator_rescale(operation_id, committed),
                    timeout=self._coordinator_rpc_timeout,
                )
                if released is True:
                    return
                last_error = RuntimeError(
                    f"checkpoint coordinator rejected completion of operator rescale {operation_id}"
                )
            except Exception as error:
                last_error = error
            if attempt == 2:
                raise RuntimeError(
                    f"failed to release checkpoint gate for operator rescale {operation_id}"
                ) from last_error

    def _request_rescale_stabilization_checkpoint(self, execution_graph: ExecutionGraph) -> None:
        """Prompt the job source to checkpoint the committed topology.

        The RPC only sets a source-thread flag; the barrier itself is emitted at
        the next record or idle callback, preserving record-boundary ordering.
        Failure is non-fatal because the topology is already committed and the
        coordinator's recovery fence remains active until a later checkpoint.
        """

        calls = [
            vertex.stream_task.request_checkpoint()
            for vertex in execution_graph.source_execution_vertices
            if vertex.stream_task is not None and vertex.status == ExecutionVertexStatus.RUNNING
        ]
        if not calls:
            return
        try:
            klein.get(calls, timeout=self._coordinator_rpc_timeout)
        except Exception:
            logger.warning("Failed to request the post-rescale stabilization checkpoint", exc_info=True)

    def _prepare_local_rescale(
        self,
        graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
    ) -> tuple[list, list]:
        target = graph.job_vertex(target_id)
        for vertex in target.execution_vertices.values():
            klein.get(vertex.stream_task.prepare_rescale_target(operation_id), timeout=self._coordinator_rpc_timeout)

        downstream_waits = []
        for downstream_id in graph.downstream_job_vertices(target_id):
            job_vertex = graph.job_vertex(downstream_id)
            for vertex in job_vertex.execution_vertices.values():
                descriptor = task_deployer.build_descriptor(graph, job_vertex, vertex)
                expected = tuple(sender for sender in descriptor.input_vertex_ids if sender.job_vertex_id == target_id)
                klein.get(
                    vertex.stream_task.prepare_rescale_downstream(operation_id, expected),
                    timeout=self._coordinator_rpc_timeout,
                )
                downstream_waits.append(vertex.stream_task.await_rescale_ready(operation_id, timeout))

        upstream_waits = []
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
        klein.get(upstream_waits, timeout=timeout)
        return upstream_waits, downstream_waits

    @staticmethod
    def _await_local_rescale_cut(
        old_target,
        participants: tuple[list, list],
        operation_id: str,
        timeout: float,
    ) -> list:
        _upstream_waits, downstream_waits = participants
        target_waits = [
            vertex.stream_task.await_rescale_ready(operation_id, timeout)
            for vertex in old_target.execution_vertices.values()
        ]
        snapshots = klein.get(target_waits, timeout=timeout)
        if downstream_waits:
            klein.get(downstream_waits, timeout=timeout)
        return list(snapshots)

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
            klein.get(calls, timeout=timeout)
        return [vertex.stream_task for vertex, _descriptor in handles]

    def _changed_live_task_handles(
        self,
        descriptor_graph: ExecutionGraph,
        target_id: int,
        *,
        live_graph: ExecutionGraph | None = None,
    ) -> list[tuple[object, object]]:
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
            klein.get(
                [handle.activate_topology_reconfiguration(operation_id) for handle in handles],
                timeout=timeout,
            )

    @staticmethod
    def _commit_live_task_topologies(
        handles: list[KleinActorHandle],
        operation_id: str,
        timeout: float,
    ) -> None:
        for handle in handles:
            try:
                klein.get(
                    handle.commit_topology_reconfiguration(operation_id),
                    timeout=timeout,
                )
            except Exception:
                # The route is already active. A lost actor is rebuilt from the
                # new ExecutionGraph; a lost response is safe to retry later.
                logger.warning("Failed to confirm a committed task topology", exc_info=True)

    def _rollback_live_task_topologies(
        self,
        descriptor_graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
        *,
        live_graph: ExecutionGraph,
    ) -> bool:
        restored = True
        for vertex, _descriptor in self._changed_live_task_handles(
            descriptor_graph,
            target_id,
            live_graph=live_graph,
        ):
            try:
                result = klein.get(
                    vertex.stream_task.rollback_topology_reconfiguration(operation_id),
                    timeout=timeout,
                )
                if result is False:
                    restored = False
            except Exception:
                restored = False
                logger.warning("Failed to roll back one live task topology", exc_info=True)
        return restored

    @classmethod
    def _release_committed_rescale(
        cls,
        old_graph: ExecutionGraph,
        new_target,
        target_id: int,
        operation_id: str,
        timeout: float,
    ) -> None:
        candidate_calls = [
            vertex.stream_task.resume_rescale(operation_id)
            for vertex in new_target.execution_vertices.values()
            if vertex.stream_task is not None
        ]
        if candidate_calls:
            results = klein.get(candidate_calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more resized target tasks did not resume")
        cls._release_rescale_participants(
            old_graph,
            target_id,
            operation_id,
            timeout,
            include_target=False,
        )

    @staticmethod
    def _release_rescale_participants(
        graph: ExecutionGraph,
        target_id: int,
        operation_id: str,
        timeout: float,
        *,
        include_target: bool,
    ) -> None:
        participant_ids = set(graph.downstream_job_vertices(target_id))
        participant_ids.update(edge.source for edge in graph.input_job_edges(target_id))
        if include_target:
            participant_ids.add(target_id)
        calls = [
            vertex.stream_task.resume_rescale(operation_id)
            for job_vertex_id in participant_ids
            for vertex in graph.job_vertex(job_vertex_id).execution_vertices.values()
            if vertex.stream_task is not None
        ]
        if calls:
            results = klein.get(calls, timeout=timeout)
            if not all(result is True for result in results):
                raise RuntimeError("one or more rescale participants did not resume")

    def _replace_recovery_graph(self, execution_graph: ExecutionGraph) -> None:
        self._recovery = RecoveryManager(
            execution_graph,
            coordinator_provider=lambda: self.coordinator,
            rpc_timeout=self._coordinator_rpc_timeout,
            start_timeout=self._schedule_start_timeout,
        )

    def _remember_pending_rescale_actors(self, job_vertex, vertices: tuple[object, ...]) -> None:
        for vertex in vertices:
            self._pending_rescale_actor_cleanup[vertex.name] = (job_vertex, vertex)

    def _cleanup_pending_rescale_actors(self, timeout: float) -> None:
        if not self._pending_rescale_actor_cleanup:
            return
        groups: dict[int, tuple[object, list[object]]] = {}
        for job_vertex, vertex in self._pending_rescale_actor_cleanup.values():
            group = groups.setdefault(id(job_vertex), (job_vertex, []))
            group[1].append(vertex)
        for job_vertex, vertices in groups.values():
            task_terminator.stop_job_vertex(
                job_vertex,
                self.namespace,
                timeout,
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
