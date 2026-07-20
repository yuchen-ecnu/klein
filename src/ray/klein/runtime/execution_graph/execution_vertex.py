# SPDX-License-Identifier: Apache-2.0
import uuid
from typing import ClassVar

from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import TaskMetricGroup
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.resources import Resources


class ExecutionVertex:
    """One physical subtask and its runtime actor state."""

    def __init__(
        self,
        vertex_id: int,
        vertex_name: str,
        vertex_resources: Resources,
        index: int,
        concurrency: int,
        operator_spec: OperatorSpec,
        config: Configuration,
        task_metric_group: TaskMetricGroup,
    ) -> None:
        self._vertex_name = vertex_name
        # ``name`` is the stable Ray actor/routing identity.  Total parallelism
        # is mutable topology metadata, so including it here would leave a
        # retained actor named ``(1/2)`` after a 2 -> 4 rescale.  Keep the
        # topology-aware form in ``display_name`` instead.
        self.name = f"{vertex_name} ({index + 1})"
        self.resources = vertex_resources
        self.id: ExecutionVertexId = ExecutionVertexId(vertex_id, index)
        self.index = index
        self.concurrency = concurrency
        self.operator_spec = operator_spec
        self.config = config
        self.stream_task: KleinActorHandle | None = None
        self.task_metric_group = task_metric_group
        # ExecutionVertexId and the human-readable task name are intentionally
        # stable across a local rescale.  A separate generation token lets the
        # control plane reject a status RPC that arrives from an older actor
        # after the same parallelism (and therefore the same name) is reused.
        self.task_generation = uuid.uuid4().hex
        # Replacement tasks keep the local-cut identity through Ray actor
        # rebuilds until a later global deployment explicitly clears it.
        self.restore_operation_id: str | None = None
        self._status = ExecutionVertexStatus.CREATED
        self._error_message: str | None = None
        # No per-vertex lock: all _status writes are serialized on the JobManager's
        # single _scheduler_lock, so the read-compare-write below can't race.

    _ALLOWED_TRANSITIONS: ClassVar[dict[ExecutionVertexStatus, frozenset[ExecutionVertexStatus]]] = {
        ExecutionVertexStatus.CREATED: frozenset({ExecutionVertexStatus.DEPLOYED, ExecutionVertexStatus.FAILED}),
        ExecutionVertexStatus.DEPLOYED: frozenset(
            {
                ExecutionVertexStatus.RUNNING,
                ExecutionVertexStatus.FAILED,
                # A bounded source can complete while setup_and_run is still
                # returning to the scheduler.
                ExecutionVertexStatus.FINISHED,
                # Graceful stop of a deployed-but-not-yet-running vertex (the
                # scheduler may tear the job down between DEPLOYED and RUNNING).
                ExecutionVertexStatus.CANCELLING,
                # Abrupt teardown (force stop / kill) before it ever ran.
                ExecutionVertexStatus.CANCELLED,
            }
        ),
        ExecutionVertexStatus.RUNNING: frozenset(
            {
                ExecutionVertexStatus.FAILED,
                ExecutionVertexStatus.FINISHED,
                ExecutionVertexStatus.CANCELLING,
                # Abrupt teardown: a force-killed actor goes straight to
                # CANCELLED without the graceful CANCELLING handshake.
                ExecutionVertexStatus.CANCELLED,
            }
        ),
        ExecutionVertexStatus.CANCELLING: frozenset(
            {
                ExecutionVertexStatus.CANCELLED,
                ExecutionVertexStatus.FAILED,
            }
        ),
        ExecutionVertexStatus.CANCELLED: frozenset(),
        ExecutionVertexStatus.FAILED: frozenset(),
        ExecutionVertexStatus.FINISHED: frozenset(),
    }

    def transition_to(
        self,
        status: ExecutionVertexStatus,
        error_message: str | None = None,
    ) -> None:
        if status == self._status:
            return
        allowed = self._ALLOWED_TRANSITIONS.get(self._status, frozenset())
        if status not in allowed:
            raise RuntimeError(f"Invalid execution vertex transition {self._status} → {status} for {self.name}")
        self._status = status
        if error_message is not None:
            self._error_message = error_message
        if status in {
            ExecutionVertexStatus.CREATED,
            ExecutionVertexStatus.DEPLOYED,
            ExecutionVertexStatus.RUNNING,
            ExecutionVertexStatus.FINISHED,
        }:
            self._error_message = None

    def reset(self) -> None:
        """Return the vertex to CREATED so it can be redeployed on a restart.

        A GLOBAL restart re-instantiates the actors but the ExecutionVertex
        objects are reused, so their state machine is still parked at the prior
        terminal status (CANCELLED/FAILED/FINISHED). Without this reset the
        subsequent DEPLOYED/RUNNING transitions are all rejected as invalid.
        """
        self._status = ExecutionVertexStatus.CREATED
        self._error_message = None

    def renew_task_generation(self) -> None:
        """Give the next actor incarnation a unique status-report identity."""

        self.task_generation = uuid.uuid4().hex

    def rebind(
        self,
        *,
        concurrency: int,
        vertex_resources: Resources,
        operator_spec: OperatorSpec,
        config: Configuration,
    ) -> "ExecutionVertex":
        """Clone this live subtask into a resized job-vertex wrapper.

        The wrapper deliberately preserves the immutable actor identity (physical
        vertex id, Ray actor name and generation), actor handle, status and metric
        group.  Only graph-owned attributes that describe the resized logical
        operator are rebound.  Runtime state is copied explicitly so a future
        mutable field cannot accidentally become shared through a shallow clone.
        """

        if isinstance(concurrency, bool) or not isinstance(concurrency, int):
            raise TypeError("concurrency must be an integer")
        if concurrency <= self.index:
            raise ValueError("concurrency must include the retained subtask index")
        if not isinstance(vertex_resources, Resources):
            raise TypeError("vertex_resources must be Resources")
        if not isinstance(operator_spec, OperatorSpec):
            raise TypeError("operator_spec must be OperatorSpec")
        if not isinstance(config, Configuration):
            raise TypeError("config must be Configuration")

        rebound = ExecutionVertex(
            vertex_id=self.id.job_vertex_id,
            vertex_name=self._vertex_name,
            vertex_resources=vertex_resources,
            index=self.index,
            concurrency=concurrency,
            operator_spec=operator_spec,
            config=config,
            task_metric_group=self.task_metric_group,
        )
        rebound.stream_task = self.stream_task
        rebound.task_generation = self.task_generation
        rebound.restore_operation_id = self.restore_operation_id
        rebound._status = self._status
        rebound._error_message = self._error_message
        return rebound

    @property
    def display_name(self) -> str:
        """Human-readable task label for the current topology."""

        return f"{self._vertex_name} ({self.index + 1}/{self.concurrency})"

    @property
    def status(self) -> ExecutionVertexStatus:
        return self._status

    @property
    def error_message(self) -> str | None:
        return self._error_message

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self.display_name!r}, actor_name={self.name!r}, status={self.status.value!r})"
        )

    def __str__(self) -> str:
        return self.display_name
