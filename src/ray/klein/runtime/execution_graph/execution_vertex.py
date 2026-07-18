# SPDX-License-Identifier: Apache-2.0
from typing import ClassVar

from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import TaskMetricGroup
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.operator.operator import StreamOperator
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
        operator: StreamOperator,
        config: Configuration,
        task_metric_group: TaskMetricGroup,
    ) -> None:
        self.name = f"{vertex_name} ({index + 1}/{concurrency})"
        self.resources = vertex_resources
        self.id: ExecutionVertexId = ExecutionVertexId(vertex_id, index)
        self.index = index
        self.concurrency = concurrency
        self.operator = operator
        self.config = config
        self.stream_task: KleinActorHandle | None = None
        self.task_metric_group = task_metric_group
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

    @property
    def status(self) -> ExecutionVertexStatus:
        return self._status

    @property
    def error_message(self) -> str | None:
        return self._error_message

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r}, status={self.status.value!r})"

    def __str__(self) -> str:
        return self.name
