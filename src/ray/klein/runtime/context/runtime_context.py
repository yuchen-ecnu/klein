# SPDX-License-Identifier: Apache-2.0
"""Runtime context implementations for streaming and batch execution."""

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.stream_runtime_context import StreamRuntimeContext
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import MetricGroup, TaskMetricGroup
from ray.klein.runtime.coordinator.checkpoint_strategy import CheckpointStrategy


class _BaseRuntimeContext(RuntimeContext):
    """Shared storage for batch and streaming task contexts."""

    def __init__(
        self,
        task_name: str,
        task_index: int,
        parallelism: int,
        config: Configuration,
        metric_group: MetricGroup,
        runtime_info: RuntimeInfo,
        job_id: str = "default",
        state_backend_task_name: str | None = None,
    ) -> None:
        self._task_name = task_name
        self._task_index = task_index
        self._parallelism = parallelism
        self._config = config
        self._metric_group = metric_group
        self._runtime_info = runtime_info
        self._job_id = job_id
        # Public task identity stays stable while a retained actor prepares a
        # second runtime during rescaling. Managed local state needs a distinct
        # filesystem identity so opening the pending RocksDB backend cannot reset
        # the still-live backend owned by the old runtime.
        self._state_backend_task_name = state_backend_task_name or task_name

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def task_index(self) -> int:
        return self._task_index

    @property
    def parallelism(self) -> int:
        return self._parallelism

    @property
    def config(self) -> Configuration:
        return self._config

    @property
    def metric_group(self) -> MetricGroup:
        return self._metric_group

    @property
    def runtime_info(self) -> RuntimeInfo:
        return self._runtime_info

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def state_backend_task_name(self) -> str:
        """Internal local-backend identity, independent of the public task name."""

        return self._state_backend_task_name


class TaskRuntimeContext(_BaseRuntimeContext, StreamRuntimeContext):
    """Streaming task context with checkpoint support."""

    def __init__(
        self,
        task_name: str,
        task_index: int,
        parallelism: int,
        config: Configuration,
        metric_group: MetricGroup,
        checkpoint_strategy: CheckpointStrategy,
        runtime_info: RuntimeInfo,
        job_id: str = "default",
        state_backend_task_name: str | None = None,
    ) -> None:
        super().__init__(
            task_name,
            task_index,
            parallelism,
            config,
            metric_group,
            runtime_info,
            job_id,
            state_backend_task_name,
        )
        self._checkpoint_strategy = checkpoint_strategy

    @property
    def checkpoint_strategy(self) -> CheckpointStrategy:
        return self._checkpoint_strategy


class BatchRuntimeContext(_BaseRuntimeContext):
    """Batch task context without checkpoint capabilities."""

    def __init__(
        self,
        task_name: str,
        task_index: int,
        parallelism: int,
        metric_group: MetricGroup,
        config: Configuration,
        runtime_info: RuntimeInfo,
    ) -> None:
        super().__init__(task_name, task_index, parallelism, config, metric_group, runtime_info)


class OperatorRuntimeContext(TaskRuntimeContext):
    """Streaming task context scoped to one operator metric group."""

    def __init__(self, task_runtime_context: TaskRuntimeContext, operator_id: int, operator_name: str) -> None:
        task_metric_group = task_runtime_context.metric_group
        if not isinstance(task_metric_group, TaskMetricGroup):
            raise TypeError("TaskRuntimeContext must hold a TaskMetricGroup")
        super().__init__(
            task_runtime_context.task_name,
            task_runtime_context.task_index,
            task_runtime_context.parallelism,
            task_runtime_context.config,
            task_metric_group.add_operator_group(str(operator_id), operator_name),
            task_runtime_context.checkpoint_strategy,
            task_runtime_context.runtime_info,
            task_runtime_context.job_id,
            task_runtime_context.state_backend_task_name,
        )
