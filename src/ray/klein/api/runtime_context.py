# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import MetricGroup

if TYPE_CHECKING:
    from ray.klein.api.runtime_info import RuntimeInfo


class RuntimeContext(ABC):
    """Read-only execution context handed to a user function's ``open()``.

    Common capabilities are available in both stream and batch execution;
    stream-only checkpoint capabilities live on :class:`StreamRuntimeContext`.
    """

    @property
    @abstractmethod
    def task_name(self) -> str:
        """Task name of the parallel task."""

    @property
    @abstractmethod
    def task_index(self) -> int:
        """Index of this parallel subtask (0 .. parallelism-1)."""

    @property
    @abstractmethod
    def parallelism(self) -> int:
        """The parallelism with which the parallel task runs."""

    @property
    @abstractmethod
    def config(self) -> Configuration:
        """The config with which the parallel task runs."""

    @property
    @abstractmethod
    def metric_group(self) -> MetricGroup:
        """The current metric group."""

    @property
    @abstractmethod
    def runtime_info(self) -> "RuntimeInfo":
        """Runtime info (batching/async) for this task."""

    @property
    @abstractmethod
    def job_id(self) -> str:
        """Stable identifier used to scope state and observability."""
