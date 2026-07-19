# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING

from ray.util.queue import Queue

from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.runtime.graph.vertex_spec import VertexSpec


class ExecutionJobVertex:
    """A logical operator expanded into its physical execution subtasks."""

    def __init__(
        self,
        spec: "VertexSpec",
        config: Configuration,
        job_metric_group: JobMetricGroup,
    ) -> None:
        self.spec = spec
        self.config = config
        self.job_metric_group = job_metric_group
        self.output_queue: Queue | None = None
        self.execution_vertices = self._create_execution_vertices()

    @property
    def id(self) -> int:
        return self.spec.id.index

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def resources(self) -> Resources:
        return self.spec.resources

    @property
    def concurrency(self) -> int:
        return self.spec.concurrency

    @property
    def operator_spec(self) -> OperatorSpec:
        return self.spec.operator

    def _create_execution_vertices(self) -> dict[int, ExecutionVertex]:
        vertices = {}
        for index in range(self.concurrency):
            task_metric_group = self.job_metric_group.add_task_group(
                task_id=f"{self.id}:{index}", task_name=self.name, subtask_index=index
            )
            vertices[index] = ExecutionVertex(
                self.id,
                self.name,
                self.resources,
                index,
                self.concurrency,
                self.operator_spec,
                self.config,
                task_metric_group,
            )
        return vertices

    def execution_vertex(self, subtask_index: int) -> ExecutionVertex:
        return self.execution_vertices[subtask_index]

    def __str__(self) -> str:
        return self.name
