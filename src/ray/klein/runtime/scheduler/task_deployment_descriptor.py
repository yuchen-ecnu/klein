# SPDX-License-Identifier: Apache-2.0
"""Picklable configuration for creating a self-contained StreamTask actor."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import TaskMetricGroup
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.partitioning.partitioner import Partitioner


@dataclass(frozen=True, slots=True)
class OutputEdgeDescriptor:
    """Routing and buffering settings for one outbound task edge."""

    target_task_names: tuple[str, ...]
    partitioner: Partitioner
    output_buffer_size: int
    put_timeout: int


@dataclass(frozen=True, slots=True)
class TaskDeploymentDescriptor:
    """Everything a StreamTask actor needs to rebuild its runtime state."""

    operator: OperatorSpec
    vertex_id: ExecutionVertexId
    task_name: str
    task_index: int
    parallelism: int
    config: Configuration
    metric_group: TaskMetricGroup
    operator_type: OperatorType
    barrier_split: Mapping[ExecutionVertexId, int]
    is_committer: bool
    out_edges: tuple[OutputEdgeDescriptor, ...]
    input_buffer_size: int
    namespace: str
    output_queue: Any | None = None
    input_vertex_ids: tuple[ExecutionVertexId, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "barrier_split", FrozenMapping(self.barrier_split))
        object.__setattr__(self, "out_edges", tuple(self.out_edges))
        object.__setattr__(self, "input_vertex_ids", tuple(self.input_vertex_ids))
