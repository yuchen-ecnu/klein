# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.execution_graph.execution_edge import ExecutionEdge
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.partitioning.partitioner import Partitioner


class ExecutionJobEdge:
    """
    An edge that connects two ExecutionJobVertices.
    """

    def __init__(
        self,
        source_job_vertex: ExecutionJobVertex,
        target_job_vertex: ExecutionJobVertex,
        partitioner: Partitioner,
    ) -> None:
        self.source = source_job_vertex.id
        self.target = target_job_vertex.id
        self.partitioner = partitioner
        self.execution_edges = self._create_execution_edges(source_job_vertex, target_job_vertex)

    def _create_execution_edges(
        self,
        source_job_vertex: ExecutionJobVertex,
        target_job_vertex: ExecutionJobVertex,
    ) -> list[ExecutionEdge]:
        execution_edges = []
        for source_index in range(source_job_vertex.concurrency):
            execution_edges.extend(
                ExecutionEdge(
                    source_job_vertex.execution_vertex(source_index),
                    target_job_vertex.execution_vertex(target_index),
                )
                for target_index in self.partitioner.target_tasks(
                    source_job_vertex.concurrency,
                    target_job_vertex.concurrency,
                    source_index,
                )
            )
        return execution_edges

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(source={self.source}, target={self.target}, partitioner={self.partitioner} ...)"
        )
