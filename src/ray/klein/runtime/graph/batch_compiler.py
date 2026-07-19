# SPDX-License-Identifier: Apache-2.0
"""Compile the immutable logical graph into a Ray Data batch pipeline."""

from typing import Any

from ray.data import Dataset

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import BatchRuntimeContext
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec


class BatchCompiler:
    """Compile and execute a :class:`LogicalGraph` with Ray Data."""

    def __init__(self, graph: LogicalGraph) -> None:
        self._graph = graph

    def execute(self) -> Any:
        results = self._compile_and_execute(JobMetricGroup(job_name=self._graph.job_name))
        return results[0] if len(self._graph.sinks) == 1 and len(results) == 1 else results

    def _compile_and_execute(self, job_metric_group: JobMetricGroup) -> list[Any]:
        compiled: dict[VertexId, Dataset] = {}
        materialized: dict[VertexId, Dataset] = {}
        results: list[Any] = []
        for source_id in self._graph.sources:
            function = self._graph.get(source_id).operator.logical_function
            if function is None:
                raise ValueError(f"Source vertex {source_id} has no logical function")
            compiled[source_id] = function.to_batch([])
            self._compile_vertex(source_id, job_metric_group, compiled, materialized, results)
        return results

    def _compile_vertex(
        self,
        vertex_id: VertexId,
        job_metric_group: JobMetricGroup,
        compiled: dict[VertexId, Dataset],
        materialized: dict[VertexId, Dataset],
        results: list[Any],
    ) -> None:
        upstream_ids = self._graph.upstream(vertex_id)
        if not all(upstream_id in compiled for upstream_id in upstream_ids):
            return

        dataset = compiled.get(vertex_id)
        if dataset is None:
            vertex = self._graph.get(vertex_id)
            function = vertex.operator.logical_function
            if function is None:
                raise ValueError(f"Vertex {vertex_id} has no logical function")
            dataset = function.to_batch(
                upstream_ds=[compiled[upstream_id] for upstream_id in upstream_ids],
                runtime_context=self._batch_runtime_context(vertex, job_metric_group),
            )
            compiled[vertex_id] = dataset

        downstream_ids = self._graph.downstream(vertex_id)
        if not downstream_ids:
            results.append(dataset)
        if len(downstream_ids) > 1 and vertex_id not in materialized:
            dataset = dataset.materialize()
            materialized[vertex_id] = dataset
            compiled[vertex_id] = dataset
        for downstream_id in downstream_ids:
            self._compile_vertex(downstream_id, job_metric_group, compiled, materialized, results)

    def _batch_runtime_context(
        self,
        vertex: VertexSpec,
        job_metric_group: JobMetricGroup,
    ) -> RuntimeContext:
        task_metrics = job_metric_group.add_task_group(
            task_id=str(vertex.id),
            task_name=vertex.name,
            subtask_index=-1,
        )
        operator_metrics = task_metrics.add_operator_group(
            operator_id=str(vertex.id),
            operator_name=vertex.name,
        )
        return BatchRuntimeContext(
            vertex.name,
            -1,
            -1,
            operator_metrics,
            self._graph.config,
            vertex.operator.runtime_info,
        )
