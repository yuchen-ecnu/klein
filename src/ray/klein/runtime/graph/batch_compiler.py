# SPDX-License-Identifier: Apache-2.0
"""Batch execution compiler.

Extracted from StreamGraph (graph refactor G5): compiling a StreamGraph into a
Ray Data pipeline and executing it is a *consumer* of the graph, not an
intrinsic graph responsibility. Keeping it here lets StreamGraph be a pure data
structure.
"""

from typing import TYPE_CHECKING, Any

from ray.data import Dataset

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import BatchRuntimeContext

if TYPE_CHECKING:
    from ray.klein.api.stream_graph import StreamGraph
    from ray.klein.api.stream_node import StreamNode


class BatchCompiler:
    """Compile + execute a StreamGraph as a Ray Data batch job."""

    def __init__(self, stream_graph: "StreamGraph") -> None:
        self._stream_graph = stream_graph

    def execute(self) -> Any:
        job_metric_group = JobMetricGroup(job_name=self._stream_graph.job_name)
        results = self._compile_and_execute(job_metric_group)
        # single sink: preserve original single result; multiple sinks: list.
        if len(self._stream_graph.sink_nodes) == 1 and len(results) == 1:
            return results[0]
        return results

    def _compile_and_execute(self, job_metric_group: JobMetricGroup) -> list[Any]:
        compiled_datasets: dict[int, Dataset] = {}
        materialized_datasets: dict[int, Dataset] = {}
        results: list[Any] = []
        for source_id in self._stream_graph.source_nodes:
            logical_function = self._stream_graph.nodes[source_id].operator.logical_function
            if logical_function is None:
                raise ValueError(f"Source node {source_id} has no logical function")
            compiled_datasets[source_id] = logical_function.to_batch([])
            self._compile_node(
                source_id,
                job_metric_group,
                compiled_datasets,
                materialized_datasets,
                results,
            )
        return results

    def _compile_node(
        self,
        node_id: int,
        job_metric_group: JobMetricGroup,
        compiled_datasets: dict[int, Dataset],
        materialized_datasets: dict[int, Dataset],
        results: list[Any],
    ) -> None:
        upstream_ids = self._stream_graph.upstream_nodes(node_id)
        if not all(upstream_id in compiled_datasets for upstream_id in upstream_ids):
            return
        dataset = compiled_datasets.get(node_id)
        if dataset is None:
            node = self._stream_graph.nodes[node_id]
            batch_runtime_context = self._batch_runtime_context(node, job_metric_group)
            upstream_datasets = [compiled_datasets[upstream_id] for upstream_id in upstream_ids]
            logical_function = node.operator.logical_function
            if logical_function is None:
                raise ValueError(f"Node {node_id} has no logical function")
            dataset = logical_function.to_batch(
                upstream_ds=upstream_datasets,
                runtime_context=batch_runtime_context,
            )
            compiled_datasets[node_id] = dataset
        downstream_ids = self._stream_graph.downstream_nodes(node_id)
        if not downstream_ids:
            results.append(dataset)
        if len(downstream_ids) > 1 and node_id not in materialized_datasets:
            materialized_dataset = dataset.materialize()
            materialized_datasets[node_id] = materialized_dataset
            compiled_datasets[node_id] = materialized_dataset
        for downstream_id in downstream_ids:
            self._compile_node(
                downstream_id,
                job_metric_group,
                compiled_datasets,
                materialized_datasets,
                results,
            )

    def _batch_runtime_context(self, node: "StreamNode", job_metric_group: JobMetricGroup) -> RuntimeContext:
        task_metric_group = job_metric_group.add_task_group(task_id=str(node.id), task_name=node.name, subtask_index=-1)
        operator_metric_group = task_metric_group.add_operator_group(operator_id=str(node.id), operator_name=node.name)
        return BatchRuntimeContext(
            f"{node.name}[{node.id}]",
            -1,
            -1,
            operator_metric_group,
            self._stream_graph.config,
            node.operator.runtime_info,
        )
