# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING

from ray.util.queue import Queue

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.actor import create_remote_actor
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.resources import Resources
from ray.klein.runtime.worker.source_stream_task import SourceStreamTask
from ray.klein.runtime.worker.stream_task import StreamTask

if TYPE_CHECKING:
    from ray.klein.runtime.graph.vertex_spec import VertexSpec
    from ray.klein.runtime.scheduler.placement import PlacementPlan
    from ray.klein.runtime.scheduler.task_deployment_descriptor import TaskDeploymentDescriptor


logger = get_logger(__name__)


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
    def operator(self) -> StreamOperator:
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
                self.operator,
                self.config,
                task_metric_group,
            )
        return vertices

    def instantiate(
        self,
        graph,
        plan: "PlacementPlan | None" = None,
    ) -> None:
        self._assign_output_queue()

        # Per-job Ray namespace — picked up from the graph (set by
        # JobManager.submit). Used two ways below: (1) the StreamTask is
        # *registered* in this namespace so a sibling job's identically-named
        # vertex can't collide; (2) it's baked into the descriptor so the
        # StreamTask itself (post-restart, in some other worker process whose
        # default namespace would otherwise be Ray's job-wide default) can
        # ``klein.get_actor_by_name(..., namespace=...)`` for its downstreams /
        # JobManager / Coordinator and reach the right ones.
        namespace = graph.namespace

        for index, vertex in self.execution_vertices.items():
            # On a GLOBAL restart the vertex objects are reused, so clear any
            # prior terminal status before redeploying the fresh actor.
            vertex.reset()
            descriptor = self._build_descriptor(graph, vertex)
            ray_remote_args = {
                "name": vertex.name,
                "num_cpus": vertex.resources.cpus,
                "num_gpus": vertex.resources.gpus,
                "max_restarts": -1,
            }
            ray_remote_args["namespace"] = namespace

            # Placement precedence: placement group, node pin, then Ray native.
            pg = None
            bundle_index = -1
            schedule_node_id = None
            if plan is not None and plan.uses_placement_group:
                pg = plan.placement_group
                bundle_index = plan.bundle_for(vertex.id)
            elif plan is not None and plan.node_by_vertex:
                schedule_node_id = plan.node_for(vertex.id)
            vertex.stream_task = create_remote_actor(
                self._task_class_for(vertex),
                construct_args={"descriptor": descriptor},
                ray_remote_args=ray_remote_args,
                schedule_node_id=schedule_node_id,
                placement_group=pg,
                placement_group_bundle_index=bundle_index,
            )
            logger.debug(
                "Created execution vertex %s[%s] with placement %s",
                vertex.name,
                index,
                f"pg[bundle={bundle_index}]" if pg is not None else f"node={schedule_node_id}",
            )

    def _build_descriptor(self, graph, vertex: ExecutionVertex) -> "TaskDeploymentDescriptor":
        """Build the self-contained deployment descriptor for one execution vertex.

        Captures everything the actor needs to (re)build its runtime state on a
        Ray restart: snapshot-strategy inputs and the outbound topology (by
        downstream actor NAME, so handles are re-resolved after restart).
        """
        from ray.klein.config.pipeline_options import PipelineOptions
        from ray.klein.runtime.scheduler.task_deployment_descriptor import (
            OutputEdgeDescriptor,
            TaskDeploymentDescriptor,
        )

        # Cached on the graph: one traversal shared across all subtasks' descriptors.
        barrier_splits = graph.barrier_splits
        is_committer = self.id in graph.sink_job_vertices
        output_buffer_size = self.config.get(PipelineOptions.OUTPUT_BUFFER_SIZE)
        put_timeout = int(self.config.get(PipelineOptions.INPUT_BUFFER_PUT_TIMEOUT).total_seconds())
        input_buffer_size = self.config.get(PipelineOptions.INPUT_BUFFER_SIZE)

        out_edges = []
        for output_edge in graph.output_job_edges(self.id):
            target_job_vertex = graph.job_vertex(output_edge.target)
            target_vertices = tuple(target_job_vertex.execution_vertices.values())
            if not target_vertices:
                continue
            out_edges.append(
                OutputEdgeDescriptor(
                    target_task_names=tuple(target.name for target in target_vertices),
                    partitioner=output_edge.partitioner,
                    output_buffer_size=output_buffer_size,
                    put_timeout=put_timeout,
                )
            )

        input_vertex_ids = []
        for input_edge in graph.input_job_edges(self.id):
            input_vertex_ids.extend(
                edge.source.id for edge in input_edge.execution_edges if edge.target.id == vertex.id
            )

        return TaskDeploymentDescriptor(
            operator=self.operator,
            vertex_id=vertex.id,
            task_name=vertex.name,
            task_index=vertex.index,
            parallelism=vertex.concurrency,
            config=self.config,
            metric_group=vertex.task_metric_group,
            operator_type=self.operator.operator_type,
            barrier_split=barrier_splits[vertex.id],
            is_committer=is_committer,
            out_edges=tuple(out_edges),
            input_buffer_size=input_buffer_size,
            output_queue=self.output_queue,
            namespace=graph.namespace,
            input_vertex_ids=tuple(input_vertex_ids),
        )

    def cancel_all_tasks(self) -> None:
        """Cancel every task associated with this job vertex."""
        for vertex in self.execution_vertices.values():
            if vertex.stream_task is not None:
                klein.kill(vertex.stream_task)
                vertex.stream_task = None
        logger.info(
            "Cancelled all stream tasks for execution job vertex %s",
            self.name,
        )

    @staticmethod
    def _task_class_for(vertex: ExecutionVertex) -> type[StreamTask]:
        if vertex.operator.operator_type == OperatorType.SOURCE:
            return SourceStreamTask
        return StreamTask

    def _assign_output_queue(self) -> None:
        # A CollectOperator subtask needs a shared output queue (the JobClient
        # drains it). The queue is a legitimately-shared runtime resource for the
        # vertex, created once here and injected per-subtask at build() time via
        # the descriptor — the OperatorSpec itself stays immutable.
        if self.operator.collecting:
            self.output_queue = Queue()

    def execution_vertex(self, subtask_index: int) -> ExecutionVertex:
        return self.execution_vertices[subtask_index]

    def __str__(self) -> str:
        return self.name
