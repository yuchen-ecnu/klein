# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.backend.collection_source import CollectionSource
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.runtime.execution_graph.checkpoint_domain import CheckpointDomain
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer


def _parallel_forward_graph(parallelism: int = 2) -> tuple[LogicalGraph, ExecutionGraph]:
    config = Configuration(include_environment=False)
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    context = KleinContext(config)
    (
        context.source(
            CollectionSource,
            fn_constructor_args=[[{"value": 1}]],
            concurrency=parallelism,
            bounded=True,
            name="Source",
        )
        .map(lambda value: value, concurrency=parallelism, name="Map")
        .write(ConsoleSinkFunction, concurrency=parallelism, name="Sink")
    )
    logical = LogicalGraph.from_sinks(context.sinks, "checkpoint-domains", config)
    optimized = LogicalOptimizer(config).optimize(logical)
    physical = ExecutionGraph.expand(
        optimized,
        config,
        JobMetricGroup("checkpoint-domains"),
        "checkpoint-domains",
    )
    return optimized, physical


def test_checkpoint_domain_validates_membership() -> None:
    member = ExecutionVertexId(1, 0)
    outside = ExecutionVertexId(2, 0)

    with pytest.raises(ValueError, match="sources must be domain members"):
        CheckpointDomain("domain", (member,), (outside,), ())


def test_checkpoint_domains_are_physical_weak_components() -> None:
    _logical, graph = _parallel_forward_graph()

    assert [domain.vertex_ids for domain in graph.checkpoint_domains] == [
        (ExecutionVertexId(1, 0), ExecutionVertexId(2, 0), ExecutionVertexId(3, 0)),
        (ExecutionVertexId(1, 1), ExecutionVertexId(2, 1), ExecutionVertexId(3, 1)),
    ]
    assert [domain.source_vertex_ids for domain in graph.checkpoint_domains] == [
        (ExecutionVertexId(1, 0),),
        (ExecutionVertexId(1, 1),),
    ]
    assert [domain.sink_vertex_ids for domain in graph.checkpoint_domains] == [
        (ExecutionVertexId(3, 0),),
        (ExecutionVertexId(3, 1),),
    ]
    assert len({domain.id for domain in graph.checkpoint_domains}) == 2


def test_checkpoint_domain_lookup_and_rescale_recompute_membership() -> None:
    logical, graph = _parallel_forward_graph()
    first, second = graph.checkpoint_domains

    assert graph.checkpoint_domain(ExecutionVertexId(2, 0)) is first
    assert graph.find_checkpoint_domain(ExecutionVertexId(2, 1)) is second
    assert graph.find_checkpoint_domain(ExecutionVertexId(99, 0)) is None
    resized = graph.rescale_operator(logical.rescale_operator(2, 3), 2)

    assert len(resized.checkpoint_domains) == 1
    assert ExecutionVertexId(2, 2) in resized.checkpoint_domains[0].vertex_ids
    assert resized.checkpoint_domains[0].id not in {first.id, second.id}


def test_checkpoint_registration_records_exact_domain_committers() -> None:
    _logical, graph = _parallel_forward_graph()
    source = graph.source_execution_vertices[0]
    domain = graph.checkpoint_domain(source.id)
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id=graph.namespace)
    coordinator._execution_graph = graph

    registration = coordinator.register_checkpoint(source.id, force=True)

    assert registration.barrier_id is not None
    checkpoint = coordinator._inflight_checkpoints[registration.barrier_id]
    assert checkpoint.domain_id == domain.id
    assert checkpoint.required_committers == domain.sink_vertex_ids
