# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from unittest.mock import Mock

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
from ray.klein.state.state_snapshot_reference import StateSnapshotReference


def _coordinator_graph(
    domain: CheckpointDomain,
    stream_tasks: dict[ExecutionVertexId, Mock],
):
    vertices = {
        vertex_id: SimpleNamespace(id=vertex_id, stream_task=stream_tasks.get(vertex_id))
        for vertex_id in domain.vertex_ids
    }
    join = next(
        vertex_id
        for vertex_id in domain.vertex_ids
        if vertex_id not in domain.source_vertex_ids and vertex_id not in domain.sink_vertex_ids
    )
    sink = domain.sink_vertex_ids[0]
    execution_edges = [
        SimpleNamespace(source=vertices[source], target=vertices[join]) for source in domain.source_vertex_ids
    ]
    execution_edges.append(SimpleNamespace(source=vertices[join], target=vertices[sink]))
    return SimpleNamespace(
        checkpoint_domains=(domain,),
        job_edges=(SimpleNamespace(execution_edges=tuple(execution_edges)),),
        find_checkpoint_domain=lambda vertex_id: domain if vertex_id in domain.vertex_ids else None,
        find_execution_vertex=vertices.get,
        execution_vertex=vertices.__getitem__,
    )


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
    barrier_splits = {vertex_id: dict(source_counts) for vertex_id, source_counts in graph.barrier_splits.items()}

    domains = graph.checkpoint_domains

    assert isinstance(domains, tuple)
    assert [domain.vertex_ids for domain in domains] == [
        (ExecutionVertexId(1, 0), ExecutionVertexId(2, 0), ExecutionVertexId(3, 0)),
        (ExecutionVertexId(1, 1), ExecutionVertexId(2, 1), ExecutionVertexId(3, 1)),
    ]
    assert [domain.source_vertex_ids for domain in domains] == [
        (ExecutionVertexId(1, 0),),
        (ExecutionVertexId(1, 1),),
    ]
    assert [domain.sink_vertex_ids for domain in domains] == [
        (ExecutionVertexId(3, 0),),
        (ExecutionVertexId(3, 1),),
    ]
    assert len({domain.id for domain in domains}) == 2
    assert all(domain.id.startswith("checkpoint-domain-") for domain in domains)
    assert graph.barrier_splits == barrier_splits


def test_checkpoint_domain_vertex_lookup_has_strict_and_optional_forms() -> None:
    _logical, graph = _parallel_forward_graph()
    first, second = graph.checkpoint_domains

    assert graph.checkpoint_domain(ExecutionVertexId(2, 0)) is first
    assert graph.find_checkpoint_domain(ExecutionVertexId(2, 1)) is second
    missing = ExecutionVertexId(99, 0)
    assert graph.find_checkpoint_domain(missing) is None
    with pytest.raises(KeyError):
        graph.checkpoint_domain(missing)


def test_rescale_recomputes_checkpoint_domains_from_new_physical_edges() -> None:
    logical, graph = _parallel_forward_graph()
    previous_domain_ids = {domain.id for domain in graph.checkpoint_domains}

    resized_logical = logical.rescale_operator(2, 3)
    resized = graph.rescale_operator(resized_logical, 2)

    assert len(resized.checkpoint_domains) == 1
    domain = resized.checkpoint_domains[0]
    assert domain.vertex_ids == (
        ExecutionVertexId(1, 0),
        ExecutionVertexId(1, 1),
        ExecutionVertexId(2, 0),
        ExecutionVertexId(2, 1),
        ExecutionVertexId(2, 2),
        ExecutionVertexId(3, 0),
        ExecutionVertexId(3, 1),
    )
    assert domain.id not in previous_domain_ids
    assert resized.checkpoint_domain(ExecutionVertexId(2, 2)) is domain


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


def test_coordinator_allocates_one_shared_epoch_for_connected_sources() -> None:
    source_a = ExecutionVertexId(1, 0)
    source_b = ExecutionVertexId(2, 0)
    join = ExecutionVertexId(3, 0)
    sink = ExecutionVertexId(4, 0)
    domain = CheckpointDomain("domain", (source_a, source_b, join, sink), (source_a, source_b), (sink,))
    tasks = {source_a: Mock(), source_b: Mock(), sink: Mock()}
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = _coordinator_graph(domain, tasks)

    registration = coordinator.register_checkpoint(source_a)

    assert registration.barrier_id is not None
    tasks[source_a].request_checkpoint.assert_not_called()
    tasks[source_b].request_checkpoint.assert_called_once_with(registration.barrier_id)
    checkpoint = coordinator._inflight_checkpoints[registration.barrier_id]
    assert checkpoint.trigger_sources == (source_a, source_b)
    assert checkpoint.required_committers == (sink,)
    assert coordinator.source_checkpoint_started(registration.barrier_id, source_a)
    assert coordinator.register_checkpoint(source_b).barrier_id == registration.barrier_id
    assert coordinator.source_checkpoint_started(registration.barrier_id, source_b)
    assert coordinator.register_checkpoint(source_a).barrier_id is None


@pytest.mark.asyncio
async def test_domain_checkpoint_commits_all_source_positions(monkeypatch) -> None:
    source_a = ExecutionVertexId(1, 0)
    source_b = ExecutionVertexId(2, 0)
    join = ExecutionVertexId(3, 0)
    sink = ExecutionVertexId(4, 0)
    domain = CheckpointDomain("domain", (source_a, source_b, join, sink), (source_a, source_b), (sink,))
    tasks = {source_a: Mock(), source_b: Mock(), sink: Mock()}
    tasks[source_a].notify_source_checkpoint_complete.return_value = (True, {"offset": 11})
    tasks[source_b].notify_source_checkpoint_complete.return_value = (True, {"offset": 29})
    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="job")
    coordinator._execution_graph = _coordinator_graph(domain, tasks)
    coordinator._ensure_locks()

    async def identity(value):
        return value

    monkeypatch.setattr("ray.klein.runtime.coordinator.checkpoint_coordinator.klein.aget", identity)
    registration = coordinator.register_checkpoint(source_a)
    barrier_id = registration.barrier_id
    assert barrier_id is not None
    sibling_state = StateSnapshotReference(7, "sibling", inline_payload=b"sibling")
    domain_state = StateSnapshotReference(6, "domain", inline_payload=b"domain")
    coordinator._latest_operator_states["3:1"] = sibling_state
    coordinator._inflight_operator_states[barrier_id] = {"3:0": domain_state}
    assert coordinator.source_checkpoint_started(barrier_id, source_a)
    assert coordinator.source_checkpoint_started(barrier_id, source_b)

    assert await coordinator.notify_checkpoint_aligned(barrier_id, sink)
    assert coordinator._latest_source_states["1:0"].state == {"offset": 11}
    assert coordinator._latest_source_states["2:0"].state == {"offset": 29}
    assert coordinator._latest_operator_states == {"3:0": domain_state, "3:1": sibling_state}
    assert "domain" not in coordinator._active_checkpoint_by_domain
