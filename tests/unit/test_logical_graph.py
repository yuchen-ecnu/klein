# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the immutable LogicalGraph data structures."""

import pytest

from ray.klein.api.node_type import NodeType
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.partitioning import ForwardPartitioner, RescalePartitioner
from ray.klein.runtime.resources import Resources


def _vspec(idx: int, node_type: NodeType = NodeType.TRANSFORM, parallelism=1) -> VertexSpec:
    operator_type = OperatorType.SOURCE if node_type == NodeType.SOURCE else OperatorType.ONE_INPUT
    return VertexSpec(
        id=VertexId("job", idx),
        name=f"op{idx}",
        operator=OperatorSpec(StreamOperator, None, idx, f"op{idx}", operator_type),
        node_type=node_type,
        resources=Resources(num_cpus=1.0, num_gpus=0, concurrency=parallelism),
    )


def test_vertex_identity():
    vid = VertexId("job", 3)
    assert str(vid) == "job/3"
    assert VertexId("job", 3) == vid


def _builder() -> LogicalGraphBuilder:
    return LogicalGraphBuilder("job", Configuration(include_environment=False))


def test_concurrency_resolution():
    assert _vspec(1, parallelism=4).concurrency == 4
    assert _vspec(1, parallelism=(2, 8)).concurrency == 2  # lower bound of range
    assert _vspec(1, parallelism=None).concurrency == 1


def test_builder_build_and_adjacency():
    b = _builder()
    s = _vspec(1, NodeType.SOURCE)
    m = _vspec(2)
    k = _vspec(3, NodeType.SINK)
    b.add_vertex(s).add_vertex(m).add_vertex(k)
    b.add_edge(EdgeSpec(s.id, m.id, ForwardPartitioner().to_spec()))
    b.add_edge(EdgeSpec(m.id, k.id, ForwardPartitioner().to_spec()))
    g = b.build()

    assert set(g.vertices.keys()) == {s.id, m.id, k.id}
    assert len(g.edges) == 2
    assert g.downstream(s.id) == (m.id,)
    assert g.upstream(k.id) == (m.id,)
    assert g.downstream(k.id) == ()
    first = g.partitioner_for(s.id, m.id).build()
    second = g.partitioner_for(s.id, m.id).build()
    assert isinstance(first, ForwardPartitioner)
    assert first is not second


def test_edge_dedupe_and_dangling_rejected():
    b = _builder()
    s, m = _vspec(1, NodeType.SOURCE), _vspec(2)
    b.add_vertex(s).add_vertex(m)
    partitioner = ForwardPartitioner().to_spec()
    b.add_edge(EdgeSpec(s.id, m.id, partitioner))
    b.add_edge(EdgeSpec(s.id, m.id, partitioner))  # identical duplicate is ignored
    with pytest.raises(KeyError, match="do not exist"):
        b.add_edge(EdgeSpec(s.id, VertexId("job", 99), ForwardPartitioner().to_spec()))
    g = b.build()
    assert len(g.edges) == 1


def test_sources_by_operator_type_not_indegree():
    # A union-branch source has only out-edges but IS a source. Build a diamond
    # where source 7 feeds into a transform that also has another input, so
    # source 7's in-degree is 0 but more importantly we assert it's detected.
    b = _builder()
    s1 = _vspec(1, NodeType.SOURCE)
    s7 = _vspec(7, NodeType.SOURCE)
    mid = _vspec(2)
    k = _vspec(3, NodeType.SINK)
    for v in (s1, s7, mid, k):
        b.add_vertex(v)
    b.add_edge(EdgeSpec(s1.id, mid.id, ForwardPartitioner().to_spec()))
    b.add_edge(EdgeSpec(s7.id, mid.id, ForwardPartitioner().to_spec()))
    b.add_edge(EdgeSpec(mid.id, k.id, ForwardPartitioner().to_spec()))
    g = b.build()

    sources = set(g.sources)
    assert sources == {s1.id, s7.id}  # both detected by operator type
    assert g.sinks == (k.id,)


def test_remove_vertex_drops_incident_edges():
    b = _builder()
    s, m, k = _vspec(1, NodeType.SOURCE), _vspec(2), _vspec(3, NodeType.SINK)
    for v in (s, m, k):
        b.add_vertex(v)
    b.add_edge(EdgeSpec(s.id, m.id, ForwardPartitioner().to_spec()))
    b.add_edge(EdgeSpec(m.id, k.id, ForwardPartitioner().to_spec()))
    b.add_edge(EdgeSpec(s.id, k.id, ForwardPartitioner().to_spec()))
    b.remove_vertex(m.id)
    g = b.build()
    assert m.id not in g.vertices
    assert len(g.edges) == 1
    assert (g.edges[0].source, g.edges[0].target) == (s.id, k.id)


def test_immutability_to_builder_roundtrip():
    b = _builder()
    s, k = _vspec(1, NodeType.SOURCE), _vspec(2, NodeType.SINK)
    b.add_vertex(s).add_vertex(k).add_edge(EdgeSpec(s.id, k.id, ForwardPartitioner().to_spec()))
    g1 = b.build()
    # derive a new graph; original must be unchanged
    g2 = g1.to_builder().remove_vertex(k.id).build()
    assert len(g1.edges) == 1 and len(g1.vertices) == 2
    assert len(g2.edges) == 0 and len(g2.vertices) == 1

    with pytest.raises(TypeError):
        g1.vertices[s.id] = k


def test_graph_owns_a_defensive_configuration_snapshot() -> None:
    config = Configuration(include_environment=False)
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    builder = LogicalGraphBuilder("job", config)
    source = _vspec(1, NodeType.SOURCE)
    builder.add_vertex(source)
    graph = builder.build()

    config.set(PipelineOptions.OPERATOR_CHAINING, True)
    exposed = graph.config
    exposed.set(PipelineOptions.OPERATOR_CHAINING, True)

    assert graph.config.get(PipelineOptions.OPERATOR_CHAINING) is False


def test_cycle_is_rejected_at_build_time() -> None:
    builder = _builder()
    source = _vspec(1, NodeType.SOURCE)
    left = _vspec(2)
    right = _vspec(3)
    for vertex in (source, left, right):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, left.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(left.id, right.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(right.id, left.id, ForwardPartitioner().to_spec()))

    with pytest.raises(ValueError, match="acyclic"):
        builder.build()


def test_rescale_operator_changes_only_target_and_rewrites_incident_forward_edges() -> None:
    builder = _builder()
    source = _vspec(1, NodeType.SOURCE, parallelism=2)
    target = _vspec(2, parallelism=2)
    sink = _vspec(3, NodeType.SINK, parallelism=2)
    for vertex in (source, target, sink):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, target.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(target.id, sink.id, ForwardPartitioner().to_spec()))
    graph = builder.build()

    resized = graph.rescale_operator(target.id.index, 4)

    assert graph.get(target.id).concurrency == 2
    assert resized.get(target.id).concurrency == 4
    assert resized.get(source.id) is source
    assert resized.get(sink.id) is sink
    assert isinstance(resized.partitioner_for(source.id, target.id).build(), RescalePartitioner)
    assert isinstance(resized.partitioner_for(target.id, sink.id).build(), RescalePartitioner)


def test_rescale_operator_resolves_numeric_and_unique_display_names() -> None:
    builder = _builder()
    source = _vspec(1, NodeType.SOURCE)
    sink = _vspec(2, NodeType.SINK)
    builder.add_vertex(source).add_vertex(sink)
    builder.add_edge(EdgeSpec(source.id, sink.id, ForwardPartitioner().to_spec()))
    graph = builder.build()

    assert graph.resolve_operator("2") == sink.id
    assert graph.resolve_operator("op2") == sink.id
    assert graph.rescale_operator("op2", 3).get(sink.id).concurrency == 3
    with pytest.raises(KeyError, match="does not exist"):
        graph.resolve_operator(99)
    with pytest.raises(ValueError, match="greater than zero"):
        graph.rescale_operator(2, 0)
