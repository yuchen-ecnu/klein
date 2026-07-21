# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the immutable LogicalGraph data structures."""

from dataclasses import replace

import pytest

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.node_type import NodeType
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.udf_options import UDFOptions
from ray.klein.runtime.graph.batch_compiler import BatchCompiler
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


class _BatchDataset:
    def __init__(self, name: str, *, materialized=None) -> None:
        self.name = name
        self.materialized = self if materialized is None else materialized
        self.materialize_count = 0

    def materialize(self):
        self.materialize_count += 1
        return self.materialized


def _batch_vspec(
    idx: int,
    node_type: NodeType,
    lowering,
    *,
    operator_type: OperatorType | None = None,
    async_buffer_size: int | None = None,
) -> VertexSpec:
    if operator_type is None:
        operator_type = OperatorType.SOURCE if node_type is NodeType.SOURCE else OperatorType.ONE_INPUT
    logical_function = (
        None
        if lowering is None
        else LogicalFunction(
            lambda value: value,
            lowering=lowering,
            batch_size=4,
            batch_timeout=1,
            batch_format="numpy",
            async_buffer_size=async_buffer_size,
        )
    )
    return VertexSpec(
        id=VertexId("job", idx),
        name=f"batch-op-{idx}",
        operator=OperatorSpec(StreamOperator, logical_function, idx, f"batch-op-{idx}", operator_type),
        node_type=node_type,
        resources=Resources(),
    )


def test_batch_compiler_waits_for_all_inputs_and_builds_runtime_context() -> None:
    left_data = _BatchDataset("left")
    right_data = _BatchDataset("right")
    joined_data = _BatchDataset("joined")
    join_contexts = []

    def lower_left(context):
        assert context.upstream_ds == ()
        assert context.runtime_context is None
        return left_data

    def lower_right(context):
        assert context.upstream_ds == ()
        assert context.runtime_context is None
        return right_data

    def lower_join(context):
        join_contexts.append(context)
        assert context.upstream_ds == (left_data, right_data)
        return joined_data

    left = _batch_vspec(1, NodeType.SOURCE, lower_left)
    right = _batch_vspec(2, NodeType.SOURCE, lower_right)
    sink = _batch_vspec(3, NodeType.SINK, lower_join, operator_type=OperatorType.TWO_INPUT)
    builder = _builder()
    for vertex in (left, right, sink):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(left.id, sink.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(right.id, sink.id, ForwardPartitioner().to_spec()))
    graph = builder.build()

    result = BatchCompiler(graph).execute()

    assert result is joined_data
    assert len(join_contexts) == 1
    runtime_context = join_contexts[0].runtime_context
    assert runtime_context is not None
    assert (runtime_context.task_name, runtime_context.task_index, runtime_context.parallelism) == (
        sink.name,
        -1,
        -1,
    )
    assert runtime_context.runtime_info == sink.operator.runtime_info
    assert runtime_context.config.to_dict() == graph.config.to_dict()
    assert runtime_context.metric_group.all_labels == {
        "job_id": "job",
        "job_name": "job",
        "operator_id": "job/3",
        "operator_name": sink.name,
        "subtask_index": "-1",
        "task_id": "job/3",
        "task_name": sink.name,
    }


def test_batch_compiler_materializes_fan_out_once_and_returns_all_sinks() -> None:
    materialized_source = _BatchDataset("materialized-source")
    source_data = _BatchDataset("source", materialized=materialized_source)
    seen_upstreams = []

    def lower_source(_context):
        return source_data

    def lower_left(context):
        seen_upstreams.append(context.upstream_ds)
        return "left-result"

    def lower_right(context):
        seen_upstreams.append(context.upstream_ds)
        return "right-result"

    source = _batch_vspec(1, NodeType.SOURCE, lower_source)
    left = _batch_vspec(2, NodeType.SINK, lower_left)
    right = _batch_vspec(3, NodeType.SINK, lower_right)
    builder = _builder()
    for vertex in (source, left, right):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, left.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(source.id, right.id, ForwardPartitioner().to_spec()))

    result = BatchCompiler(builder.build()).execute()

    assert result == ["left-result", "right-result"]
    assert seen_upstreams == [(materialized_source,), (materialized_source,)]
    assert source_data.materialize_count == 1


def test_batch_compiler_rejects_source_without_batch_function() -> None:
    source = _batch_vspec(1, NodeType.SOURCE, None)
    graph = _builder().add_vertex(source).build()

    with pytest.raises(ValueError, match=r"Source vertex job/1 has no logical function"):
        BatchCompiler(graph).execute()


def test_batch_compiler_rejects_non_source_without_batch_function() -> None:
    source_data = _BatchDataset("source")
    source = _batch_vspec(1, NodeType.SOURCE, lambda _context: source_data)
    sink = _batch_vspec(2, NodeType.SINK, None)
    graph = (
        _builder()
        .add_vertex(source)
        .add_vertex(sink)
        .add_edge(EdgeSpec(source.id, sink.id, ForwardPartitioner().to_spec()))
        .build()
    )

    with pytest.raises(ValueError, match=r"Vertex job/2 has no logical function"):
        BatchCompiler(graph).execute()


def test_runtime_mode_requires_streaming_for_unbounded_source() -> None:
    source = _batch_vspec(1, NodeType.SOURCE, lambda _context: _BatchDataset("source"))
    sink = _batch_vspec(2, NodeType.SINK, lambda context: context.upstream_ds[0])
    graph = (
        _builder()
        .add_vertex(source)
        .add_vertex(sink)
        .add_edge(EdgeSpec(source.id, sink.id, ForwardPartitioner().to_spec()))
        .build()
    )

    assert graph.runtime_mode_requires_streaming


def test_runtime_mode_checks_streaming_only_intermediate_operators() -> None:
    source = _batch_vspec(1, NodeType.SOURCE, lambda _context: _BatchDataset("source"))
    source = replace(source, operator=replace(source.operator, parameters={"bounded": True}))
    streaming_only = _batch_vspec(2, NodeType.TRANSFORM, None)
    sink = _batch_vspec(3, NodeType.SINK, lambda context: context.upstream_ds[0])
    builder = _builder()
    for vertex in (source, streaming_only, sink):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, streaming_only.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(streaming_only.id, sink.id, ForwardPartitioner().to_spec()))

    assert builder.build().runtime_mode_requires_streaming


def test_runtime_mode_allows_fully_lowered_bounded_graph() -> None:
    source = _batch_vspec(1, NodeType.SOURCE, lambda _context: _BatchDataset("source"))
    source = replace(source, operator=replace(source.operator, parameters={"bounded": True}))
    transform = _batch_vspec(2, NodeType.TRANSFORM, lambda context: context.upstream_ds[0])
    sink = _batch_vspec(3, NodeType.SINK, lambda context: context.upstream_ds[0])
    builder = _builder()
    for vertex in (source, transform, sink):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, transform.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(transform.id, sink.id, ForwardPartitioner().to_spec()))

    assert not builder.build().runtime_mode_requires_streaming


def test_runtime_mode_requires_streaming_for_async_intermediate_operator() -> None:
    source = _batch_vspec(1, NodeType.SOURCE, lambda _context: _BatchDataset("source"))
    source = replace(source, operator=replace(source.operator, parameters={"bounded": True}))
    async_transform = _batch_vspec(
        2,
        NodeType.TRANSFORM,
        lambda context: context.upstream_ds[0],
        async_buffer_size=8,
    )
    sink = _batch_vspec(3, NodeType.SINK, lambda context: context.upstream_ds[0])
    builder = _builder()
    for vertex in (source, async_transform, sink):
        builder.add_vertex(vertex)
    builder.add_edge(EdgeSpec(source.id, async_transform.id, ForwardPartitioner().to_spec()))
    builder.add_edge(EdgeSpec(async_transform.id, sink.id, ForwardPartitioner().to_spec()))

    assert builder.build().runtime_mode_requires_streaming


def test_runtime_mode_requires_streaming_for_ignore_udf_exception_policy() -> None:
    config = Configuration(include_environment=False)
    config.set(UDFOptions.IGNORE_EXCEPTIONS, True)
    source = _batch_vspec(1, NodeType.SOURCE, lambda _context: _BatchDataset("source"))
    source = replace(source, operator=replace(source.operator, parameters={"bounded": True}))
    sink = _batch_vspec(2, NodeType.SINK, lambda context: context.upstream_ds[0])
    graph = (
        LogicalGraphBuilder("job", config)
        .add_vertex(source)
        .add_vertex(sink)
        .add_edge(EdgeSpec(source.id, sink.id, ForwardPartitioner().to_spec()))
        .build()
    )

    assert graph.runtime_mode_requires_streaming
