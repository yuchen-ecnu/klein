# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the immutable LogicalGraph data structures (G1)."""

import pytest

from ray.klein.api.node_type import NodeType
from ray.klein.runtime.graph.edge_spec import EdgeSpec
from ray.klein.runtime.graph.logical_graph_builder import LogicalGraphBuilder
from ray.klein.runtime.graph.subtask_id import SubtaskId
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.graph.vertex_spec import VertexSpec
from ray.klein.runtime.partitioning import ForwardPartitioner
from ray.klein.runtime.resources import Resources


def _vspec(idx: int, node_type: NodeType = NodeType.TRANSFORM, parallelism=1) -> VertexSpec:
    return VertexSpec(
        id=VertexId("job", idx),
        name=f"op{idx}",
        operator=None,  # operator not needed for topology tests
        node_type=node_type,
        resources=Resources(num_cpus=1.0, num_gpus=0, concurrency=parallelism),
    )


def test_vertex_and_subtask_identity():
    vid = VertexId("job", 3)
    assert str(vid) == "job/3"
    sid = SubtaskId(vid, 2)
    assert sid.actor_name == "job/3#2"
    assert str(sid) == "job/3#2"
    # identity is hashable/value-equal (frozen dataclass)
    assert VertexId("job", 3) == vid
    assert SubtaskId(vid, 2) == sid


def test_concurrency_resolution():
    assert _vspec(1, parallelism=4).concurrency == 4
    assert _vspec(1, parallelism=(2, 8)).concurrency == 2  # lower bound of range
    assert _vspec(1, parallelism=None).concurrency == 1


def test_builder_build_and_adjacency():
    b = LogicalGraphBuilder()
    s = _vspec(1, NodeType.SOURCE)
    m = _vspec(2)
    k = _vspec(3, NodeType.SINK)
    b.add_vertex(s).add_vertex(m).add_vertex(k)
    b.add_edge(EdgeSpec(s.id, m.id, ForwardPartitioner()))
    b.add_edge(EdgeSpec(m.id, k.id, ForwardPartitioner()))
    g = b.build()

    assert set(g.vertices.keys()) == {s.id, m.id, k.id}
    assert len(g.edges) == 2
    assert g.downstream(s.id) == (m.id,)
    assert g.upstream(k.id) == (m.id,)
    assert g.downstream(k.id) == ()


def test_edge_dedupe_and_dangling_rejected():
    b = LogicalGraphBuilder()
    s, m = _vspec(1, NodeType.SOURCE), _vspec(2)
    b.add_vertex(s).add_vertex(m)
    b.add_edge(EdgeSpec(s.id, m.id, ForwardPartitioner()))
    b.add_edge(EdgeSpec(s.id, m.id, ForwardPartitioner()))  # dup ignored
    b.add_edge(EdgeSpec(s.id, VertexId("job", 99), ForwardPartitioner()))  # dangling target
    g = b.build()
    assert len(g.edges) == 1


def test_sources_by_operator_type_not_indegree():
    # A union-branch source has only out-edges but IS a source. Build a diamond
    # where source 7 feeds into a transform that also has another input, so
    # source 7's in-degree is 0 but more importantly we assert it's detected.
    b = LogicalGraphBuilder()
    s1 = _vspec(1, NodeType.SOURCE)
    s7 = _vspec(7, NodeType.SOURCE)
    mid = _vspec(2)
    k = _vspec(3, NodeType.SINK)
    for v in (s1, s7, mid, k):
        b.add_vertex(v)
    b.add_edge(EdgeSpec(s1.id, mid.id, ForwardPartitioner()))
    b.add_edge(EdgeSpec(s7.id, mid.id, ForwardPartitioner()))
    b.add_edge(EdgeSpec(mid.id, k.id, ForwardPartitioner()))
    g = b.build()

    sources = set(g.sources)
    assert sources == {s1.id, s7.id}  # both detected by operator type
    assert g.sinks == (k.id,)


def test_remove_vertex_drops_incident_edges():
    b = LogicalGraphBuilder()
    s, m, k = _vspec(1, NodeType.SOURCE), _vspec(2), _vspec(3, NodeType.SINK)
    for v in (s, m, k):
        b.add_vertex(v)
    b.add_edge(EdgeSpec(s.id, m.id, ForwardPartitioner()))
    b.add_edge(EdgeSpec(m.id, k.id, ForwardPartitioner()))
    b.remove_vertex(m.id)
    g = b.build()
    assert m.id not in g.vertices
    assert len(g.edges) == 0  # both edges touching m removed


def test_immutability_to_builder_roundtrip():
    b = LogicalGraphBuilder()
    s, k = _vspec(1, NodeType.SOURCE), _vspec(2, NodeType.SINK)
    b.add_vertex(s).add_vertex(k).add_edge(EdgeSpec(s.id, k.id, ForwardPartitioner()))
    g1 = b.build()
    # derive a new graph; original must be unchanged
    g2 = g1.to_builder().remove_vertex(k.id).build()
    assert len(g1.edges) == 1 and len(g1.vertices) == 2
    assert len(g2.edges) == 0 and len(g2.vertices) == 1

    with pytest.raises(TypeError):
        g1.vertices[s.id] = k
