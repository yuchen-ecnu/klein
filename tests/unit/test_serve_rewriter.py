# SPDX-License-Identifier: Apache-2.0
import functools

import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.serve_options import ServeOptions
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.serve_rewriter import ServeRewriter
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.broadcast_partitioner import BroadcastPartitioner
from ray.klein.runtime.partitioning.channel_topology import FORWARD
from ray.klein.runtime.partitioning.forward_partitioner import ForwardPartitioner
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec
from ray.klein.runtime.partitioning.round_robin_partitioner import RoundRobinPartitioner


def _identity(batch):
    return batch


def _serve_config() -> Configuration:
    config = Configuration()
    config.set(ServeOptions.CLIENT_NUM_CPUS, 1.0)
    config.set(ServeOptions.CLIENT_CONCURRENCY, 2)
    config.set(ServeOptions.CLIENT_BATCH_SIZE, 2)
    config.set(ServeOptions.CLIENT_BATCH_TIMEOUT, 3)
    return config


def _graph(context: KleinContext, name: str = "serve-rewriter") -> LogicalGraph:
    return LogicalGraph.from_sinks(context.sinks, name, context.config)


@pytest.mark.parametrize("method", ["map", "flat_map", "filter"])
def test_rejects_non_map_batches_operators(method: str) -> None:
    context = KleinContext(_serve_config())
    stream = context.from_items([{"value": 1}])
    function = (lambda row: True) if method == "filter" else _identity
    getattr(stream, method)(function, ray_serve_enabled=True, batch_size=1).show()

    with pytest.raises(ValueError, match="only map_batches operators are supported"):
        ServeRewriter(_graph(context)).rewrite()


@pytest.mark.parametrize("batch_format", ["pandas", "pyarrow", "native"])
def test_rejects_unsupported_batch_format(batch_format: str) -> None:
    context = KleinContext()
    context.from_items([{"value": 1}]).map_batches(
        _identity,
        ray_serve_enabled=True,
        batch_format=batch_format,
    ).show()

    with pytest.raises(ValueError, match="unsupported batch_format"):
        ServeRewriter(_graph(context)).extract_serve_functions()


async def _async_identity(batch):
    return batch


class _AsyncCallable:
    async def __call__(self, batch):
        return batch


class _NotCallable:
    pass


class _AsyncClose:
    def __call__(self, batch):
        return batch

    async def close(self) -> None:
        return None


class _CustomForwardPartitioner(Partitioner):
    topology = FORWARD

    def partition(self, record: Record) -> list[int]:
        return [0]

    def to_spec(self) -> PartitionerSpec:
        return PartitionerSpec(type(self), topology=self.topology)


@pytest.mark.parametrize("function", [_async_identity, _AsyncCallable])
def test_rejects_async_serve_callable(function) -> None:
    context = KleinContext()
    context.from_items([{"value": 1}]).map_batches(function, ray_serve_enabled=True).show()

    with pytest.raises(ValueError, match="only synchronous"):
        ServeRewriter(_graph(context)).rewrite()


@pytest.mark.parametrize("function", [42, _NotCallable])
def test_rejects_non_callable_serve_function(function) -> None:
    context = KleinContext()
    context.from_items([{"value": 1}]).map_batches(function, ray_serve_enabled=True).show()

    with pytest.raises(ValueError, match=r"must (?:be callable|define __call__)"):
        ServeRewriter(_graph(context)).extract_serve_functions()


def test_rejects_wrapped_async_serve_callable() -> None:
    @functools.wraps(_async_identity)
    def hidden_async(batch):
        return _async_identity(batch)

    context = KleinContext()
    context.from_items([{"value": 1}]).map_batches(hidden_async, ray_serve_enabled=True).show()

    with pytest.raises(ValueError, match="only synchronous"):
        ServeRewriter(_graph(context)).rewrite()


def test_rejects_async_close_lifecycle() -> None:
    context = KleinContext()
    context.from_items([{"value": 1}]).map_batches(_AsyncClose, ray_serve_enabled=True).show()

    with pytest.raises(ValueError, match=r"close\(\) must be synchronous"):
        ServeRewriter(_graph(context)).rewrite()


def test_rejects_external_output_from_internal_vertex() -> None:
    context = KleinContext(_serve_config())
    first = context.from_items([{"value": 1}]).map_batches(_identity, ray_serve_enabled=True)
    tail = first.map_batches(_identity, ray_serve_enabled=True)
    first.map_batches(_identity).show()
    tail.show()

    with pytest.raises(ValueError, match="external output from an internal vertex"):
        ServeRewriter(_graph(context)).rewrite()


def test_tail_fanout_is_preserved() -> None:
    context = KleinContext(_serve_config())
    first = context.from_items([{"value": 1}]).map_batches(_identity, ray_serve_enabled=True)
    tail = first.map_batches(_identity, ray_serve_enabled=True)
    tail.map_batches(_identity, name="left").show()
    tail.map_batches(_identity, name="right").show()

    rewritten = ServeRewriter(_graph(context)).rewrite()
    proxy_id = next(vertex_id for vertex_id, vertex in rewritten.vertices.items() if vertex.name.startswith("Embedded"))
    assert len(rewritten.downstream(proxy_id)) == 2


def test_boundary_partitioners_are_preserved() -> None:
    context = KleinContext()
    source = context.from_items([{"value": 1}]).round_robin()
    served = source.map_batches(_identity, ray_serve_enabled=True).broadcast()
    downstream = served.map_batches(_identity)
    downstream.show()
    job_name = "serve-partitioners"
    graph = _graph(context, job_name)

    source_id = VertexId(job_name, source.id)
    served_id = VertexId(job_name, served.id)
    downstream_id = VertexId(job_name, downstream.id)
    assert graph.partitioner_for(source_id, served_id).is_type(RoundRobinPartitioner)
    assert graph.partitioner_for(served_id, downstream_id).is_type(BroadcastPartitioner)

    rewritten = ServeRewriter(graph).rewrite()
    assert rewritten.partitioner_for(source_id, served_id).is_type(RoundRobinPartitioner)
    assert rewritten.partitioner_for(served_id, downstream_id).is_type(BroadcastPartitioner)


def test_retargets_builtin_forward_when_proxy_concurrency_changes() -> None:
    context = KleinContext()
    source = context.from_items([{"value": 1}]).partition_by(ForwardPartitioner())
    served = source.map_batches(_identity, ray_serve_enabled=True, concurrency=2)
    served.show()

    rewritten = ServeRewriter(_graph(context)).rewrite()
    source_id = VertexId("serve-rewriter", source.id)
    proxy_id = VertexId("serve-rewriter", served.id)
    assert not rewritten.partitioner_for(source_id, proxy_id).is_type(ForwardPartitioner)


def test_rejects_custom_forward_when_proxy_concurrency_changes() -> None:
    context = KleinContext()
    source = context.from_items([{"value": 1}]).partition_by(_CustomForwardPartitioner())
    source.map_batches(_identity, ray_serve_enabled=True, concurrency=2).show()

    with pytest.raises(ValueError, match="Cannot preserve custom FORWARD"):
        ServeRewriter(_graph(context)).rewrite()
