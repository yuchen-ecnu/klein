# SPDX-License-Identifier: Apache-2.0
from collections import Counter
from time import sleep
from typing import Any

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.data_stream import DataStream
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.node_type import NodeType
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer
from tests.support.execution_graph import expand_execution_graph


class MockSourceFunction(SourceFunction):
    def __init__(self, prefix: str, sleep_interval: float = 0.05, record_num: int = -1):
        self.idx: int = 0
        self._prefix = prefix
        self._sub_task_id: int | None = None
        self._interrupted = False
        self._sleep_interval = sleep_interval
        self._record_num = record_num

    def open(self, runtime_context: RuntimeContext) -> None:
        self._sub_task_id = runtime_context.task_index

    def run(self, context: SourceContext) -> None:
        while not self._interrupted:
            context.collect({"idx": self.gen_idx()})
            sleep(self._sleep_interval)
            if 0 < self._record_num <= self.idx:
                self.cancel()

    def gen_idx(self):
        self.idx += 1
        return f"{self._prefix}-{self._sub_task_id}-{self.idx}"

    def snapshot_state(self, checkpoint_id: int) -> int:
        return self.idx

    def restore_state(self, state: int) -> None:
        self.idx = state

    def cancel(self) -> None:
        self._interrupted = True


def gen_evi(ejvi: int, idx) -> ExecutionVertexId:
    return ExecutionVertexId(ejvi, idx)


def mock_flat_map(data: dict[str, Any]):
    yield data


def test_common_union():
    config: Configuration = Configuration()
    ctx = KleinContext(config)

    stream1: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S1"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    ).map(lambda x: x, name="map1", num_cpus=0.1, num_gpus=0, concurrency=4)

    stream2: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S2"],
        fn_constructor_kwargs={"record_num": 2},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=4,
    ).filter(lambda x: True, name="filter", num_cpus=0.1, num_gpus=0, concurrency=3)

    stream3: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S3"],
        fn_constructor_kwargs={"record_num": 3},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    data_stream = stream1.union(stream2, stream3).map(lambda x: x, name="map2", num_cpus=0.1, num_gpus=0, concurrency=2)
    data_stream.show(-1, num_cpus=0.1, concurrency=2)
    data_stream = data_stream.flat_map(
        lambda x: mock_flat_map(x),
        name="flat_map1",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=3,
    )
    data_stream.write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)

    expect_barrier_splits: dict[ExecutionVertexId, dict[ExecutionVertexId, int]] = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(3, 0): {gen_evi(3, 0): 1},
        gen_evi(3, 1): {gen_evi(3, 1): 1},
        gen_evi(3, 2): {gen_evi(3, 2): 1},
        gen_evi(3, 3): {gen_evi(3, 3): 1},
        gen_evi(5, 0): {gen_evi(5, 0): 1},
        gen_evi(5, 1): {gen_evi(5, 1): 1},
        gen_evi(2, 0): {gen_evi(1, 0): 1},
        gen_evi(2, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 2): {gen_evi(1, 0): 1},
        gen_evi(2, 3): {gen_evi(1, 1): 1},
        gen_evi(4, 0): {
            gen_evi(3, 0): 1,
            gen_evi(3, 1): 1,
            gen_evi(3, 2): 1,
            gen_evi(3, 3): 1,
        },
        gen_evi(4, 1): {
            gen_evi(3, 0): 1,
            gen_evi(3, 1): 1,
            gen_evi(3, 2): 1,
            gen_evi(3, 3): 1,
        },
        gen_evi(4, 2): {
            gen_evi(3, 0): 1,
            gen_evi(3, 1): 1,
            gen_evi(3, 2): 1,
            gen_evi(3, 3): 1,
        },
        gen_evi(7, 0): {
            gen_evi(1, 0): 2,
            gen_evi(3, 0): 3,
            gen_evi(3, 1): 3,
            gen_evi(3, 2): 3,
            gen_evi(3, 3): 3,
            gen_evi(5, 0): 1,
        },
        gen_evi(7, 1): {
            gen_evi(1, 1): 2,
            gen_evi(3, 0): 3,
            gen_evi(3, 1): 3,
            gen_evi(3, 2): 3,
            gen_evi(3, 3): 3,
            gen_evi(5, 1): 1,
        },
        gen_evi(8, 0): {
            gen_evi(1, 0): 1,
            gen_evi(3, 0): 1,
            gen_evi(3, 1): 1,
            gen_evi(3, 2): 1,
            gen_evi(3, 3): 1,
            gen_evi(5, 0): 1,
        },
        gen_evi(8, 1): {
            gen_evi(1, 1): 1,
            gen_evi(3, 0): 1,
            gen_evi(3, 1): 1,
            gen_evi(3, 2): 1,
            gen_evi(3, 3): 1,
            gen_evi(5, 1): 1,
        },
        gen_evi(9, 0): {
            gen_evi(1, 0): 1,
            gen_evi(1, 1): 1,
            gen_evi(3, 0): 2,
            gen_evi(3, 1): 2,
            gen_evi(3, 2): 2,
            gen_evi(3, 3): 2,
            gen_evi(5, 0): 1,
            gen_evi(5, 1): 1,
        },
        gen_evi(9, 1): {
            gen_evi(1, 0): 1,
            gen_evi(1, 1): 1,
            gen_evi(3, 0): 2,
            gen_evi(3, 1): 2,
            gen_evi(3, 2): 2,
            gen_evi(3, 3): 2,
            gen_evi(5, 0): 1,
            gen_evi(5, 1): 1,
        },
        gen_evi(9, 2): {
            gen_evi(1, 0): 1,
            gen_evi(1, 1): 1,
            gen_evi(3, 0): 2,
            gen_evi(3, 1): 2,
            gen_evi(3, 2): 2,
            gen_evi(3, 3): 2,
            gen_evi(5, 0): 1,
            gen_evi(5, 1): 1,
        },
        gen_evi(10, 0): {
            gen_evi(1, 0): 3,
            gen_evi(1, 1): 3,
            gen_evi(3, 0): 3,
            gen_evi(3, 1): 3,
            gen_evi(3, 2): 3,
            gen_evi(3, 3): 3,
            gen_evi(5, 0): 3,
            gen_evi(5, 1): 3,
        },
    }
    assert expect_barrier_splits == barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 2,
        gen_evi(1, 1): 2,
        gen_evi(3, 0): 3,
        gen_evi(3, 1): 3,
        gen_evi(3, 2): 3,
        gen_evi(3, 3): 3,
        gen_evi(5, 0): 2,
        gen_evi(5, 1): 2,
    }
    assert expect_act_dict == act_dict

    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S2-0-1"},
        {"idx": "S2-0-2"},
        {"idx": "S2-1-1"},
        {"idx": "S2-1-2"},
        {"idx": "S2-2-1"},
        {"idx": "S2-2-2"},
        {"idx": "S2-3-1"},
        {"idx": "S2-3-2"},
        {"idx": "S3-0-1"},
        {"idx": "S3-0-2"},
        {"idx": "S3-0-3"},
        {"idx": "S3-1-1"},
        {"idx": "S3-1-2"},
        {"idx": "S3-1-3"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


def test_chain_union():
    config: Configuration = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, True)
    ctx = KleinContext(config)

    stream1: DataStream = (
        ctx.source(
            MockSourceFunction,
            fn_constructor_args=["S1"],
            fn_constructor_kwargs={"record_num": 1},
            name="source",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=2,
        )
        .map(lambda x: x, name="map1", num_cpus=0.1, num_gpus=0, concurrency=3)
        .filter(lambda x: True, name="filter1", num_cpus=0.1, num_gpus=0, concurrency=3)
    )

    stream2: DataStream = (
        ctx.source(
            MockSourceFunction,
            fn_constructor_args=["S2"],
            fn_constructor_kwargs={"record_num": 2},
            name="source",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=1,
        )
        .map(lambda x: x, name="map2", num_cpus=0.1, num_gpus=0, concurrency=1)
        .flat_map(
            lambda x: mock_flat_map(x),
            name="flat_map1",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=2,
        )
    )

    data_stream = stream1.union(stream2)

    data_stream = data_stream.map(lambda x: x, name="map3", num_cpus=0.1, num_gpus=0, concurrency=2).filter(
        lambda x: True, name="filter2", num_cpus=0.1, num_gpus=0, concurrency=2
    )

    data_stream.write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST1", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)
    expect_barrier_splits = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(4, 0): {gen_evi(4, 0): 1},
        gen_evi(2, 0): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(2, 1): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(2, 2): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(6, 0): {gen_evi(4, 0): 1},
        gen_evi(6, 1): {gen_evi(4, 0): 1},
        gen_evi(8, 0): {gen_evi(1, 0): 3, gen_evi(1, 1): 3, gen_evi(4, 0): 1},
        gen_evi(8, 1): {gen_evi(1, 0): 3, gen_evi(1, 1): 3, gen_evi(4, 0): 1},
        gen_evi(10, 0): {gen_evi(1, 0): 2, gen_evi(1, 1): 2, gen_evi(4, 0): 2},
    }
    assert barrier_splits == expect_barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 1,
        gen_evi(1, 1): 1,
        gen_evi(4, 0): 1,
    }
    assert act_dict == expect_act_dict
    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S2-0-1"},
        {"idx": "S2-0-2"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


def test_union_union():
    config: Configuration = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, True)
    ctx = KleinContext(config)

    stream1: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S1"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    stream2: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S2"],
        fn_constructor_kwargs={"record_num": 2},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=1,
    )

    data_stream = stream1.union(stream2)

    data_stream = data_stream.map(lambda x: x, name="map1", num_cpus=0.1, num_gpus=0, concurrency=3)

    stream3: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S3"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    data_stream = stream3.union(data_stream)

    data_stream.write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST1", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)
    expect_barrier_splits = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 0): {gen_evi(2, 0): 1},
        gen_evi(4, 0): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(2, 0): 1},
        gen_evi(4, 1): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(2, 0): 1},
        gen_evi(4, 2): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(2, 0): 1},
        gen_evi(5, 0): {gen_evi(5, 0): 1},
        gen_evi(5, 1): {gen_evi(5, 1): 1},
        gen_evi(7, 0): {
            gen_evi(1, 0): 3,
            gen_evi(1, 1): 3,
            gen_evi(2, 0): 3,
            gen_evi(5, 0): 1,
            gen_evi(5, 1): 1,
        },
    }
    assert barrier_splits == expect_barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 1,
        gen_evi(1, 1): 1,
        gen_evi(2, 0): 1,
        gen_evi(5, 0): 1,
        gen_evi(5, 1): 1,
    }
    assert act_dict == expect_act_dict
    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S2-0-1"},
        {"idx": "S2-0-2"},
        {"idx": "S3-0-1"},
        {"idx": "S3-1-1"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


def test_neighbor_union1():
    config: Configuration = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, True)
    ctx = KleinContext(config)
    stream1: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S1"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    stream2: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S2"],
        fn_constructor_kwargs={"record_num": 2},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=1,
    )

    union_stream1 = stream1.union(stream2)

    stream3: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S3"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    data_stream = union_stream1.union(stream3)

    data_stream.write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST1", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)
    expect_barrier_splits = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 0): {gen_evi(2, 0): 1},
        gen_evi(4, 0): {gen_evi(4, 0): 1},
        gen_evi(4, 1): {gen_evi(4, 1): 1},
        gen_evi(6, 0): {
            gen_evi(1, 0): 1,
            gen_evi(1, 1): 1,
            gen_evi(2, 0): 1,
            gen_evi(4, 0): 1,
            gen_evi(4, 1): 1,
        },
    }
    assert barrier_splits == expect_barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 1,
        gen_evi(1, 1): 1,
        gen_evi(2, 0): 1,
        gen_evi(4, 0): 1,
        gen_evi(4, 1): 1,
    }
    assert act_dict == expect_act_dict
    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S2-0-1"},
        {"idx": "S2-0-2"},
        {"idx": "S3-0-1"},
        {"idx": "S3-1-1"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


def test_neighbor_union2():
    config: Configuration = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, True)
    ctx = KleinContext(config)
    stream1: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S1"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    stream2: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S2"],
        fn_constructor_kwargs={"record_num": 2},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=1,
    )

    union_stream1 = stream1.union(stream2)

    stream3: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S3"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    )

    data_stream = stream3.union(union_stream1)

    data_stream.write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST1", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)
    expect_barrier_splits = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 0): {gen_evi(2, 0): 1},
        gen_evi(4, 0): {gen_evi(4, 0): 1},
        gen_evi(4, 1): {gen_evi(4, 1): 1},
        gen_evi(6, 0): {
            gen_evi(1, 0): 1,
            gen_evi(1, 1): 1,
            gen_evi(2, 0): 1,
            gen_evi(4, 0): 1,
            gen_evi(4, 1): 1,
        },
    }
    assert barrier_splits == expect_barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 1,
        gen_evi(1, 1): 1,
        gen_evi(2, 0): 1,
        gen_evi(4, 0): 1,
        gen_evi(4, 1): 1,
    }
    assert act_dict == expect_act_dict
    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S2-0-1"},
        {"idx": "S2-0-2"},
        {"idx": "S3-0-1"},
        {"idx": "S3-1-1"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


def test_complex_union1():
    config: Configuration = Configuration()
    ctx = KleinContext(config)

    def flat_map_mock(data: dict[str, Any]):
        yield data

    stream1: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S1"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    ).flat_map(flat_map_mock, name="flat_map1", num_cpus=0.1, num_gpus=0, concurrency=4)

    stream2: DataStream = stream1.filter(lambda x: True, name="filter1", num_cpus=0.1, num_gpus=0, concurrency=1).map(
        lambda x: x, name="map1", num_cpus=0.1, num_gpus=0, concurrency=4
    )

    stream3: DataStream = stream1.filter(lambda x: True, name="filter2", num_cpus=0.1, num_gpus=0, concurrency=1)

    stream2.union(stream3).filter(lambda x: True, name="filter2", num_cpus=0.1, num_gpus=0, concurrency=1).write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)

    expect_barrier_splits: dict[ExecutionVertexId, dict[ExecutionVertexId, int]] = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 0): {gen_evi(1, 0): 1},
        gen_evi(2, 2): {gen_evi(1, 0): 1},
        gen_evi(2, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 3): {gen_evi(1, 1): 1},
        gen_evi(3, 0): {gen_evi(1, 0): 2, gen_evi(1, 1): 2},
        gen_evi(5, 0): {gen_evi(1, 0): 2, gen_evi(1, 1): 2},
        gen_evi(4, 0): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(4, 1): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(4, 2): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(4, 3): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
        gen_evi(7, 0): {gen_evi(1, 0): 5, gen_evi(1, 1): 5},
        gen_evi(8, 0): {gen_evi(1, 0): 1, gen_evi(1, 1): 1},
    }

    assert expect_barrier_splits == barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 1,
        gen_evi(1, 1): 1,
    }
    assert expect_act_dict == act_dict

    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


def test_complex_union2():
    config: Configuration = Configuration()
    ctx = KleinContext(config)

    def flat_map_mock(data: dict[str, Any]):
        yield data

    stream1: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S1"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=2,
    ).flat_map(flat_map_mock, name="flat_map1", num_cpus=0.1, num_gpus=0, concurrency=4)

    stream2: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=["S2"],
        fn_constructor_kwargs={"record_num": 1},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=1,
    )

    stream3 = stream1.union(stream2)

    stream4: DataStream = stream3.filter(lambda x: True, name="filter1", num_cpus=0.1, num_gpus=0, concurrency=1).map(
        lambda x: x, name="map1", num_cpus=0.1, num_gpus=0, concurrency=4
    )

    stream5: DataStream = stream3.filter(lambda x: True, name="filter2", num_cpus=0.1, num_gpus=0, concurrency=1)

    stream4.union(stream5).filter(lambda x: True, name="filter2", num_cpus=0.1, num_gpus=0, concurrency=1).write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    logical_graph: LogicalGraph = LogicalGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    optimized_graph = LogicalOptimizer(config).optimize(logical_graph)
    exec_graph = expand_execution_graph(optimized_graph)
    barrier_splits = checkpoint_io.barrier_split_counts(exec_graph)

    expect_barrier_splits: dict[ExecutionVertexId, dict[ExecutionVertexId, int]] = {
        gen_evi(1, 0): {gen_evi(1, 0): 1},
        gen_evi(1, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 0): {gen_evi(1, 0): 1},
        gen_evi(2, 2): {gen_evi(1, 0): 1},
        gen_evi(2, 1): {gen_evi(1, 1): 1},
        gen_evi(2, 3): {gen_evi(1, 1): 1},
        gen_evi(3, 0): {gen_evi(3, 0): 1},
        gen_evi(5, 0): {gen_evi(1, 0): 2, gen_evi(1, 1): 2, gen_evi(3, 0): 1},
        gen_evi(6, 0): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(3, 0): 1},
        gen_evi(6, 1): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(3, 0): 1},
        gen_evi(6, 2): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(3, 0): 1},
        gen_evi(6, 3): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(3, 0): 1},
        gen_evi(7, 0): {gen_evi(1, 0): 2, gen_evi(1, 1): 2, gen_evi(3, 0): 1},
        gen_evi(9, 0): {gen_evi(1, 0): 5, gen_evi(1, 1): 5, gen_evi(3, 0): 5},
        gen_evi(10, 0): {gen_evi(1, 0): 1, gen_evi(1, 1): 1, gen_evi(3, 0): 1},
    }

    assert expect_barrier_splits == barrier_splits
    act_dict = checkpoint_io.coordinator_ack_counts(exec_graph)
    expect_act_dict = {
        gen_evi(1, 0): 1,
        gen_evi(1, 1): 1,
        gen_evi(3, 0): 1,
    }
    assert expect_act_dict == act_dict

    client: JobHandle = ctx.execute("test")
    client.wait()
    result = client.get()
    expect_result = [
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S1-0-1"},
        {"idx": "S1-1-1"},
        {"idx": "S2-0-1"},
        {"idx": "S2-0-1"},
    ]
    actual_counter = Counter(tuple(sorted(d.items())) for d in result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter
