# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace

from ray.klein.api.klein_context import KleinContext
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer
from ray.klein.runtime.scheduler.assignment import WorkerNode, round_robin_allocate
from tests.support.execution_graph import expand_execution_graph
from tests.support.streaming import LoopSourceFunction


def test_cluster_node_lookup_is_scoped_to_the_connected_ray_runtime(monkeypatch) -> None:
    from ray.klein.runtime.scheduler import assignment

    captured = {}
    monkeypatch.setattr(
        assignment.ray,
        "get_runtime_context",
        lambda: SimpleNamespace(gcs_address="127.0.0.1:4321"),
    )

    def list_nodes(**kwargs):
        captured.update(kwargs)
        return [SimpleNamespace(node_id="node-1", resources_total={"CPU": 4, "GPU": 1})]

    monkeypatch.setattr(assignment.ray.util.state, "list_nodes", list_nodes)

    nodes, node_ids = assignment.cluster_worker_nodes()

    assert captured["address"] == "127.0.0.1:4321"
    assert node_ids == ["node-1"]
    assert [(node.index, node.cpu, node.gpu) for node in nodes] == [(0, 4.0, 1.0)]


def scheduling_source(context: KleinContext, *, num_cpus: float, concurrency: int):
    return context.source(
        LoopSourceFunction,
        num_cpus=num_cpus,
        concurrency=concurrency,
        name="TestSource",
        bounded=False,
    )


def test_round_robin():
    """
    测试普通场景EV 分布是否均匀
    """
    config: Configuration = Configuration()
    ctx = KleinContext(config)

    (
        scheduling_source(ctx, num_cpus=1, concurrency=1)
        .map(lambda x: x, name="map1", num_cpus=1, num_gpus=0, concurrency=2)
        .rescale()
        .map(lambda x: x, name="map2", num_cpus=1, num_gpus=0, concurrency=3)
        .rescale()
        .write(ConsoleSinkFunction, num_cpus=1, concurrency=2, name="ConsoleSink")
    )

    stream_graph: StreamGraph = StreamGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    job_graph = LogicalOptimizer(config).optimize(stream_graph)
    exec_graph = expand_execution_graph(job_graph)

    job_vertices = sorted(exec_graph.job_vertices.values(), key=lambda job_vertex: job_vertex.id)
    worker_nodes = [
        WorkerNode(0, 5, 0.0),
        WorkerNode(1, 5, 0.0),
        WorkerNode(2, 5, 0.0),
        WorkerNode(3, 5, 0.0),
    ]
    res, _ = round_robin_allocate(job_vertices, worker_nodes)
    assert res == {1: [3], 2: [3, 0], 3: [0, 1, 2], 4: [1, 2]}


def test_round_robin_2():
    config: Configuration = Configuration()
    # Disable chaining so source + map1 stay separate EJVs; this test pins the
    # algorithm's per-EJV assignment, not the chaining optimization.
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    ctx = KleinContext(config)

    (
        scheduling_source(ctx, num_cpus=2, concurrency=1)
        .map(lambda x: x, name="map1", num_cpus=2, num_gpus=0, concurrency=1)
        .map(lambda x: x, name="map2", num_cpus=2, num_gpus=0, concurrency=4)
        .write(ConsoleSinkFunction, num_cpus=2, concurrency=1, name="ConsoleSink")
    )

    stream_graph: StreamGraph = StreamGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    job_graph = LogicalOptimizer(config).optimize(stream_graph)
    exec_graph = expand_execution_graph(job_graph)

    job_vertices = sorted(exec_graph.job_vertices.values(), key=lambda job_vertex: job_vertex.id)
    worker_nodes = [
        WorkerNode(0, 0.0, 0.0),
        WorkerNode(1, 4.0, 0.0),
        WorkerNode(2, 4.0, 0.0),
        WorkerNode(3, 6.0, 0.0),
        WorkerNode(4, 6.0, 0.0),
        WorkerNode(5, 4.0, 0.0),
    ]
    res, _ = round_robin_allocate(job_vertices, worker_nodes)

    assert res == {1: [5], 2: [3], 3: [3, 4, 1, 2], 4: [4]}


def test_round_robin_3():
    """
    测试GPU算子的多个EV允许分布在一个worker 场景
    """
    config: Configuration = Configuration()
    ctx = KleinContext(config)

    (
        scheduling_source(ctx, num_cpus=2, concurrency=3)
        .map(lambda x: x, name="map1", num_cpus=1, num_gpus=0, concurrency=3)
        .map(lambda x: x, name="map2", num_cpus=1, num_gpus=2, concurrency=2)
        .write(ConsoleSinkFunction, num_cpus=2, concurrency=4, name="ConsoleSink")
    )

    stream_graph: StreamGraph = StreamGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    job_graph = LogicalOptimizer(config).optimize(stream_graph)
    exec_graph = expand_execution_graph(job_graph)

    job_vertices = sorted(exec_graph.job_vertices.values(), key=lambda job_vertex: job_vertex.id)
    worker_nodes = [
        WorkerNode(0, 4.0, 4),
        WorkerNode(1, 4.0, 0),
        WorkerNode(2, 4.0, 0),
        WorkerNode(3, 6.0, 0),
        WorkerNode(4, 4.0, 0),
    ]
    res, _ = round_robin_allocate(job_vertices, worker_nodes)

    assert res == {1: [3, 1, 2], 2: [4, 0, 3], 3: [0, 0], 4: [3, 1, 2, 4]}


def test_round_robin_4():
    """
    测试资源异构情况下的轮询分配策略：
    场景：部分节点 GPU 资源充足但 CPU 较少，而作业算子主要依赖 CPU。
    目标：验证在资源约束条件下，调度器能否合理分配任务并实现节点轮转。
    """
    config: Configuration = Configuration()
    ctx = KleinContext(config)

    (
        scheduling_source(ctx, num_cpus=2, concurrency=4)
        .map(lambda x: x, name="map2", num_cpus=5, num_gpus=0, concurrency=2)
        .write(ConsoleSinkFunction, num_cpus=5, concurrency=3, name="ConsoleSink")
    )

    stream_graph: StreamGraph = StreamGraph.from_sinks(ctx.sinks, "TEST", ctx.config)
    job_graph = LogicalOptimizer(config).optimize(stream_graph)
    exec_graph = expand_execution_graph(job_graph)

    job_vertices = sorted(exec_graph.job_vertices.values(), key=lambda job_vertex: job_vertex.id)
    worker_nodes = [
        WorkerNode(0, 5.0, 10),
        WorkerNode(1, 20.0, 5),
        WorkerNode(2, 10.0, 1),
    ]
    res, _ = round_robin_allocate(job_vertices, worker_nodes)

    assert res == {3: [0, 1, 2], 2: [1, 2], 1: [1, 1, 1, 1]}
