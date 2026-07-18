# SPDX-License-Identifier: Apache-2.0

import pytest

from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import (
    CheckpointTriggerOptions,
)
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.restart_strategy_options import RestartStrategyOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer
from ray.klein.runtime.partitioning import (
    AdaptivePartitioner,
    ForwardPartitioner,
    Partitioner,
    RescalePartitioner,
)
from tests.support.streaming import LoopSourceFunction
from tests.support.waiting import wait_until


class TestOperatorChaining:
    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path):
        self.config = Configuration()
        self.config.set(PipelineOptions.OPERATOR_CHAINING, True)
        self.config.set(RestartStrategyOptions.MAX_ATTEMPTS, 0)
        self.chk_path = tmp_path

    def assert_chaining_result(self, logical_graph, expected_op_name, expected_op_id):
        vertices = logical_graph.vertices
        assert len(vertices) == len(expected_op_name)
        assert {vertex_id.index for vertex_id in vertices} == set(expected_op_id)
        assert {vertex.name for vertex in vertices.values()} == set(expected_op_name)

    def test_operator_chaining_disabled(self):
        self.config.set(PipelineOptions.OPERATOR_CHAINING, False)
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner(), None),
                (1.0, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1]", "op2[2]", "op3[3]", "op4[4]"], [1, 2, 3, 4])
        ctx.execute("TEST").wait()

    def test_chain_into_one_op(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner(), None),
                (1.0, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1] -> op2[2] -> op3[3] -> op4[4]"], [1])
        ctx.execute("TEST").wait()

    def test_restore_from_chain_to_unchain(self):
        self.ut_restore_for_operator_chaining(chaining_conf=(True, False))

    def test_restore_from_unchain_to_chain(self):
        self.ut_restore_for_operator_chaining(chaining_conf=(False, True))

    def ut_restore_for_operator_chaining(self, chaining_conf):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner(), None),
                (1.0, 0.0, 1, None),
            ],
            record_num=-1,
        )
        ctx.config.set(PipelineOptions.OPERATOR_CHAINING, chaining_conf[0])
        ctx.config.set(CheckpointOptions.DIRECTORY, str(self.chk_path))
        ctx.config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        ctx.config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
        client: JobHandle = ctx.execute("TEST")
        wait_until(
            lambda: list(self.chk_path.rglob("_metadata")) or None,
            timeout=30,
            interval=0.1,
            description="operator checkpoint",
        )
        client.cancel(30)

        # Test Restore
        # Checkpoint retention can remove the first checkpoint while the
        # unbounded source is still running. Select from the completed set
        # after cancellation so the savepoint path cannot race cleanup.
        metadata_files = list(self.chk_path.rglob("_metadata"))
        assert metadata_files
        latest_metadata = max(metadata_files, key=lambda path: int(path.parent.name.removeprefix("chk-")))
        chk_path = str(latest_metadata.parent)
        _id, restored_states, _high_water = checkpoint_io.restore_checkpoint(chk_path)
        assert len(restored_states) == 1

        def validator(x):
            assert restored_states[0].state == x

        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, AdaptivePartitioner(), None),
                (1.0, 0.0, 1, None),
            ],
            record_num=1,
            restore_validator=validator,
        )
        ctx.config.set(PipelineOptions.OPERATOR_CHAINING, chaining_conf[1])
        ctx.config.set(CheckpointOptions.RESTORE_PATH, chk_path)
        ctx.execute().wait()

    def test_datasource_chaining(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner(), None),
                (1.1, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1] -> op2[2]", "op3[3] -> op4[4]"], [1, 3])
        ctx.execute("TEST").wait()

    def test_middle_chaining(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner(), None),
                (1.0, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1]", "op2[2] -> op3[3]", "op4[4]"], [1, 2, 4])
        ctx.execute("TEST").wait()

    def test_sink_chaining(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner()),
                (1.2, 0.0, 1, ForwardPartitioner(), None),
                (1.2, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1]", "op2[2]", "op3[3] -> op4[4]"], [1, 2, 3])
        ctx.execute("TEST").wait()

    def test_unchainable_caused_by_diff_parallelism(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, RescalePartitioner()),
                (1.0, 0.0, 2, RescalePartitioner()),
                (1.0, 0.0, 1, RescalePartitioner(), None),
                (1.0, 0.0, 2, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1]", "op2[2]", "op3[3]", "op4[4]"], [1, 2, 3, 4])
        ctx.execute("TEST").wait()

    def test_unchainable_caused_by_shuffle_partitioner(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, AdaptivePartitioner()),
                (1.0, 0.0, 1, AdaptivePartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner(), None),
                (1.0, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1]", "op2[2]", "op3[3] -> op4[4]"], [1, 2, 3])
        ctx.execute("TEST").wait()

    def test_unchainable_caused_by_batch_size(self):
        ctx = self.gen_single_sink_pipeline(
            [
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner(), 2),
                (1.0, 0.0, 1, None),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["op1[1] -> op2[2]", "op3[3]", "op4[4]"], [1, 3, 4])
        ctx.execute("TEST").wait()

    def test_multi_sink_operator_chaining_disabled(self):
        self.config.set(PipelineOptions.OPERATOR_CHAINING, False)
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 1, None),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(
            job_graph,
            ["source[1]", "map1[2]", "map2[3]", "sink1[4]", "sink2[5]"],
            [1, 2, 3, 4, 5],
        )
        ctx.execute("TEST").wait()

    def test_multi_sink_chain_into_one_op(self):
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 1, None),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["source[1] -> map1[2] -> map2[3] -> sink1[4], sink2[5]"], [1])
        ctx.execute("TEST").wait()

    def test_multi_sink_datasource_chaining(self):
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 1, None),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(
            job_graph,
            ["source[1] -> map1[2]", "map2[3] -> sink1[4]", "sink2[5]"],
            [1, 3, 5],
        )
        ctx.execute("TEST").wait()

    def test_multi_sink_no_chaining_caused_by_child_unchainable(self):
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 1, None),
                (1.1, 0.0, 1, ForwardPartitioner()),
                (1.1, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
                (1.0, 0.0, 1, ForwardPartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(
            job_graph,
            ["source[1]", "map1[2]", "map2[3]", "sink1[4]", "sink2[5]"],
            [1, 2, 3, 4, 5],
        )
        ctx.execute("TEST").wait()

    def test_multi_sink_sink_chaining(self):
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 1, None),
                (1.2, 0.0, 1, ForwardPartitioner()),
                (1.2, 0.0, 1, ForwardPartitioner()),
                (1.2, 0.0, 1, ForwardPartitioner()),
                (1.2, 0.0, 1, ForwardPartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(job_graph, ["source[1]", "map1[2] -> map2[3] -> sink1[4], sink2[5]"], [1, 2])
        ctx.execute("TEST").wait()

    def test_multi_sink_unchainable_caused_by_diff_parallelism(self):
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 1, None),
                (1.0, 0.0, 2, RescalePartitioner()),
                (1.0, 0.0, 1, RescalePartitioner()),
                (1.0, 0.0, 2, RescalePartitioner()),
                (1.0, 0.0, 2, RescalePartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(
            job_graph,
            ["source[1]", "map1[2]", "map2[3]", "sink1[4]", "sink2[5]"],
            [1, 2, 3, 4, 5],
        )
        ctx.execute("TEST").wait()

    def test_multi_sink_unchainable_caused_by_shuffle_partitioner(self):
        ctx = self.gen_multi_sink_pipeline(
            [
                (1.0, 0.0, 2, None),
                (1.0, 0.0, 2, ForwardPartitioner()),
                (1.0, 0.0, 2, AdaptivePartitioner()),
                (1.0, 0.0, 2, AdaptivePartitioner()),
                (1.0, 0.0, 2, AdaptivePartitioner()),
            ]
        )
        stream_graph = StreamGraph.from_sinks(ctx.sinks, "TEST", self.config)
        job_graph = LogicalOptimizer(self.config).optimize(stream_graph)
        self.assert_chaining_result(
            job_graph,
            ["source[1] -> map1[2]", "map2[3]", "sink1[4]", "sink2[5]"],
            [1, 3, 4, 5],
        )
        ctx.execute("TEST").wait()

    def gen_single_sink_pipeline(
        self,
        resources: list[tuple[float, float, int, Partitioner]],
        record_num=5,
        restore_validator=None,
    ) -> KleinContext:
        ctx = KleinContext(self.config)
        (
            ctx.source(
                LoopSourceFunction,
                fn_constructor_kwargs={
                    "record_num": record_num,
                    "restore_validator": restore_validator,
                },
                name="op1",
                num_cpus=resources[0][0],
                num_gpus=resources[0][1],
                concurrency=resources[0][2],
            )
            .partition_by(resources[0][3])
            .map(
                lambda x: x,
                name="op2",
                num_cpus=resources[1][0],
                num_gpus=resources[1][1],
                concurrency=resources[1][2],
            )
            .partition_by(resources[1][3])
            .map(
                lambda x: x,
                name="op3",
                num_cpus=resources[2][0],
                num_gpus=resources[2][1],
                concurrency=resources[2][2],
                batch_size=resources[2][4],
            )
            .partition_by(resources[2][3])
            .write(
                ConsoleSinkFunction,
                name="op4",
                num_cpus=resources[3][0],
                num_gpus=resources[3][1],
                concurrency=resources[3][2],
            )
        )
        return ctx

    def gen_multi_sink_pipeline(self, resources: list[tuple[float, float, int, Partitioner]]) -> KleinContext:
        ctx = KleinContext(self.config)
        stream = (
            ctx.source(
                LoopSourceFunction,
                fn_constructor_kwargs={"record_num": 5},
                name="source",
                num_cpus=resources[0][0],
                num_gpus=resources[0][1],
                concurrency=resources[0][2],
            )
            .partition_by(resources[1][3])
            .map(
                lambda x: x,
                name="map1",
                num_cpus=resources[1][0],
                num_gpus=resources[1][1],
                concurrency=resources[1][2],
            )
        )
        stream.partition_by(resources[2][3]).map(
            lambda x: x,
            name="map2",
            num_cpus=resources[2][0],
            num_gpus=resources[2][1],
            concurrency=resources[2][2],
        ).partition_by(resources[3][3]).write(
            ConsoleSinkFunction,
            name="sink1",
            num_cpus=resources[3][0],
            num_gpus=resources[3][1],
            concurrency=resources[3][2],
        )
        stream.partition_by(resources[4][3]).write(
            ConsoleSinkFunction,
            name="sink2",
            num_cpus=resources[4][0],
            num_gpus=resources[4][1],
            concurrency=resources[4][2],
        )
        return ctx
