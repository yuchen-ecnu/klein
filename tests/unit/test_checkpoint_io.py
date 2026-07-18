# SPDX-License-Identifier: Apache-2.0
import pickle
import tempfile
from datetime import timedelta
from pathlib import Path
from unittest import TestCase

import numpy

from ray.klein.api.klein_context import KleinContext
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.config.checkpoint_trigger_options import (
    CheckpointTriggerOptions,
)
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.graph.logical_optimizer import LogicalOptimizer
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem
from ray.klein.state.checkpoint_layout import CheckpointLayout
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry
from tests.support.execution_graph import expand_execution_graph
from tests.support.streaming import LoopSourceFunction, flat_map_identity


class CheckpointIOTest(TestCase):
    """Checkpoint topology and persistence tests."""

    def setUp(self):
        super().setUp()

    def test_get_num_of_aligning_barriers_origin(self):
        ctx = KleinContext()
        stream = (
            ctx.from_values({"id": 1}, {"id": 2}, {"id": 3})
            .map(lambda x: {"id": x["id"] * x["id"]}, concurrency=2)
            .map(lambda x: {"id": x["id"] * x["id"]}, concurrency=3)
        )
        stream.write(ConsoleSinkFunction, concurrency=2)
        stream.write(ConsoleSinkFunction, concurrency=3)
        # Translate DataStream to JobGraph
        StreamGraph.from_sinks(ctx.sinks, "test_job_name", Configuration())
        # JobGraph:
        # Source(concurrency=1) -[Rescale]--> Map(concurrency=2)
        #                       -[Adaptive]-> Map(concurrency=3)--[Adaptive]-> Sink(concurrency=2)
        #                                                       \-[Forward]--> Sink(concurrency=3)
        exec_graph = self._to_exec_graph(ctx.sinks)

        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source = ExecutionVertexId(1, 0)
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 1)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 2)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 0)], {source: 3})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 1)], {source: 3})
        self.assertEqual(ev_aligns[ExecutionVertexId(5, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(5, 1)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(5, 2)], {source: 1})
        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source: 5},
        )

    def test_get_num_of_aligning_barriers_multiple_partitioner(self):
        ctx = KleinContext()
        stream = (
            ctx.from_values({"id": 1}, {"id": 2}, {"id": 3})
            .map(lambda x: {"id": x["id"] * x["id"]}, concurrency=2)
            .map(lambda x: {"id": x["id"] * x["id"]}, concurrency=3)
            .rescale()
            .map(lambda x: {"id": x["id"] * x["id"]}, concurrency=4)
        )
        stream.write(ConsoleSinkFunction, concurrency=2)
        stream.write(ConsoleSinkFunction, concurrency=3)
        # JobGraph:
        # Source(concurrency=1) -[Rescale]--> Map(concurrency=2)
        #                       -[Adaptive]-> Map(concurrency=3)
        #                       -[Rescale]--> Map(concurrency=4)--[Rescale]--> Sink(concurrency=2)
        #                                                       \-[Adaptive]-> Sink(concurrency=3)
        exec_graph = self._to_exec_graph(ctx.sinks)

        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source = ExecutionVertexId(1, 0)
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 1)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 2)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 1)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 2)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 3)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(5, 0)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(5, 1)], {source: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(6, 0)], {source: 4})
        self.assertEqual(ev_aligns[ExecutionVertexId(6, 1)], {source: 4})
        self.assertEqual(ev_aligns[ExecutionVertexId(6, 2)], {source: 4})
        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source: 5},
        )

    def test_barrier_split_counts_parallelism_source_single_sink(self) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)
        stream = (
            ctx.source(LoopSourceFunction, num_cpus=0.5, concurrency=2)  # 1C
            .map(  # 3C
                lambda x: {"idx": numpy.array(x["idx"]) * 2},
                num_cpus=1.5,
                concurrency=2,
                batch_size=2,
                name="MapOperator",
            )
            .rescale()
            .flat_map(flat_map_identity, num_cpus=0.5, concurrency=3)
        )
        # 1C
        stream.rescale().write(ConsoleSinkFunction, num_cpus=0.25, concurrency=1, name="ConsoleSink")
        # JobGraph:
        # Source(concurrency=2) -[Forward]-> Map(concurrency=2)
        #                       -[Rescale]-> Flat_Map(concurrency=3) -[Rescale]-> Sink(concurrency=1)
        exec_graph = self._to_exec_graph(ctx.sinks)
        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source1 = ExecutionVertexId(1, 0)
        source2 = ExecutionVertexId(1, 1)
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 2)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 0)], {source1: 2, source2: 1})
        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source1: 1, source2: 1},
        )

    def test_barrier_split_counts_single_source_single_sink(self) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)
        stream = (
            ctx.source(LoopSourceFunction, num_cpus=0.5, concurrency=1)  # 1C
            .map(  # 3C
                lambda x: {"idx": numpy.array(x["idx"]) * 2},
                num_cpus=1.5,
                concurrency=2,
                batch_size=2,
                name="MapOperator",
            )
            .rescale()
            .flat_map(flat_map_identity, num_cpus=0.5, concurrency=3)
        )
        # 1C
        stream.rescale().write(ConsoleSinkFunction, num_cpus=0.25, concurrency=1, name="ConsoleSink")
        # Translate DataStream to JobGraph
        exec_graph = self._to_exec_graph(ctx.sinks)
        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source = ExecutionVertexId(1, 0)
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 1)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 2)], {source: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 0)], {source: 3})
        self.assertEqual(checkpoint_io.coordinator_ack_counts(exec_graph), {source: 1})

    def test_barrier_split_counts_compaction(self) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)
        stream = (
            ctx.source(LoopSourceFunction, num_cpus=0.5, concurrency=2)  # 1C
            .map(  # 3C
                lambda x: {"idx": numpy.array(x["idx"]) * 2},
                num_cpus=1.5,
                concurrency=4,
                batch_size=2,
                name="MapOperator",
            )
            .flat_map(flat_map_identity, num_cpus=0.5, concurrency=1)
            .map(lambda x: {"0": x["0"] * x["0"]}, concurrency=1)
        )
        # 1C
        stream.write(ConsoleSinkFunction, num_cpus=0.25, concurrency=1, name="ConsoleSink")
        # JobGraph:
        # Source(concurrency=2) -[Rescale]-> Map(concurrency=4)
        #                       -[Rescale]-> Flat_Map(concurrency=1)
        #                       -[Forward]-> Map(concurrency=1) -[Forward]-> Sink(concurrency=1)
        exec_graph = self._to_exec_graph(ctx.sinks)
        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source1 = ExecutionVertexId(1, 0)
        source2 = ExecutionVertexId(1, 1)
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(1, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 2)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 3)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source1: 2, source2: 2})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 0)], {source1: 1, source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(5, 0)], {source1: 1, source2: 1})
        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source1: 1, source2: 1},
        )

    def test_barrier_split_counts_multiple_sink(self) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)
        stream = (
            ctx.source(LoopSourceFunction, concurrency=5)
            .map(
                lambda x: {"idx": numpy.array(x["idx"]) * 2},
                concurrency=2,
                name="MapOperator",
            )
            .flat_map(flat_map_identity, concurrency=2)
        )
        stream.write(ConsoleSinkFunction, concurrency=2, name="ConsoleSink")
        stream.show()
        # JobGraph:
        # Source(concurrency=5) -[Adaptive]-> Map(concurrency=2)
        #                       -[Forward]--> Flat_Map(concurrency=2)--[Forward]-> Sink(concurrency=2)
        #                                                            \-[Rescale]-> Show(concurrency=1)
        exec_graph = self._to_exec_graph(ctx.sinks)
        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source1 = ExecutionVertexId(1, 0)
        source2 = ExecutionVertexId(1, 1)
        source3 = ExecutionVertexId(1, 2)
        source4 = ExecutionVertexId(1, 3)
        source5 = ExecutionVertexId(1, 4)
        self.assertEqual(
            ev_aligns[ExecutionVertexId(2, 0)],
            {source1: 1, source2: 1, source3: 1, source4: 1, source5: 1},
        )
        self.assertEqual(
            ev_aligns[ExecutionVertexId(2, 1)],
            {source1: 1, source2: 1, source3: 1, source4: 1, source5: 1},
        )
        self.assertEqual(
            ev_aligns[ExecutionVertexId(3, 0)],
            {source1: 1, source2: 1, source3: 1, source4: 1, source5: 1},
        )
        self.assertEqual(
            ev_aligns[ExecutionVertexId(3, 1)],
            {source1: 1, source2: 1, source3: 1, source4: 1, source5: 1},
        )
        self.assertEqual(
            ev_aligns[ExecutionVertexId(4, 0)],
            {source1: 1, source2: 1, source3: 1, source4: 1, source5: 1},
        )
        self.assertEqual(
            ev_aligns[ExecutionVertexId(4, 1)],
            {source1: 1, source2: 1, source3: 1, source4: 1, source5: 1},
        )
        self.assertEqual(
            ev_aligns[ExecutionVertexId(5, 0)],
            {source1: 2, source2: 2, source3: 2, source4: 2, source5: 2},
        )
        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source1: 3, source2: 3, source3: 3, source4: 3, source5: 3},
        )

    def test_barrier_split_counts_with_multiple_group(self) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)
        stream = ctx.source(LoopSourceFunction, concurrency=2).map(
            lambda x: {"idx": numpy.array(x["idx"]) * 2},
            concurrency=6,
            name="MapOperator",
        )
        stream.write(ConsoleSinkFunction, concurrency=2, name="ConsoleSink")
        # JobGraph:
        # Source(concurrency=2) -[Rescale]-> Map(concurrency=6) -[Rescale]-> Sink(concurrency=2)
        exec_graph = self._to_exec_graph(ctx.sinks)
        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source1 = ExecutionVertexId(1, 0)
        source2 = ExecutionVertexId(1, 1)
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 2)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 3)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 4)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 5)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source1: 3})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 1)], {source2: 3})

        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source1: 1, source2: 1},
        )

    def test_barrier_split_counts_with_multiple_group_and_multiple_sink(
        self,
    ) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)
        stream = ctx.source(LoopSourceFunction, concurrency=2).map(
            lambda x: {"idx": numpy.array(x["idx"]) * 2},
            concurrency=6,
            name="MapOperator",
        )
        stream.write(ConsoleSinkFunction, concurrency=2, name="ConsoleSink")
        stream.show()
        # JobGraph:
        # Source(concurrency=2) -[Rescale]-> Map(concurrency=6)--[Rescale]-> Sink(concurrency=2)
        #                                                      \-[Rescale]-> Show(concurrency=1)
        exec_graph = self._to_exec_graph(ctx.sinks)
        ev_aligns = checkpoint_io.barrier_split_counts(exec_graph)
        source1 = ExecutionVertexId(1, 0)
        source2 = ExecutionVertexId(1, 1)
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 0)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 1)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 2)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 3)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 4)], {source1: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(2, 5)], {source2: 1})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 0)], {source1: 3})
        self.assertEqual(ev_aligns[ExecutionVertexId(3, 1)], {source2: 3})
        self.assertEqual(ev_aligns[ExecutionVertexId(4, 0)], {source1: 3, source2: 3})

        self.assertEqual(
            checkpoint_io.coordinator_ack_counts(exec_graph),
            {source1: 2, source2: 2},
        )

    def test_write_source_state_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            chk_dir = Path(temp_dir)
            source_states = [
                SourceCheckpointEntry("11:0", 1, {"offset": 1}),
                SourceCheckpointEntry("12:0", 1, {"offset": 2}),
                SourceCheckpointEntry("21:0", 1, {"offset": 3}),
            ]

            state_path = checkpoint_io.write_checkpoint(
                source_states,
                1,
                str(chk_dir),
                barrier_high_water=42,
                job_id="job/one",
            )

            chk_path = chk_dir / "job%2Fone" / "chk-1"
            self.assertEqual(state_path, str(chk_path))
            self.assertTrue(Path(state_path).exists())

            _id, restored_states, high_water = checkpoint_io.restore_checkpoint(str(chk_path))
            self.assertListEqual(source_states, restored_states)
            self.assertEqual(high_water, 42)

    def test_latest_checkpoint_falls_back_from_corrupt_pointer_and_retains_latest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root_uri = Path(temp_dir).as_uri()
            source_states = [SourceCheckpointEntry("11:0", 1, {"offset": 1})]
            for checkpoint_id in (1, 2, 3):
                checkpoint_io.write_checkpoint(
                    source_states,
                    checkpoint_id,
                    root_uri,
                    barrier_high_water=checkpoint_id,
                    job_id="job-a",
                )

            filesystem = CheckpointFileSystem(root_uri)
            layout = CheckpointLayout("job-a")
            filesystem.write_bytes(layout.latest_pointer, b"not-a-pickle", atomic=True)
            filesystem.create_dir(layout.checkpoint_directory(99))

            latest = checkpoint_io.latest_checkpoint(root_uri, "job-a")

            self.assertEqual(latest, f"{root_uri}/job-a/chk-3")
            checkpoint_io.cleanup_checkpoints(root_uri, "job-a", retained_count=2)
            self.assertTupleEqual(
                checkpoint_io.list_completed_checkpoints(root_uri, "job-a"),
                (2, 3),
            )
            self.assertTrue(filesystem.exists(layout.checkpoint_directory(99)))

    def test_explicit_restore_rejects_corrupt_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint = Path(temp_dir) / "chk-1"
            checkpoint.mkdir()
            (checkpoint / "_metadata").write_bytes(b"not-a-checkpoint")

            with self.assertRaises(pickle.UnpicklingError):
                checkpoint_io.restore_checkpoint(str(checkpoint))

            with self.assertRaises(pickle.UnpicklingError):
                checkpoint_io.restore_operator_state_entries(str(checkpoint))

    def test_restore_rejects_pre_revision_checkpoint_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            checkpoint = Path(temp_dir) / "chk-1"
            checkpoint.mkdir()
            (checkpoint / "_metadata").write_bytes(
                pickle.dumps(
                    {
                        "version": 2,
                        "snapshot_id": 1,
                        "source_states": (),
                        "barrier_high_water": 0,
                        "operator_states": (),
                    }
                )
            )

            with self.assertRaisesRegex(ValueError, "unsupported checkpoint format version"):
                checkpoint_io.restore_checkpoint(str(checkpoint))

    @staticmethod
    def _to_exec_graph(sinks) -> ExecutionGraph:
        stream_graph = StreamGraph.from_sinks(sinks, "test_job_name", Configuration())
        config = Configuration()
        config.set(PipelineOptions.OPERATOR_CHAINING, False)
        job_graph = LogicalOptimizer(config=config).optimize(stream_graph)
        return expand_execution_graph(job_graph)
