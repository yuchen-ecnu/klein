# SPDX-License-Identifier: Apache-2.0

import pytest

import ray.klein as klein
from ray.klein.api.job_client import JobClient
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.api.stream_graph import StreamGraph
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.runtime.partitioning.broadcast_partitioner import BroadcastPartitioner


class MockLoopSourceFunction(SourceFunction):
    """
    TestLoopSourceFunction.
    """

    def __init__(self):
        self.idx: int = 0
        self._interrupted = False

    def run(self, context: SourceContext) -> None:
        pass

    def cancel(self) -> None:
        self._interrupted = True

    def snapshot_state(self, checkpoint_id: int) -> int:
        return self.idx

    def restore_state(self, state: int) -> None:
        self.idx = state

    def open(self, runtime_context: RuntimeContext) -> None:
        pass

    @staticmethod
    def is_bounded() -> bool:
        return False


class TestKleinContext:
    def test_from_values_validates_every_value(self) -> None:
        context = KleinContext()

        with pytest.raises(ValueError, match="at least one"):
            context.from_values()
        with pytest.raises(TypeError, match="index 1 is int"):
            context.from_values({"id": 1}, 2)

    def test_stream_lifecycle_functions_must_be_interface_classes(self) -> None:
        context = KleinContext()

        with pytest.raises(TypeError, match="SourceFunction class"):
            context.source(object)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="SourceFunction class"):
            context.source(MockLoopSourceFunction())  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="SinkFunction class"):
            context.from_values({"id": 1}).write(object)  # type: ignore[arg-type]

    def test_broadcast_selects_the_broadcast_partitioner(self) -> None:
        stream = KleinContext().from_values({"id": 1})

        assert stream.broadcast() is stream
        assert isinstance(stream.partitioner, BroadcastPartitioner)

    def test_take_requires_interactive_mode(self) -> None:
        stream = KleinContext().from_values({"id": 1})

        with pytest.raises(RuntimeError, match="interactive"):
            stream.take()

    def test_global_context_configuration_and_top_level_read_api(self) -> None:
        context = KleinContext.reset("execution.runtime.mode=batch")

        stream = klein.from_items([{"id": 1}])

        assert stream.context is context
        assert klein.current_context() is context
        assert context.config.get(ExecutionOptions.MODE) is RuntimeExecutionMode.BATCH

    def test_module_reader_matches_ray_data_style(self) -> None:
        context = KleinContext.reset({"state.backend.type": "memory"})

        stream = klein.read_csv("mock_src.csv")

        assert stream.context is context
        assert stream.name == "RayData.read_csv"

    def test_runtime_context_auto_detection_stream(self) -> None:
        config = Configuration()
        ctx = KleinContext(config)
        stream = ctx.from_values({"id": 1}, {"id": 2}, {"id": 3}).map(lambda x: {"id": x["id"] * x["id"]})
        stream.write(ConsoleSinkFunction)
        sg = StreamGraph.from_sinks(ctx.sinks, "test_auto_detection_stream", config)
        mode = JobClient._determine_runtime_mode(sg)
        assert mode is RuntimeExecutionMode.STREAMING

    def test_runtime_context_auto_detection_batch(self) -> None:
        config = Configuration()
        ctx = KleinContext(config)
        stream = ctx.data.read_csv("mock_src.csv").map(lambda x: {"id": x["id"] * x["id"]})
        stream.data.write_csv("mock_dst.csv")
        sg = StreamGraph.from_sinks(ctx.sinks, "test_auto_detection_batch", config)
        mode = JobClient._determine_runtime_mode(sg)

        assert mode is RuntimeExecutionMode.BATCH
