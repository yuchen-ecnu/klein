# SPDX-License-Identifier: Apache-2.0

import pytest

import ray.klein as klein
from ray.klein.api.job_client import JobClient
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.api.stream_sink import StreamSink
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.runtime.graph.logical_graph import LogicalGraph
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

    def test_take_builds_a_lazy_terminal_sink(self) -> None:
        stream = KleinContext().from_values({"id": 1})

        assert isinstance(stream.take(), StreamSink)

    def test_top_level_execute_accepts_explicit_sink_roots(self, monkeypatch) -> None:
        context = KleinContext()
        selected = context.from_values({"id": 1}).take_all()
        pending = context.from_values({"id": 2}).show()
        sentinel = object()
        captured = {}

        def execute(_client, job_name, sinks):
            captured["job_name"] = job_name
            captured["sinks"] = tuple(sinks)
            return sentinel

        monkeypatch.setattr(JobClient, "execute", execute)

        assert klein.execute("selected-job", sinks=(selected,)) is sentinel
        assert captured == {"job_name": "selected-job", "sinks": (selected,)}
        assert context.sinks == (pending,)

    def test_top_level_execute_submits_all_pending_side_effect_sinks(self, monkeypatch) -> None:
        context = KleinContext.reset()
        first = context.from_values({"id": 1}).show()
        second = context.from_values({"id": 2}).show()
        captured = {}

        def execute(_client, job_name, sinks):
            captured["job_name"] = job_name
            captured["sinks"] = tuple(sinks)
            return object()

        monkeypatch.setattr(JobClient, "execute", execute)

        klein.execute("multi-sink")

        assert captured == {"job_name": "multi-sink", "sinks": (first, second)}
        assert context.sinks == ()

    def test_collecting_sink_must_be_executed_alone(self) -> None:
        context = KleinContext.reset()
        collected = context.from_values({"id": 1}).take_all()
        side_effect = context.from_values({"id": 2}).show()

        with pytest.raises(ValueError, match="cannot be combined"):
            klein.execute("ambiguous-results")
        assert context.sinks == (collected, side_effect)

    def test_ray_data_collecting_sink_must_be_executed_alone(self) -> None:
        context = KleinContext.reset()
        collected = context.from_values({"id": 1}).data.take_all()
        side_effect = context.from_values({"id": 2}).show()

        with pytest.raises(ValueError, match="cannot be combined"):
            klein.execute("ambiguous-ray-data-results")
        assert context.sinks == (collected, side_effect)

    def test_multiple_collecting_sinks_are_rejected_before_submission(self, monkeypatch) -> None:
        context = KleinContext.reset()
        first = context.from_values({"id": 1}).take()
        second = context.from_values({"id": 2}).take_all()

        def unexpected_execute(*_args, **_kwargs):
            raise AssertionError("submission should not be attempted")

        monkeypatch.setattr(JobClient, "execute", unexpected_execute)

        with pytest.raises(ValueError, match="only one"):
            klein.execute("ambiguous-results")

        assert context.sinks == (first, second)

    def test_top_level_execute_rejects_sinks_from_different_pipelines(self) -> None:
        first = KleinContext().from_values({"id": 1}).show()
        second = KleinContext().from_values({"id": 2}).show()

        with pytest.raises(ValueError, match="same Klein pipeline"):
            klein.execute(sinks=(first, second))

    def test_explicit_sink_must_be_pending_and_unique(self, monkeypatch) -> None:
        context = KleinContext()
        sink = context.from_values({"id": 1}).show()

        monkeypatch.setattr(JobClient, "execute", lambda *_args, **_kwargs: object())

        with pytest.raises(ValueError, match="only once"):
            context.execute("duplicate", sinks=(sink, sink))
        context.execute("first", sinks=(sink,))
        with pytest.raises(ValueError, match="still be pending"):
            context.execute("again", sinks=(sink,))

    def test_sink_token_is_not_a_positional_job_name(self) -> None:
        context = KleinContext.reset()
        sink = context.from_values({"id": 1}).show()

        with pytest.raises(TypeError, match="job_name"):
            klein.execute(sink)  # type: ignore[arg-type]
        assert context.sinks == (sink,)

    def test_sink_cannot_be_submitted_twice_concurrently(self, monkeypatch) -> None:
        context = KleinContext()
        sink = context.from_values({"id": 1}).show()

        def execute(_client, _job_name, _sinks):
            with pytest.raises(RuntimeError, match="already being submitted"):
                context.execute("nested", sinks=(sink,))
            return object()

        monkeypatch.setattr(JobClient, "execute", execute)

        context.execute("outer", sinks=(sink,))

    def test_failed_submission_keeps_the_selected_sink_pending(self, monkeypatch) -> None:
        context = KleinContext()
        sink = context.from_values({"id": 1}).show()
        sentinel = object()
        attempts = 0

        def fail(_client, _job_name, _sinks):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("submission failed")
            return sentinel

        monkeypatch.setattr(JobClient, "execute", fail)

        with pytest.raises(RuntimeError, match="submission failed"):
            klein.execute(sinks=(sink,))
        assert context.sinks == (sink,)
        assert klein.execute(sinks=(sink,)) is sentinel
        assert context.sinks == ()

    def test_global_context_configuration_and_top_level_read_api(self) -> None:
        context = KleinContext.reset("execution.runtime.mode=batch")

        stream = klein.from_items([{"id": 1}])

        assert stream.context is context
        assert klein.current_context() is context
        assert context.config.get(ExecutionOptions.MODE) is RuntimeExecutionMode.BATCH

    def test_top_level_configuration_does_not_expose_the_pipeline_context(self) -> None:
        context = KleinContext.reset()

        configured = klein.configure({"state.backend.type": "memory"})

        assert configured is context.config
        assert klein.get_config() is configured

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
        sg = LogicalGraph.from_sinks(ctx.sinks, "test_auto_detection_stream", config)
        mode = JobClient._determine_runtime_mode(sg)
        assert mode is RuntimeExecutionMode.STREAMING

    def test_runtime_context_auto_detection_batch(self) -> None:
        config = Configuration()
        ctx = KleinContext(config)
        stream = ctx.data.read_csv("mock_src.csv").map(lambda x: {"id": x["id"] * x["id"]})
        stream.data.write_csv("mock_dst.csv")
        sg = LogicalGraph.from_sinks(ctx.sinks, "test_auto_detection_batch", config)
        mode = JobClient._determine_runtime_mode(sg)

        assert mode is RuntimeExecutionMode.BATCH
