# SPDX-License-Identifier: Apache-2.0

import copy
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

import ray.klein as klein
from ray.klein.api.completed_job_handle import CompletedJobHandle
from ray.klein.api.job_client import JobClient
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.pipeline import Pipeline
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.api.statement_set import StatementSet
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
    def test_default_context_is_isolated_between_threads(self) -> None:
        main_context = KleinContext.reset()
        rendezvous = Barrier(2)

        def current_context(_index: int) -> KleinContext:
            rendezvous.wait()
            return KleinContext.current()

        with ThreadPoolExecutor(max_workers=2) as executor:
            worker_contexts = tuple(executor.map(current_context, range(2)))

        assert worker_contexts[0] is not worker_contexts[1]
        assert main_context not in worker_contexts

    def test_deepcopy_rebuilds_the_process_local_lock(self, context) -> None:
        cloned = copy.deepcopy(context)

        assert cloned is not context
        assert cloned._lock is not context._lock
        assert cloned.config.to_dict() == context.config.to_dict()

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

    def test_pipeline_collect_submits_single_sink_immediately(self, monkeypatch) -> None:
        pipeline = Pipeline(name="fluent-collect")
        captured = {}

        def execute(_client, job_name, sinks):
            captured.update(job_name=job_name, sinks=tuple(sinks))
            return CompletedJobHandle([{"id": 1}])

        monkeypatch.setattr(JobClient, "execute", execute)

        handle = pipeline.from_values({"id": 1}).collect()
        rows = handle.result()

        assert rows == [{"id": 1}]
        assert captured["job_name"] == "fluent-collect"
        assert len(captured["sinks"]) == 1
        assert isinstance(captured["sinks"][0], StreamSink)
        assert pipeline.sinks == ()

    def test_legacy_terminal_result_submits_only_that_sink(self, monkeypatch) -> None:
        context = KleinContext()
        selected = context.from_values({"id": 1}).collect()
        pending = context.from_values({"id": 2}).show()
        captured = {}

        def execute(_client, job_name, sinks):
            captured.update(job_name=job_name, sinks=tuple(sinks))
            return CompletedJobHandle([{"id": 1}])

        monkeypatch.setattr(JobClient, "execute", execute)

        assert selected.result("selected") == [{"id": 1}]
        assert captured == {"job_name": "selected", "sinks": (selected,)}
        assert context.sinks == (pending,)

    def test_pipeline_side_effect_terminal_submits_immediately(self, monkeypatch) -> None:
        pipeline = Pipeline(name="show-one")
        calls = []
        monkeypatch.setattr(
            JobClient,
            "execute",
            lambda _client, job_name, sinks: calls.append((job_name, tuple(sinks))) or CompletedJobHandle(None),
        )

        handle = pipeline.from_values({"id": 1}).show()

        assert handle.status.name == "FINISHED"
        assert len(calls) == 1
        assert calls[0][0] == "show-one"
        assert pipeline.sinks == ()

    def test_statement_set_defers_and_submits_multiple_sinks_once(self, monkeypatch) -> None:
        pipeline = Pipeline(name="fanout")
        first = pipeline.from_values({"id": 1})
        second = pipeline.from_values({"id": 2})
        statements = pipeline.create_statement_set()
        calls = []

        def execute(_client, job_name, sinks):
            calls.append((job_name, tuple(sinks)))
            return CompletedJobHandle(None)

        monkeypatch.setattr(JobClient, "execute", execute)

        statements.add(first.show).add(second.show)

        assert calls == []
        assert len(statements.sinks) == 2
        handle = statements.execute()

        assert handle.status.name == "FINISHED"
        assert len(calls) == 1
        assert calls[0][0] == "fanout"
        assert len(calls[0][1]) == 2
        assert statements.sinks == ()
        assert pipeline.sinks == ()

    def test_statement_set_context_captures_writes(self, monkeypatch) -> None:
        pipeline = Pipeline()
        statements = pipeline.create_statement_set()
        calls = []
        monkeypatch.setattr(
            JobClient,
            "execute",
            lambda _client, job_name, sinks: calls.append((job_name, tuple(sinks))) or CompletedJobHandle(None),
        )

        with statements:
            pipeline.from_values({"id": 1}).show()
            pipeline.from_values({"id": 2}).show()

        assert calls == []
        assert len(statements.sinks) == 2
        statements.execute("context-fanout")
        assert calls[0][0] == "context-fanout"
        assert len(calls[0][1]) == 2

    def test_statement_set_owned_sink_cannot_bypass_the_set(self, monkeypatch) -> None:
        pipeline = Pipeline(name="fanout")
        statements = pipeline.create_statement_set()
        statements.add(pipeline.from_values({"id": 1}).show)
        sink = statements.sinks[0]
        calls = []
        monkeypatch.setattr(
            JobClient,
            "execute",
            lambda _client, job_name, sinks: calls.append((job_name, tuple(sinks))) or CompletedJobHandle(None),
        )

        with pytest.raises(RuntimeError, match="through that StatementSet"):
            pipeline.execute()
        with pytest.raises(RuntimeError, match="through that StatementSet"):
            sink.run()

        statements.execute()
        assert len(calls) == 1

    def test_statement_set_failed_submission_remains_retryable(self, monkeypatch) -> None:
        pipeline = Pipeline(name="retry-fanout")
        statements = pipeline.create_statement_set()
        statements.add(pipeline.from_values({"id": 1}).show)
        attempts = 0

        def execute(_client, _job_name, _sinks):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("submission failed")
            return CompletedJobHandle(None)

        monkeypatch.setattr(JobClient, "execute", execute)

        with pytest.raises(RuntimeError, match="submission failed"):
            statements.execute()
        assert len(statements.sinks) == 1

        statements.execute()
        assert attempts == 2
        assert statements.sinks == ()
        assert pipeline.sinks == ()

    def test_statement_set_rolls_back_context_on_construction_failure(self) -> None:
        pipeline = Pipeline()
        statements = pipeline.create_statement_set()

        with pytest.raises(RuntimeError, match="builder failed"), statements:
            pipeline.from_values({"id": 1}).show()
            raise RuntimeError("builder failed")

        assert statements.sinks == ()
        assert pipeline.sinks == ()

    def test_sink_cannot_belong_to_two_statement_sets(self) -> None:
        pipeline = Pipeline()
        first = pipeline.create_statement_set()
        second = pipeline.create_statement_set()
        first.add(pipeline.from_values({"id": 1}).show)

        with pytest.raises(ValueError, match="already belongs"):
            second.add_sink(first.sinks[0])

        assert len(first.sinks) == 1
        assert second.sinks == ()

    def test_statement_set_rejects_collecting_terminals_without_leaking_them(self) -> None:
        pipeline = Pipeline()
        statements = pipeline.create_statement_set()

        with pytest.raises(ValueError, match="side-effect sinks"):
            statements.add(pipeline.from_values({"id": 1}).collect)

        assert statements.sinks == ()
        assert pipeline.sinks == ()

    def test_pipeline_ray_data_consumer_submits_single_sink(self, monkeypatch) -> None:
        pipeline = Pipeline(name="ray-data-take")
        captured = {}

        def execute(_client, job_name, sinks):
            captured.update(job_name=job_name, sinks=tuple(sinks))
            return CompletedJobHandle([{"id": 1}])

        monkeypatch.setattr(JobClient, "execute", execute)

        rows = pipeline.from_values({"id": 1}).ray_data.take_all().result()

        assert rows == [{"id": 1}]
        assert captured["job_name"] == "ray-data-take"
        assert len(captured["sinks"]) == 1

    def test_pipeline_uses_strict_configuration_by_default(self) -> None:
        with pytest.raises(ValueError, match="unknown Klein configuration option"):
            Pipeline({"execution.runtime.mod": "streaming"})

        pipeline = Pipeline({"execution.runtime.mode": "batch"})
        assert pipeline.config.strict is True

    @pytest.mark.parametrize("name", ["", "   "])
    def test_pipeline_rejects_empty_job_name(self, name) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            Pipeline(name=name)

        with pytest.raises(TypeError, match="string or None"):
            Pipeline(name=1)  # type: ignore[arg-type]

    def test_statement_set_requires_a_pipeline(self) -> None:
        with pytest.raises(TypeError, match="must be a Pipeline"):
            StatementSet(KleinContext())  # type: ignore[arg-type]

    def test_pipeline_can_opt_into_application_metadata(self) -> None:
        pipeline = Pipeline({"application.owner": "analytics"}, strict_config=False)

        assert pipeline.config.to_dict() == {"application.owner": "analytics"}

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
