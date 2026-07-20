# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import timedelta

import pytest

from ray.klein.api.changelog_row import ChangelogRow
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.missing_data_strategy import MissingDataStrategy
from ray.klein.api.row_kind import RowKind
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.api.tumbling_window import TumblingWindow
from ray.klein.runtime.operator.batch_process_operator import BatchProcessOperator
from ray.klein.runtime.operator.filter_operator import FilterOperator
from ray.klein.runtime.operator.flat_map_operator import FlatMapOperator
from ray.klein.runtime.operator.flat_map_with_rank_operator import FlatMapWithRankOperator
from ray.klein.runtime.operator.interval_join_operator import IntervalJoinOperator
from ray.klein.runtime.operator.keyed_process_operator import KeyedProcessOperator
from ray.klein.runtime.operator.map_operator import MapOperator
from ray.klein.runtime.operator.reduce_operator import ReduceOperator
from ray.klein.runtime.operator.sink import SinkOperator
from ray.klein.runtime.operator.source import SourceFunctionOperator
from ray.klein.runtime.operator.window_operator import WindowOperator
from ray.klein.runtime.partitioning.adaptive_partitioner import AdaptivePartitioner
from ray.klein.runtime.partitioning.broadcast_partitioner import BroadcastPartitioner
from ray.klein.runtime.partitioning.key_partitioner import KeyPartitioner
from ray.klein.runtime.partitioning.rescale_partitioner import RescalePartitioner
from ray.klein.runtime.partitioning.round_robin_partitioner import RoundRobinPartitioner
from ray.klein.runtime.partitioning.simple_partitioner import SimplePartitioner
from ray.klein.runtime.resources import Resources


class _ConfiguredCallable:
    def __init__(self, prefix: str, *, scale: int) -> None:
        self.prefix = prefix
        self.scale = scale

    def __call__(self, value):
        return value


class _ConfiguredStage:
    def __init__(self, stage: str, **options) -> None:
        self.stage = stage
        self.options = options

    def __call__(self, value):
        return [value] if self.stage == "pre" else value


class _ConfiguredSource(SourceFunction):
    def __init__(self, values: list[dict], *, label: str) -> None:
        self.values = values
        self.label = label

    def run(self, context: SourceContext) -> None:
        for value in self.values:
            context.collect(value)

    def cancel(self) -> None:
        return None

    def snapshot_state(self, checkpoint_id: int):
        return checkpoint_id

    def restore_state(self, state) -> None:
        self.restored_state = state


class _ConfiguredSink(SinkFunction):
    def __init__(self, target: list[dict], *, label: str) -> None:
        self.target = target
        self.label = label

    def open(self, runtime_context: RuntimeContext) -> None:
        self.runtime_context = runtime_context

    def write(self, value) -> None:
        self.target.append(value)

    def flush(self) -> None:
        return None


def _key(value):
    return value["key"]


def _timestamp(value):
    return value["timestamp"]


def _process(value, _context):
    return value


@pytest.mark.parametrize(
    ("method", "operator_class", "batch_format"),
    [
        ("map", MapOperator, "default"),
        ("map_batches", MapOperator, "pandas"),
        ("flat_map", FlatMapOperator, "default"),
        ("filter", FilterOperator, "default"),
    ],
)
def test_elementwise_api_forwards_runtime_and_resource_parameters(method, operator_class, batch_format) -> None:
    source = KleinContext().from_values({"key": "a", "value": 1})
    options = {
        "fn_constructor_args": ["configured"],
        "fn_constructor_kwargs": {"scale": 3},
        "num_cpus": 0.25,
        "num_gpus": 0.5,
        "concurrency": (2, 4),
        "batch_size": 7,
        "batch_timeout": timedelta(seconds=5),
        "name": "ConfiguredOperator",
        "ray_serve_enabled": True,
        "async_buffer_size": 9,
    }
    if method == "map_batches":
        options["batch_format"] = batch_format

    stream = getattr(source, method)(_ConfiguredCallable, **options)
    function = stream.stream_operator.logical_function

    assert isinstance(stream.stream_operator, operator_class)
    assert stream.name == "ConfiguredOperator"
    assert stream.ray_serve_enabled is True
    assert stream.resources == Resources(0.25, 0.5, (2, 4))
    assert stream.concurrency == (2, 4)
    assert function is not None
    assert function.function is _ConfiguredCallable
    assert function.constructor_args == ("configured",)
    assert function.constructor_kwargs == {"scale": 3}
    assert function.runtime_info == RuntimeInfo(
        batch_size=7,
        batch_timeout=5,
        batch_format=batch_format,
        async_buffer_size=9,
    )
    # An async execution window is a native-streaming semantic even though a
    # Ray Data lowering remains available for an explicit batch override.
    assert function.batch_supported is False
    assert function.batch_lowering is not None


def test_source_and_sink_api_forward_lifecycle_parameters() -> None:
    context = KleinContext()
    source = context.source(
        _ConfiguredSource,
        fn_constructor_args=[[{"key": "a"}]],
        fn_constructor_kwargs={"label": "input"},
        num_cpus=0.2,
        num_gpus=0.3,
        concurrency=(1, 3),
        name="ConfiguredSource",
        bounded=True,
    )
    target: list[dict] = []
    sink = source.write(
        _ConfiguredSink,
        fn_constructor_args=[target],
        fn_constructor_kwargs={"label": "output"},
        num_cpus=0.4,
        num_gpus=0.6,
        concurrency=(2, 5),
        batch_size=4,
        batch_timeout=timedelta(seconds=6),
        name="ConfiguredSink",
    )

    source_function = source.stream_operator.logical_function
    sink_function = sink.stream_operator.logical_function
    assert isinstance(source.stream_operator, SourceFunctionOperator)
    assert source.stream_operator.bounded is True
    assert source.resources == Resources(0.2, 0.3, (1, 3))
    assert source_function is not None
    assert source_function.constructor_args == ([{"key": "a"}],)
    assert source_function.constructor_kwargs == {"label": "input"}
    assert isinstance(sink.stream_operator, SinkOperator)
    assert sink.resources == Resources(0.4, 0.6, (2, 5))
    assert sink_function is not None
    assert sink_function.constructor_args == (target,)
    assert sink_function.constructor_kwargs == {"label": "output"}
    assert sink_function.runtime_info == RuntimeInfo(batch_size=4, batch_timeout=6, batch_format="default")
    assert context.sinks == (sink,)


def test_from_values_is_a_bounded_dual_mode_source() -> None:
    source = KleinContext().from_values({"key": "a"}, {"key": "b"})
    function = source.stream_operator.logical_function

    assert source.stream_operator.bounded is True
    assert function is not None
    assert function.batch_supported is True


def test_map_reduce_forwards_each_stage_configuration() -> None:
    source = KleinContext().from_values({"key": "a", "value": 1})

    result = source.map_reduce(
        key_selector=_key,
        preprocess_fn=_ConfiguredStage,
        batch_process_fn=_ConfiguredStage,
        postprocess_fn=_ConfiguredStage,
        preprocess_fn_constructor_args=["pre"],
        preprocess_fn_constructor_kwargs={"flag": True},
        batch_process_fn_constructor_args=["batch"],
        batch_process_fn_constructor_kwargs={"flag": False},
        postprocess_fn_constructor_args=["post"],
        postprocess_fn_constructor_kwargs={"mode": "strict"},
        num_cpus=(0.1, 0.2, 0.3),
        num_gpus=(0.4, 0.5, 0.6),
        concurrency=(1, 2, 3),
        preprocess_missing_data_strategy=MissingDataStrategy.IGNORE,
        batch_process_size=8,
        batch_process_timeout=timedelta(seconds=7),
        batch_process_format="numpy",
        name="Composite",
    )

    process = result.input_streams[0]
    preprocess = process.input_streams[0]
    assert isinstance(preprocess.stream_operator, FlatMapWithRankOperator)
    assert preprocess.name == "Composite-FlatMap"
    assert preprocess.resources == Resources(0.1, 0.4, 1)
    assert isinstance(preprocess.partitioner, AdaptivePartitioner)
    assert preprocess.stream_operator.to_spec().parameters == {
        "missing_data_strategy": MissingDataStrategy.IGNORE,
    }
    assert preprocess.stream_operator.logical_function.constructor_args == ("pre",)
    assert preprocess.stream_operator.logical_function.constructor_kwargs == {"flag": True}

    assert isinstance(process.stream_operator, BatchProcessOperator)
    assert process.name == "Composite-MapBatches"
    assert process.resources == Resources(0.2, 0.5, 2)
    assert isinstance(process.partitioner, KeyPartitioner)
    assert process.stream_operator.logical_function.constructor_args == ("batch",)
    assert process.stream_operator.logical_function.constructor_kwargs == {"flag": False}
    assert process.stream_operator.runtime_info == RuntimeInfo(
        batch_size=8,
        batch_timeout=7,
        batch_format="numpy",
    )

    assert isinstance(result.stream_operator, ReduceOperator)
    assert result.name == "Composite-Reduce"
    assert result.resources == Resources(0.3, 0.6, 3)
    assert result.stream_operator.logical_function.constructor_args == ("post",)
    assert result.stream_operator.logical_function.constructor_kwargs == {"mode": "strict"}


def test_keyed_window_and_join_forward_stateful_parameters() -> None:
    context = KleinContext()
    left = context.from_values({"key": "a", "timestamp": 1, "value": 1})
    right = context.from_values({"key": "a", "timestamp": 2, "value": 2})
    window = (
        left.key_by(_key)
        .window(
            TumblingWindow(timedelta(seconds=10)),
            timestamp_selector=_timestamp,
            allowed_lateness=timedelta(seconds=2),
            state_ttl=timedelta(minutes=1),
        )
        .reduce(lambda first, _second: first, num_cpus=0.2, num_gpus=0.3, concurrency=4, name="ConfiguredWindow")
    )
    joined = left.join(
        right,
        left_key=_key,
        right_key=_key,
        left_timestamp=_timestamp,
        right_timestamp=_timestamp,
        lower_bound=timedelta(seconds=-1),
        upper_bound=timedelta(seconds=3),
        join_function=lambda left_row, right_row: {**left_row, **right_row},
        allowed_lateness=timedelta(seconds=4),
        state_ttl=timedelta(minutes=2),
        num_cpus=0.5,
        num_gpus=0.6,
        concurrency=5,
        name="ConfiguredJoin",
    )

    assert isinstance(window.stream_operator, WindowOperator)
    assert window.resources == Resources(0.2, 0.3, 4)
    window_parameters = window.stream_operator.to_spec().parameters
    assert window_parameters["timestamp_selector"] is _timestamp
    assert window_parameters["allowed_lateness"] == timedelta(seconds=2)
    assert window_parameters["state_ttl"] == timedelta(minutes=1)

    assert isinstance(joined.stream_operator, IntervalJoinOperator)
    assert joined.resources == Resources(0.5, 0.6, 5)
    join_parameters = joined.stream_operator.to_spec().parameters
    assert join_parameters["lower_bound"] == timedelta(seconds=-1)
    assert join_parameters["upper_bound"] == timedelta(seconds=3)
    assert join_parameters["allowed_lateness"] == timedelta(seconds=4)
    assert join_parameters["state_ttl"] == timedelta(minutes=2)
    assert all(isinstance(stream.partitioner, KeyPartitioner) for stream in joined.input_streams)


def test_keyed_process_and_partitioning_build_the_requested_runtime_contracts() -> None:
    source = KleinContext().from_values({"key": "a", "timestamp": 1})

    processed = source.key_by(_key).process(
        _process,
        timestamp_selector=_timestamp,
        num_cpus=0.2,
        num_gpus=0.3,
        concurrency=4,
        name="ConfiguredProcess",
    )

    assert isinstance(processed.stream_operator, KeyedProcessOperator)
    assert processed.resources == Resources(0.2, 0.3, 4)
    parameters = processed.stream_operator.to_spec().parameters
    assert parameters["key_selector"] is _key
    assert parameters["timestamp_selector"] is _timestamp

    partitioned = KleinContext().from_values({"key": "a"})
    assert partitioned.broadcast() is partitioned
    assert isinstance(partitioned.partitioner, BroadcastPartitioner)
    assert partitioned.rescale() is partitioned
    assert isinstance(partitioned.partitioner, RescalePartitioner)
    assert partitioned.round_robin() is partitioned
    assert isinstance(partitioned.partitioner, RoundRobinPartitioner)
    assert partitioned.adaptive_shuffle() is partitioned
    assert isinstance(partitioned.partitioner, AdaptivePartitioner)
    assert partitioned.partition_by(_key) is partitioned
    assert isinstance(partitioned.partitioner, SimplePartitioner)


def test_changelog_modes_propagate_and_stream_composition_rejects_mixed_contexts() -> None:
    context = KleinContext()
    additions = context.from_values(ChangelogRow.insert({"key": "a"}))
    deletions = context.from_values(ChangelogRow.delete({"key": "a"}))

    assert additions.map(lambda row: row).filter(lambda _row: True).changelog_mode == frozenset({RowKind.INSERT})
    assert additions.union(deletions).changelog_mode == frozenset({RowKind.INSERT, RowKind.DELETE})

    other = KleinContext().from_values({"key": "a", "timestamp": 1})
    with pytest.raises(ValueError, match="multiple Klein contexts"):
        additions.union(other)
    with pytest.raises(ValueError, match="multiple Klein contexts"):
        additions.join(
            other,
            left_key=_key,
            right_key=_key,
            left_timestamp=lambda _row: 0,
            right_timestamp=_timestamp,
            lower_bound=timedelta(0),
            upper_bound=timedelta(0),
            join_function=lambda left, right: {**left, **right},
        )
