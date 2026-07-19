# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the version-adaptive Ray Data boundary."""

import inspect
from typing import Any

import pytest
import ray.data
from ray.data import Dataset
from ray.data.expressions import col, download

import ray.klein as klein
from ray.klein._internal.streaming_expression import (
    AsyncStreamingWithColumn,
    StreamingExpressionFilter,
    StreamingWithColumn,
)
from ray.klein.api.data_stream import DataStream
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.ray_data import (
    RayDataAPIError,
    RayDataMethodKind,
    classify_dataset_method,
    has_public_dataset_method,
    public_dataset_factories,
    public_dataset_methods,
)
from ray.klein.integrations.sql import StreamingSQLSink
from tests.support.ray_data import FakeDataset, logical_function_of


def test_context_namespace_tracks_every_installed_ray_data_function() -> None:
    ctx = KleinContext()

    assert ctx.data.available == public_dataset_factories()
    assert "configure_logging" not in ctx.data.available
    for name in public_dataset_factories():
        assert callable(getattr(ctx.data, name))

    with pytest.raises(AttributeError, match="not a Dataset factory"):
        _ = ctx.data.configure_logging


def test_module_namespace_tracks_installed_ray_data_factories() -> None:
    context = KleinContext.reset()

    assert inspect.signature(klein.read_csv) == inspect.signature(ray.data.read_csv)
    assert "read_csv" in dir(klein)
    assert klein.read_csv("input/").context is context


def test_stream_namespace_tracks_every_installed_dataset_method() -> None:
    stream = KleinContext().data.source(lambda: FakeDataset())

    assert stream.data.available == public_dataset_methods()
    for name in public_dataset_methods():
        assert callable(getattr(stream.data, name))


def test_dynamic_calls_preserve_installed_ray_signatures_and_docs() -> None:
    ctx = KleinContext()
    stream = ctx.data.source(lambda: FakeDataset())

    assert inspect.signature(ctx.data.read_csv) == inspect.signature(ray.data.read_csv)
    for name in public_dataset_methods():
        target = getattr(Dataset, name)
        expected = inspect.signature(target)
        if inspect.isfunction(inspect.getattr_static(Dataset, name)):
            expected = expected.replace(parameters=tuple(expected.parameters.values())[1:])
        adapted = getattr(stream.data, name)
        assert inspect.signature(adapted) == expected
        assert adapted.__doc__ == target.__doc__


def test_names_are_resolved_at_execution_and_arguments_are_forwarded_verbatim(monkeypatch) -> None:
    ctx = KleinContext()
    source = ctx.data.read_csv("input/", ray_remote_args={"num_cpus": 2})
    expected = FakeDataset()
    captured = []

    def replacement_reader(*args, **kwargs):
        captured.append(("source", args, kwargs))
        return expected

    monkeypatch.setattr(ray.data, "read_csv", replacement_reader)
    assert logical_function_of(source).to_batch([]) is expected

    transformed = source.data.rename_columns({"old": "new"}, concurrency=3)

    def replacement_transform(dataset, *args, **kwargs):
        captured.append(("transform", (dataset, *args), kwargs))
        return dataset

    monkeypatch.setattr(Dataset, "rename_columns", replacement_transform)
    assert logical_function_of(transformed).to_batch([expected]) is expected
    assert captured == [
        ("source", ("input/",), {"ray_remote_args": {"num_cpus": 2}}),
        ("transform", (expected, {"old": "new"}), {"concurrency": 3}),
    ]


def test_ray_data_expression_is_forwarded_without_wrapping(monkeypatch) -> None:
    dataset = FakeDataset()
    stream = KleinContext().data.source(lambda: dataset)
    expression = download("uri")
    captured = []

    def replacement_with_column(dataset, name, expr, **kwargs):
        captured.append((dataset, name, expr, kwargs))
        return dataset

    transformed = stream.data.with_column("body", expression, num_cpus=0.5)
    monkeypatch.setattr(Dataset, "with_column", replacement_with_column)

    assert logical_function_of(transformed).to_batch([dataset]) is dataset
    assert len(captured) == 1
    captured_dataset, captured_name, captured_expression, captured_options = captured[0]
    assert captured_dataset is dataset
    assert captured_name == "body"
    assert captured_expression is expression
    assert captured_options == {"num_cpus": 0.5}


def test_ray_data_expression_transforms_have_streaming_implementations() -> None:
    stream = KleinContext().from_values({"uri": "local:///tmp/file", "value": 2})

    computed = stream.data.with_column(column_name="doubled", expr=col("value") * 2)
    downloaded = stream.data.with_column("body", download("uri"))
    filtered = stream.data.filter(expr=col("value") > 1)

    assert logical_function_of(computed).function is StreamingWithColumn
    assert logical_function_of(downloaded).function is AsyncStreamingWithColumn
    assert logical_function_of(downloaded).runtime_info.async_buffer_size == 32
    assert logical_function_of(filtered).function is StreamingExpressionFilter


def test_write_sql_matches_ray_data_arguments_and_lowers_to_its_consumer(monkeypatch) -> None:
    ray_parameters = tuple(inspect.signature(Dataset.write_sql).parameters.values())[1:]
    klein_parameters = tuple(inspect.signature(DataStream.write_sql).parameters.values())[1:]
    assert [(parameter.name, parameter.kind, parameter.default) for parameter in klein_parameters] == [
        (parameter.name, parameter.kind, parameter.default) for parameter in ray_parameters
    ]

    dataset = FakeDataset()
    stream = KleinContext().data.source(lambda: dataset)
    captured = []

    def connection_factory():
        return object()

    def replacement_write_sql(dataset, *args, **kwargs):
        captured.append((dataset, args, kwargs))

    sink = stream.write_sql(
        "INSERT INTO test VALUES(?)",
        connection_factory,
        ray_remote_args={"num_cpus": 2},
        concurrency=3,
    )
    monkeypatch.setattr(Dataset, "write_sql", replacement_write_sql)

    assert logical_function_of(sink).function is StreamingSQLSink
    assert logical_function_of(sink).to_batch([dataset]) is None
    assert captured == [
        (
            dataset,
            ("INSERT INTO test VALUES(?)", connection_factory),
            {"ray_remote_args": {"num_cpus": 2}, "concurrency": 3},
        )
    ]


def test_source_callable_is_lazy_and_validates_its_result() -> None:
    expected = FakeDataset()
    stream = KleinContext().data.source(lambda value: value, expected)

    assert logical_function_of(stream).to_batch([]) is expected

    invalid = KleinContext().data.source(lambda: 42)
    with pytest.raises(RayDataAPIError, match="returned int"):
        logical_function_of(invalid).to_batch([])


def test_explicit_transform_and_consume_cover_arbitrary_ray_data_code() -> None:
    dataset = FakeDataset()
    stream = KleinContext().data.source(lambda: dataset)

    transformed = stream.data.transform(lambda ds, marker: ds, "groupby can finish here")
    assert logical_function_of(transformed).to_batch([dataset]) is dataset

    consumed = transformed.data.consume(lambda ds: {"dataset": ds})
    assert logical_function_of(consumed).to_batch([dataset]) == {"dataset": dataset}


def test_transform_rejects_a_terminal_result_with_actionable_error() -> None:
    dataset = FakeDataset()
    stream = KleinContext().data.source(lambda: dataset)
    invalid = stream.data.transform(lambda ds: 1)

    with pytest.raises(RayDataAPIError, match=r"stream\.data\.consume"):
        logical_function_of(invalid).to_batch([dataset])


def test_other_klein_streams_become_dataset_dependencies_even_when_nested() -> None:
    ctx = KleinContext()
    left_stream = ctx.data.source(lambda: FakeDataset())
    right_stream = ctx.data.source(lambda: FakeDataset())
    captured: list[Any] = []

    def combine(left, payload):
        captured.append((left, payload))
        return left

    combined = left_stream.data.transform(combine, {"right": [right_stream]})
    left, right = FakeDataset(), FakeDataset()

    assert len(combined.input_streams) == 2
    assert logical_function_of(combined).to_batch([left, right]) is left
    assert captured == [(left, {"right": [right]})]


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("map_batches", RayDataMethodKind.TRANSFORM),
        ("materialize", RayDataMethodKind.TRANSFORM),
        ("rename_columns", RayDataMethodKind.TRANSFORM),
        ("take_all", RayDataMethodKind.CONSUME),
        ("write_csv", RayDataMethodKind.CONSUME),
        ("write_sql", RayDataMethodKind.CONSUME),
        ("groupby", RayDataMethodKind.CONSUME),
    ],
)
def test_automatic_method_classification(name: str, kind: RayDataMethodKind) -> None:
    assert classify_dataset_method(name) is kind


@pytest.mark.skipif(
    not has_public_dataset_method("explain"),
    reason="installed Ray Data does not expose Dataset.explain",
)
def test_explain_is_classified_as_a_consumer_when_available() -> None:
    assert classify_dataset_method("explain") is RayDataMethodKind.CONSUME


def test_unknown_dataset_method_is_rejected() -> None:
    with pytest.raises(AttributeError, match="has no public method"):
        classify_dataset_method("not_a_ray_data_method")


def test_all_installed_dataset_methods_have_an_automatic_dispatch() -> None:
    assert {classify_dataset_method(name) for name in public_dataset_methods()} <= {
        RayDataMethodKind.TRANSFORM,
        RayDataMethodKind.CONSUME,
    }


def test_ray_data_calls_require_the_explicit_namespace() -> None:
    ctx = KleinContext()
    with pytest.raises(AttributeError):
        ctx.read_csv("input/")

    source = ctx.data.source(lambda: FakeDataset())
    with pytest.raises(AttributeError):
        source.random_shuffle()

    assert isinstance(source.data.random_shuffle(), DataStream)


def test_manual_ray_data_api_mirrors_do_not_grow_back() -> None:
    context_methods = set(KleinContext.__dict__)
    stream_methods = set(DataStream.__dict__)

    assert {name for name in context_methods if name.startswith(("read_", "from_"))} == {
        "read_canal",
        "read_kafka",
        "read_rocketmq",
        "from_items",
        "from_values",
    }
    # These are intentional Klein sink APIs. File methods share Ray Data's
    # familiar spelling but add checkpointed 2PC, while write_sql selects Ray
    # Data in batch mode and Klein's native SQL sink in streaming mode.
    assert {name for name in stream_methods if name.startswith("write_")} == {
        "write_csv",
        "write_files",
        "write_json",
        "write_kafka",
        "write_parquet",
        "write_redis",
        "write_sql",
        "write_text",
    }
