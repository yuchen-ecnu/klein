# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import numpy as np
import pyarrow as pa
import pytest
from sqlglot import exp, parse_one

from ray.klein import ChangelogRow, KleinContext, SQLQueryError, SQLSession, sql
from ray.klein._internal.sql.ai_function_execution import (
    _ai_projection_plan,
    _ApplyAIFunctionBatch,
    _ApplyAsyncAIFunctionBatch,
)
from ray.klein._internal.sql.ai_function_registry import AIFunctionRegistry, ai_function_arguments
from ray.klein.config.configuration import Configuration
from tests.support.ray_data import FakeDataset, logical_function_of


def _ai_call(query: str) -> exp.Expression:
    statement = parse_one(query, read="spark")
    projection = statement.expressions[0]
    return projection.this if isinstance(projection, exp.Alias) else projection


def _generate(calls: list[tuple[Any, ...]]) -> list[str]:
    return [f"answer:{call[0]}" for call in calls]


def test_ai_function_registry_is_case_insensitive_and_validates_options() -> None:
    registry = AIFunctionRegistry()
    registry.register(
        "AI_GENERATE",
        _generate,
        num_cpus=0.25,
        num_gpus=1,
        concurrency=(1, 3),
        batch_size=16,
        batch_timeout=timedelta(seconds=5),
    )

    spec = registry.snapshot()["ai_generate"]
    assert registry.identifiers == ("ai_generate",)
    assert spec.resources.num_cpus == 0.25
    assert spec.resources.num_gpus == 1
    assert spec.resources.concurrency == (1, 3)
    assert spec.batch_size == 16
    assert spec.batch_timeout_seconds == 5

    with pytest.raises(SQLQueryError, match="already registered"):
        registry.register("ai_generate", _generate)
    with pytest.raises(SQLQueryError, match="supported names"):
        registry.register("ai_classify", _generate)
    with pytest.raises(ValueError, match="batch_size"):
        AIFunctionRegistry().register("ai_embed", _generate, batch_size=0)
    with pytest.raises(ValueError, match="batch_timeout"):
        AIFunctionRegistry().register("ai_embed", _generate, batch_timeout=timedelta(0))
    with pytest.raises(TypeError, match="callable class"):
        AIFunctionRegistry().register("ai_embed", _generate, fn_constructor_args=("model",))
    with pytest.raises(ValueError, match="only for async"):
        AIFunctionRegistry().register("ai_embed", _generate, async_buffer_size=2)

    registry.drop("Ai_GeNeRaTe")
    assert registry.identifiers == ()
    with pytest.raises(SQLQueryError, match="Unknown SQL AI function"):
        registry.drop("ai_generate")


def test_ai_function_registry_preserves_subsecond_batch_timeout() -> None:
    registry = AIFunctionRegistry()

    registry.register("ai_generate", _generate, batch_timeout=timedelta(milliseconds=250))

    assert registry.snapshot()["ai_generate"].batch_timeout_seconds == 0.25


def test_ai_function_binding_rejects_missing_unsupported_nested_and_aggregate_calls() -> None:
    context = KleinContext()
    rows = context.data.source(lambda: FakeDataset())
    session = SQLSession(context)

    with pytest.raises(SQLQueryError, match=r"Unregistered.*AI_GENERATE"):
        session.sql("SELECT AI_GENERATE(prompt) FROM rows", tables={"rows": rows})

    session.register_ai_function("ai_generate", _generate)
    with pytest.raises(SQLQueryError, match="top-level SELECT"):
        session.sql("SELECT LOWER(AI_GENERATE(prompt)) FROM rows", tables={"rows": rows})
    with pytest.raises(SQLQueryError, match="aggregate query"):
        session.sql("SELECT AI_GENERATE(prompt), COUNT(*) FROM rows", tables={"rows": rows})
    with pytest.raises(SQLQueryError, match="AI_CLASSIFY is not supported yet"):
        session.sql("SELECT AI_CLASSIFY(prompt, ARRAY('yes', 'no')) FROM rows", tables={"rows": rows})
    with pytest.raises(SQLQueryError, match="requires one input"):
        session.sql("SELECT AI_GENERATE(prompt, '{}', 'extra') FROM rows", tables={"rows": rows})


def test_ai_function_bindings_are_snapshotted_and_inherited_by_top_level_sql(monkeypatch) -> None:
    context = KleinContext()
    rows = context.data.source(lambda: FakeDataset())
    captured = []

    def fake_sql_transform(primary, query, table_names, *others, **options):
        captured.append(options["ai_functions"])
        return primary

    monkeypatch.setattr("ray.klein.api.sql_session.sql_transform", fake_sql_transform)
    context.sql_session.register_ai_function("ai_generate", _generate, batch_size=7)

    result = sql(
        "SELECT AI_GENERATE(prompt) AS answer FROM rows",
        tables={"rows": rows},
        context=context,
    )
    context.sql_session.register_ai_function("ai_generate", lambda calls: [], replace=True)

    dataset = FakeDataset()
    assert logical_function_of(result).to_batch([dataset]) is dataset
    assert captured[0]["ai_generate"].function is _generate
    assert captured[0]["ai_generate"].batch_size == 7


def test_async_ai_backend_requires_streaming_execution() -> None:
    async def generate(calls):
        return [call[0] for call in calls]

    context = KleinContext()
    rows = context.data.source(lambda: FakeDataset())
    context.sql_session.register_ai_function("ai_generate", generate)

    with pytest.raises(SQLQueryError, match="require streaming"):
        context.sql("SELECT AI_GENERATE(prompt) FROM rows", tables={"rows": rows})


def test_async_ai_backend_plans_an_ordered_streaming_batch_operator() -> None:
    async def generate(calls):
        return [call[0] for call in calls]

    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    rows = context.from_items([{"prompt": "hello"}])
    context.sql_session.register_ai_function("ai_generate", generate, batch_size=4)

    result = context.sql("SELECT AI_GENERATE(prompt) AS answer FROM rows", tables={"rows": rows})
    logical = logical_function_of(result.input_streams[0])

    assert logical.function is _ApplyAsyncAIFunctionBatch
    assert logical.runtime_info.batch_size == 4
    assert logical.runtime_info.async_buffer_size == 8


def test_sync_ai_batch_worker_batches_calls_and_propagates_null() -> None:
    seen: list[list[tuple[Any, ...]]] = []

    def backend(calls):
        seen.append(calls)
        return [f"{prompt}:{config}" for prompt, config in calls]

    registry = AIFunctionRegistry()
    registry.register("ai_generate", backend)
    call = _ai_call("SELECT AI_GENERATE(prompt, config) AS answer FROM rows")
    worker = _ApplyAIFunctionBatch(
        "ai_generate",
        registry.snapshot()["ai_generate"],
        ai_function_arguments(call),
        {},
        "answer",
    )

    result = worker(
        {
            "prompt": np.asarray(["one", None, "two"], dtype=object),
            "config": np.asarray(["{}", "{}", '{"temperature":0}'], dtype=object),
        }
    )

    assert seen == [[("one", "{}"), ("two", '{"temperature":0}')]]
    assert result["answer"].tolist() == ["one:{}", None, 'two:{"temperature":0}']


def test_ai_batch_worker_preserves_arrow_list_columns() -> None:
    seen: list[list[tuple[Any, ...]]] = []

    def backend(calls):
        seen.append(calls)
        return ["ok" for _ in calls]

    registry = AIFunctionRegistry()
    registry.register("ai_generate", backend)
    call = _ai_call("SELECT AI_GENERATE(image) AS answer FROM rows")
    worker = _ApplyAIFunctionBatch(
        "ai_generate", registry.snapshot()["ai_generate"], ai_function_arguments(call), {}, "answer"
    )
    pages = pa.array([[b"page-1", b"page-2"], None], type=pa.list_(pa.binary()))
    images = pa.array([b"image", b"other"], type=pa.binary())

    result = worker(pa.table({"pages": pages, "image": images}))

    assert seen == [[(b"image",), (b"other",)]]
    assert result["pages"].type == pa.list_(pa.binary())
    assert result["pages"].to_pylist() == [[b"page-1", b"page-2"], None]


def test_ai_batch_worker_initializes_a_registered_backend_class() -> None:
    class Backend:
        def __init__(self, prefix: str) -> None:
            self._prefix = prefix

        def __call__(self, calls):
            return [f"{self._prefix}{call[0]}" for call in calls]

    registry = AIFunctionRegistry()
    registry.register("ai_generate", Backend, fn_constructor_kwargs={"prefix": "answer:"})
    call = _ai_call("SELECT AI_GENERATE(prompt) AS answer FROM rows")
    worker = _ApplyAIFunctionBatch(
        "ai_generate", registry.snapshot()["ai_generate"], ai_function_arguments(call), {}, "answer"
    )

    assert worker({"prompt": np.asarray(["hello"])})["answer"].tolist() == ["answer:hello"]


def test_ai_batch_worker_validates_results_and_redacts_backend_error() -> None:
    registry = AIFunctionRegistry()
    call = _ai_call("SELECT AI_EMBED(text) AS embedding FROM rows")

    registry.register("ai_embed", lambda _calls: [])
    wrong_length = _ApplyAIFunctionBatch(
        "ai_embed", registry.snapshot()["ai_embed"], ai_function_arguments(call), {}, "embedding"
    )
    with pytest.raises(SQLQueryError, match="returned 0 results for 1 calls"):
        wrong_length({"text": np.asarray(["hello"])})

    def failing(_calls):
        raise RuntimeError("secret prompt and token")

    registry.register("ai_embed", failing, replace=True)
    failure = _ApplyAIFunctionBatch(
        "ai_embed", registry.snapshot()["ai_embed"], ai_function_arguments(call), {}, "embedding"
    )
    with pytest.raises(SQLQueryError, match="AI_EMBED backend failed with RuntimeError") as raised:
        failure({"text": np.asarray(["hello"])})
    assert "secret prompt" not in str(raised.value)


def test_async_ai_batch_worker_awaits_one_batch() -> None:
    async def backend(calls):
        await asyncio.sleep(0)
        return [[float(len(call[0]))] for call in calls]

    registry = AIFunctionRegistry()
    registry.register("ai_embed", backend)
    call = _ai_call("SELECT AI_EMBED(text) AS embedding FROM rows")
    worker = _ApplyAsyncAIFunctionBatch(
        "ai_embed", registry.snapshot()["ai_embed"], ai_function_arguments(call), {}, "embedding"
    )

    result = asyncio.run(worker({"text": np.asarray(["a", "abc"])}))

    assert result["embedding"].tolist() == [[1.0], [3.0]]


def test_ai_projection_plan_deduplicates_calls_and_preserves_aliases() -> None:
    registry = AIFunctionRegistry()
    registry.register("ai_generate", _generate)
    statement = parse_one(
        "SELECT AI_GENERATE(prompt) AS first, AI_GENERATE(prompt) AS second, id FROM rows",
        read="spark",
    )

    computations, projections = _ai_projection_plan(statement.expressions, registry.snapshot())

    assert len(computations) == 1
    assert [projection.alias for projection in projections] == ["first", "second", ""]
    assert projections[0].this.name == "_klein_ai_result_0"
    assert projections[1].this.name == "_klein_ai_result_0"


@pytest.mark.parametrize(
    "argument",
    ["DOWNLOAD(uri)", "UUID()", "MONOTONICALLY_INCREASING_ID()"],
)
def test_ai_projection_plan_rejects_ray_native_only_arguments(argument: str) -> None:
    registry = AIFunctionRegistry()
    registry.register("ai_generate", _generate)
    statement = parse_one(f"SELECT AI_GENERATE({argument}) FROM rows", read="spark")

    with pytest.raises(SQLQueryError, match="Ray-native-only"):
        _ai_projection_plan(statement.expressions, registry.snapshot())


@pytest.mark.parametrize("runtime_mode", ["batch", "streaming"])
def test_ai_download_composition_fails_during_graph_construction(runtime_mode: str) -> None:
    context = KleinContext(Configuration(f"execution.runtime.mode={runtime_mode}"))
    rows = (
        context.data.source(lambda: FakeDataset())
        if runtime_mode == "batch"
        else context.from_items([{"uri": "https://example.test/image.png"}])
    )
    context.sql_session.register_ai_function("ai_generate", _generate)

    with pytest.raises(SQLQueryError, match="standalone SELECT value"):
        context.sql(
            "SELECT AI_GENERATE(DOWNLOAD(uri)) AS answer FROM rows",
            tables={"rows": rows},
        )


@pytest.mark.parametrize("runtime_mode", ["batch", "streaming"])
@pytest.mark.parametrize("argument", ["UUID()", "MONOTONICALLY_INCREASING_ID()"])
def test_ai_ray_native_only_arguments_fail_during_graph_construction(
    runtime_mode: str,
    argument: str,
) -> None:
    context = KleinContext(Configuration(f"execution.runtime.mode={runtime_mode}"))
    rows = (
        context.data.source(lambda: FakeDataset())
        if runtime_mode == "batch"
        else context.from_items([{"prompt": "hello"}])
    )
    context.sql_session.register_ai_function("ai_generate", _generate)

    with pytest.raises(SQLQueryError, match="Ray-native-only"):
        context.sql(
            f"SELECT AI_GENERATE({argument}) AS answer FROM rows",
            tables={"rows": rows},
        )


def test_streaming_ai_projection_uses_dedicated_batched_operator_and_rejects_retractions() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    rows = context.from_items([{"prompt": "hello"}])
    context.sql_session.register_ai_function(
        "ai_generate",
        _generate,
        num_cpus=0.25,
        concurrency=2,
        batch_size=8,
    )

    result = context.sql(
        "SELECT AI_GENERATE(prompt) AS answer FROM rows",
        tables={"rows": rows},
    )
    ai_stream = result.input_streams[0]
    logical = logical_function_of(ai_stream)
    assert logical.function is _ApplyAIFunctionBatch
    assert logical.runtime_info.batch_size == 8
    assert logical.runtime_info.batch_format == "numpy"
    assert ai_stream.resources.num_cpus == 0.25
    assert ai_stream.resources.concurrency == 2

    changes = context.from_values(
        ChangelogRow.insert({"prompt": "hello"}),
        ChangelogRow.delete({"prompt": "hello"}),
    )
    with pytest.raises(SQLQueryError, match="insert-only"):
        context.sql(
            "SELECT AI_GENERATE(prompt) AS answer FROM changes",
            tables={"changes": changes},
        )
