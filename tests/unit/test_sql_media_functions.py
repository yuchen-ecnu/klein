# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pyarrow as pa
import pytest
from sqlglot import exp, parse_one

from ray.klein import ChangelogRow, KleinContext, SQLQueryError, SQLSession
from ray.klein._internal.sql.media_function import (
    is_media_function_call,
    plan_media_projections,
    validate_media_function_calls,
)
from ray.klein._internal.sql.media_function_execution import (
    MEDIA_BATCH_SIZE,
    _ApplyMediaFunctionsBatch,
    apply_batch_media_computations,
)
from ray.klein._internal.sql.media_runtime import MediaLimits
from ray.klein.config.configuration import Configuration
from tests.support.ray_data import FakeDataset, logical_function_of

_MEDIA_FUNCTIONS = (
    "IMAGE_RESIZE",
    "IMAGE_WIDTH",
    "IMAGE_HEIGHT",
    "IMAGE_FORMAT",
    "PDF_PAGE_COUNT",
    "PDF_SPLIT",
    "PDF_RENDER_PAGE",
    "PDF_TO_IMAGES",
)


def _batch_rows():
    context = KleinContext()
    return context, context.data.source(lambda: FakeDataset())


@pytest.mark.parametrize("name", _MEDIA_FUNCTIONS)
def test_media_function_names_are_reserved_from_scalar_udfs(name: str) -> None:
    with pytest.raises(SQLQueryError, match="reserved"):
        SQLSession(KleinContext()).register_scalar_function(name, lambda value: value)


@pytest.mark.parametrize(
    "call",
    [
        "IMAGE_RESIZE(payload, 10)",
        "IMAGE_RESIZE(payload, 10, 20, 'contain', 'PNG', 85, 'extra')",
        "IMAGE_WIDTH()",
        "IMAGE_WIDTH(payload, 1)",
        "IMAGE_HEIGHT()",
        "IMAGE_HEIGHT(payload, 1)",
        "IMAGE_FORMAT()",
        "IMAGE_FORMAT(payload, 1)",
        "PDF_PAGE_COUNT()",
        "PDF_PAGE_COUNT(payload, 1)",
        "PDF_SPLIT()",
        "PDF_SPLIT(payload, 1, 2, 3)",
        "PDF_RENDER_PAGE(payload)",
        "PDF_RENDER_PAGE(payload, 1, 144, 2)",
        "PDF_TO_IMAGES()",
        "PDF_TO_IMAGES(payload, 144, 1, 2, 3)",
    ],
)
def test_media_binding_rejects_invalid_arity(call: str) -> None:
    context, rows = _batch_rows()

    with pytest.raises(SQLQueryError):
        context.sql(f"SELECT {call} AS result FROM rows", tables={"rows": rows})


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id FROM rows WHERE IMAGE_WIDTH(payload) > 0",
        "SELECT id FROM rows GROUP BY id, IMAGE_FORMAT(payload)",
        "SELECT id FROM rows HAVING IMAGE_HEIGHT(payload) > 0",
        "SELECT COUNT(IMAGE_WIDTH(payload)) AS count FROM rows",
        "SELECT IMAGE_WIDTH(payload) AS width, COUNT(*) AS count FROM rows",
    ],
)
def test_media_binding_rejects_stateful_and_predicate_locations(query: str) -> None:
    context, rows = _batch_rows()

    with pytest.raises(SQLQueryError, match=r"media|Media|MEDIA|IMAGE_|PDF_"):
        context.sql(query, tables={"rows": rows})


def test_media_binding_rejects_join_conditions() -> None:
    context, rows = _batch_rows()
    dimensions = context.data.source(lambda: FakeDataset())

    with pytest.raises(SQLQueryError, match=r"media|Media|MEDIA|IMAGE_"):
        context.sql(
            "SELECT * FROM rows JOIN dimensions AS d ON IMAGE_WIDTH(rows.payload) = d.width",
            tables={"rows": rows, "dimensions": dimensions},
        )


def test_media_binding_accepts_nested_projection_expressions_and_ai_inputs() -> None:
    context, rows = _batch_rows()
    context.sql_session.register_ai_function("ai_generate", lambda calls: [len(call[0]) for call in calls])

    projected = context.sql(
        "SELECT IMAGE_WIDTH(payload) + 1 AS padded_width, "
        "CONCAT(IMAGE_FORMAT(payload), '-image') AS kind, "
        "IMAGE_WIDTH(IMAGE_RESIZE(payload, 10, 10)) AS resized_width FROM rows",
        tables={"rows": rows},
    )
    ai_input = context.sql(
        "SELECT AI_GENERATE(IMAGE_RESIZE(payload, 10, 10, 'stretch', 'PNG', 85)) AS answer FROM rows",
        tables={"rows": rows},
    )

    assert projected.input_streams == [rows]
    assert ai_input.input_streams == [rows]


def test_media_projection_plan_deduplicates_calls_hoists_downloads_and_fuses_one_batch() -> None:
    statement = parse_one(
        "SELECT IMAGE_WIDTH(payload) AS first, IMAGE_HEIGHT(payload) AS height, "
        "IMAGE_WIDTH(payload) AS second, "
        "IMAGE_WIDTH(IMAGE_RESIZE(payload, 10, 10)) AS resized_width, "
        "IMAGE_RESIZE(DOWNLOAD(uri), 2, 2) AS downloaded, "
        "IMAGE_RESIZE(DOWNLOAD(uri), 2, 2) AS downloaded_again FROM rows",
        read="spark",
    )
    plan = plan_media_projections(statement.expressions)

    assert len(plan.downloads) == 1
    assert [computation.function_name for computation in plan.computations] == [
        "image_width",
        "image_height",
        "image_resize",
        "image_width",
        "image_resize",
    ]
    assert plan.projections[0].this.name == "_klein_media_result_0"
    assert plan.projections[2].this.name == "_klein_media_result_0"
    assert plan.projections[4].this.name == "_klein_media_result_4"
    assert plan.projections[5].this.name == "_klein_media_result_4"
    nested_width = plan.computations[3]
    assert isinstance(nested_width.arguments[0], exp.Column)
    assert nested_width.arguments[0].name == "_klein_media_result_2"
    assert not any(is_media_function_call(node) for projection in plan.projections for node in projection.walk())

    class RecordingDataset:
        def __init__(self) -> None:
            self.calls = []

        def map_batches(self, function, **options):
            self.calls.append((function, options))
            return self

    dataset = RecordingDataset()
    assert (
        apply_batch_media_computations(
            dataset,
            plan.computations,
            functions={},
            num_cpus=0.5,
        )
        is dataset
    )
    assert len(dataset.calls) == 1
    function, options = dataset.calls[0]
    assert function is _ApplyMediaFunctionsBatch
    assert options["batch_size"] == MEDIA_BATCH_SIZE
    assert options["batch_format"] == "pyarrow"
    assert options["num_cpus"] == 0.5


def test_media_projection_plan_shares_one_stage_with_direct_downloads() -> None:
    statement = parse_one(
        "SELECT DOWNLOAD(raw_uri) AS raw, IMAGE_RESIZE(DOWNLOAD(image_uri), 2, 2) AS thumbnail FROM rows",
        read="spark",
    )

    plan = plan_media_projections(statement.expressions)

    assert [name for name, _ in plan.downloads] == [
        "_klein_media_download_0",
        "_klein_media_download_1",
    ]
    assert [expression.sql() for _, expression in plan.downloads] == [
        "DOWNLOAD(raw_uri)",
        "DOWNLOAD(image_uri)",
    ]
    assert plan.projections[0].this.sql() == "_klein_media_download_0"


def test_media_batch_worker_fuses_calls_reuses_each_row_cache_and_propagates_null() -> None:
    statement = parse_one(
        "SELECT IMAGE_WIDTH(payload), IMAGE_HEIGHT(payload) FROM rows",
        read="spark",
    )
    computations = plan_media_projections(statement.expressions).computations
    worker = _ApplyMediaFunctionsBatch(computations, {})

    class RecordingRuntime:
        def __init__(self) -> None:
            self.calls = []
            self.cleared = []

        def execute(self, name, arguments, cache):
            self.calls.append((name, arguments, cache))
            multiplier = 1 if name == "image_width" else 2
            return len(arguments[0]) * multiplier

        def clear_cache(self, cache):
            self.cleared.append(cache)

    runtime = RecordingRuntime()
    worker._runtime = runtime
    result = worker({"payload": [b"a", None, b"abc"]})

    fields = [computation.field_name for computation in computations]
    assert result[fields[0]].tolist() == [1, None, 3]
    assert result[fields[1]].tolist() == [2, None, 6]
    assert [call[0] for call in runtime.calls] == [
        "image_width",
        "image_height",
        "image_width",
        "image_height",
    ]
    assert runtime.calls[0][2] is runtime.calls[1][2]
    assert runtime.calls[2][2] is runtime.calls[3][2]
    assert runtime.calls[0][2] is not runtime.calls[2][2]
    assert len(runtime.cleared) == 3


def test_media_batch_worker_uses_native_arrow_lists_for_pdf_arrays() -> None:
    statement = parse_one("SELECT PDF_SPLIT(payload), PDF_TO_IMAGES(payload) FROM rows", read="spark")
    computations = plan_media_projections(statement.expressions).computations
    worker = _ApplyMediaFunctionsBatch(computations, {})

    class RecordingRuntime:
        def execute(self, name, _arguments, _cache):
            return [name.encode(), b"page-2"]

        def clear_cache(self, _cache):
            return None

    worker._runtime = RecordingRuntime()
    result = worker({"payload": [b"pdf", None]})

    for computation in computations:
        values = result[computation.field_name]
        assert isinstance(values, pa.Array)
        assert values.type == pa.list_(pa.binary())
        assert values.to_pylist() == [[computation.function_name.encode(), b"page-2"], None]


def test_media_batch_worker_redacts_runtime_failures() -> None:
    statement = parse_one("SELECT IMAGE_WIDTH(payload) FROM rows", read="spark")
    computations = plan_media_projections(statement.expressions).computations
    worker = _ApplyMediaFunctionsBatch(computations, {})

    class FailingRuntime:
        def execute(self, _name, _arguments, _cache):
            raise RuntimeError("secret image payload")

        def clear_cache(self, _cache):
            return None

    worker._runtime = FailingRuntime()
    with pytest.raises(SQLQueryError, match="IMAGE_WIDTH failed with RuntimeError") as raised:
        worker({"payload": [b"secret"]})
    assert "secret image payload" not in str(raised.value)


def test_media_batch_worker_bounds_cumulative_binary_values_and_clears_cache() -> None:
    statement = parse_one("SELECT IMAGE_RESIZE(payload, 1, 1) FROM rows", read="spark")
    computations = plan_media_projections(statement.expressions).computations
    worker = _ApplyMediaFunctionsBatch(computations, {}, limits=MediaLimits(max_batch_bytes=5))

    class RecordingRuntime:
        def __init__(self) -> None:
            self.calls = 0
            self.cleared = 0

        def execute(self, _name, _arguments, _cache):
            self.calls += 1
            return b"out"

        def clear_cache(self, _cache):
            self.cleared += 1

    runtime = RecordingRuntime()
    worker._runtime = runtime

    with pytest.raises(SQLQueryError, match="5-byte cumulative binary safety limit"):
        worker({"payload": [b"aa", b"bb"]})

    assert runtime.calls == 1
    assert runtime.cleared == 2


@pytest.mark.parametrize(
    "query",
    [
        "SELECT IMAGE_RESIZE(payload, DOWNLOAD(uri), 10) FROM rows",
        "SELECT PDF_PAGE_COUNT(DOWNLOAD(uri) || payload) FROM rows",
    ],
)
def test_media_binding_rejects_invalid_download_placement(query: str) -> None:
    context, rows = _batch_rows()

    with pytest.raises(SQLQueryError):
        context.sql(query, tables={"rows": rows})


@pytest.mark.parametrize(
    "query",
    [
        "SELECT IMAGE_RESIZE(payload, UUID(), 10) FROM rows",
        "SELECT IMAGE_RESIZE(payload, MONOTONICALLY_INCREASING_ID(), 10) FROM rows",
    ],
)
def test_media_binding_rejects_ray_native_only_arguments(query: str) -> None:
    context, rows = _batch_rows()

    with pytest.raises(SQLQueryError, match="Ray-native-only"):
        context.sql(query, tables={"rows": rows})


@pytest.mark.parametrize(
    "query",
    [
        "SELECT AI_GENERATE(DOWNLOAD(uri)) FROM rows",
        "SELECT COALESCE(DOWNLOAD(uri), payload) FROM rows",
        "SELECT id FROM rows WHERE DOWNLOAD(uri) IS NOT NULL",
    ],
)
def test_download_validation_rejects_non_lowerable_composition(query: str) -> None:
    with pytest.raises(SQLQueryError, match="standalone SELECT value"):
        validate_media_function_calls(parse_one(query, read="spark"))


@pytest.mark.parametrize(
    "query",
    [
        "SELECT DOWNLOAD(uri) AS body FROM rows",
        "SELECT COUNT(DOWNLOAD(uri)) AS downloaded FROM rows",
        "SELECT IMAGE_RESIZE(DOWNLOAD(uri), 2, 2) AS image FROM rows",
        "SELECT AI_GENERATE(IMAGE_RESIZE(DOWNLOAD(uri), 2, 2)) AS answer FROM rows",
    ],
)
def test_download_validation_accepts_supported_placements(query: str) -> None:
    validate_media_function_calls(parse_one(query, read="spark"))


def test_streaming_media_functions_require_insert_only_input() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    changes = context.from_values(
        ChangelogRow.insert({"payload": b"image"}),
        ChangelogRow.delete({"payload": b"image"}),
    )

    with pytest.raises(SQLQueryError, match="insert-only"):
        context.sql(
            "SELECT IMAGE_WIDTH(payload) AS width FROM changes",
            tables={"changes": changes},
        )


def test_streaming_media_projection_uses_one_fused_batch_operator() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    rows = context.from_items([{"payload": b"image"}])

    result = context.sql(
        "SELECT IMAGE_WIDTH(payload) AS width, IMAGE_HEIGHT(payload) AS height FROM rows",
        tables={"rows": rows},
    )
    media_stream = result.input_streams[0]

    assert MEDIA_BATCH_SIZE == 1
    assert logical_function_of(media_stream).function is _ApplyMediaFunctionsBatch
    assert logical_function_of(media_stream).runtime_info.batch_size == MEDIA_BATCH_SIZE
