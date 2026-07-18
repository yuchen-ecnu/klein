# SPDX-License-Identifier: Apache-2.0
from typing import Any

import pytest

from ray.klein._internal.logging import get_logger
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from tests.support.assertions import assert_rows_equal

logger = get_logger(__name__)


def log_map(row):
    logger.debug("batch row: %s", row)
    return row


def map_batches(batch):
    return {"id": batch["idx2"] + 1}


def origin_map_function(row: dict[str, Any]) -> dict[str, Any]:
    return row


class IdentityMap:
    def __call__(self, row):
        return row


class BatchMapFunction:
    def __init__(self, name: str, runtime_context: RuntimeContext = None):
        self.name = name
        self.runtime_context = runtime_context
        self.counter = runtime_context.metric_group.counter("my_counter")

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        self.counter.inc()
        return row


class BatchMapWithKwargsFunction:
    def __init__(
        self,
        name: str,
        test_kwarg1: str = "default",
        runtime_context: RuntimeContext = None,
    ):
        if test_kwarg1 == "default":
            raise ValueError("test_kwarg1 was not injected")
        self.name = name
        self.test_kwarg1 = test_kwarg1
        self.runtime_context = runtime_context

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        return row


@pytest.fixture()
def batch_context(configuration):
    configuration.set(ExecutionOptions.MODE, RuntimeExecutionMode.BATCH)
    return KleinContext(configuration)


@pytest.fixture()
def batch_stream(batch_context, test_data_dir):
    stream = (
        batch_context.data.read_csv(str(test_data_dir / "test_data.csv"))
        .map(log_map)
        .map(lambda row: {"idx2": row["id"] * 2})
        .map_batches(map_batches)
    )
    return batch_context, stream


def test_batch_pipeline_writes_parquet(batch_stream, tmp_path) -> None:
    context, stream = batch_stream
    stream.data.write_parquet(str(tmp_path / "parquet"))

    context.execute("batch-write-parquet").wait()


def test_schema_and_show_are_executable_sinks(batch_stream) -> None:
    context, stream = batch_stream
    stream.schema()
    stream.show(limit=1)

    results = context.execute("batch-metadata-sinks").get()

    assert len(results) == 2


def test_take_returns_exact_prefix(batch_stream) -> None:
    context, stream = batch_stream
    context.enable_interactive_mode()

    assert stream.take(limit=1) == [{"id": 3}]


def test_take_all_supports_actor_pool_concurrency(batch_stream) -> None:
    context, stream = batch_stream
    context.enable_interactive_mode()

    assert stream.map(IdentityMap, concurrency=(1, 2)).take_all() == [{"id": 3}, {"id": 5}, {"id": 7}]


def test_take_all_rejects_a_limit(batch_stream) -> None:
    context, stream = batch_stream
    context.enable_interactive_mode()

    with pytest.raises(ValueError, match="limit"):
        stream.take_all(limit=1)


def test_runtime_context_is_injected_into_callable_class(batch_context, test_data_dir) -> None:
    stream = batch_context.data.read_csv(str(test_data_dir / "test_data.csv")).map(
        BatchMapFunction,
        fn_constructor_args=["map-with-context"],
        num_cpus=1.5,
        concurrency=2,
        batch_size=2,
        name="MapOperator",
    )
    stream.show(limit=2)

    batch_context.execute("runtime-context-injection").wait()


def test_constructor_kwargs_are_injected(batch_context, test_data_dir) -> None:
    stream = batch_context.data.read_csv(str(test_data_dir / "test_data.csv")).map(
        BatchMapWithKwargsFunction,
        fn_constructor_args=["map-with-kwargs"],
        fn_constructor_kwargs={"test_kwarg1": "user-defined"},
        num_cpus=1.5,
        concurrency=2,
        batch_size=2,
        name="MapOperator",
    )
    stream.show(limit=2)

    batch_context.execute("constructor-kwargs-injection").wait()


def test_multi_sink_outputs_are_independent(batch_context, test_data_dir, tmp_path) -> None:
    csv_output = tmp_path / "csv"
    json_output = tmp_path / "json"
    parquet_output = tmp_path / "parquet"
    first = batch_context.data.read_csv(str(test_data_dir / "csv_data1.csv"))
    second = batch_context.data.read_csv(str(test_data_dir / "csv_data2.csv"))
    union = first.union(second)
    union.map(origin_map_function, num_cpus=0.1, concurrency=2).data.write_csv(str(csv_output), min_rows_per_file=1000)
    union.data.write_json(str(json_output), min_rows_per_file=1000)
    second.map(origin_map_function, num_cpus=0.1, concurrency=2).data.write_parquet(str(parquet_output))

    batch_context.execute("multi-sink").wait()

    reader = KleinContext(batch_context.config)
    reader.enable_interactive_mode()
    union_rows = [
        {"id": "cd2_1", "name": "cd2_tom", "age": 18},
        {"id": "cd2_2", "name": "cd2_tony", "age": 22},
        {"id": "cd2_3", "name": "cd2_jerry", "age": 21},
        {"id": "cd1_1", "name": "cd1_tom", "age": 18},
        {"id": "cd1_2", "name": "cd1_tony", "age": 22},
        {"id": "cd1_3", "name": "cd1_jerry", "age": 21},
    ]
    second_rows = union_rows[:3]

    assert_rows_equal(reader.data.read_csv(str(csv_output)).take_all(), union_rows, order_sensitive=False)
    assert_rows_equal(reader.data.read_json(str(json_output)).take_all(), union_rows, order_sensitive=False)
    assert_rows_equal(reader.data.read_parquet(str(parquet_output)).take_all(), second_rows, order_sensitive=False)
