# SPDX-License-Identifier: Apache-2.0
"""Debug-runtime integration tests; collection actions still use a Ray queue."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import patch

import numpy
import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.config.udf_options import UDFOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.runtime.operator.error_handling import handle_udf_exception
from tests.support.streaming import LoopSourceFunction
from tests.support.terminal import execute_terminal


@pytest.fixture(autouse=True)
def debug_mode(monkeypatch) -> None:
    monkeypatch.setenv(EnvironmentVariables.DEBUG, "1")


def _context(*, ignore_udf_errors: bool = False) -> KleinContext:
    config = Configuration()
    config.set(UDFOptions.IGNORE_EXCEPTIONS, ignore_udf_errors)
    return KleinContext(config)


def test_take_stops_after_the_requested_limit() -> None:
    config = Configuration()
    config.set(ExecutionOptions.MODE, RuntimeExecutionMode.STREAMING)
    context = KleinContext(config)
    stream = context.source(LoopSourceFunction, num_cpus=0.1).map(
        lambda row: {"idx": numpy.array(row["idx"]) * 2},
        num_cpus=0.1,
        batch_size=2,
    )

    actual = execute_terminal(stream.take(5), job_name="streaming-take-limit")
    assert actual == [{"idx": 2}, {"idx": 4}, {"idx": 6}, {"idx": 8}, {"idx": 10}]


@pytest.mark.parametrize("limit", [None, 1_000])
def test_finite_source_take_actions_stop_at_eof(limit) -> None:
    context = _context()
    stream = context.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"record_num": 5},
        num_cpus=0.1,
    )

    sink = stream.take_all() if limit is None else stream.take(limit)
    actual = execute_terminal(sink, job_name=f"finite-source-take-{limit}")

    assert actual == [{"idx": 1}, {"idx": 2}, {"idx": 3}, {"idx": 4}, {"idx": 5}]


def test_map_reduce_returns_grouped_batches() -> None:
    config = Configuration()
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
    context = KleinContext(config)

    def preprocess(row):
        for comment in row["comment_list"]:
            yield {"note_id": row["note_id"], "comment_input_id": list(range(len(comment)))}

    def infer(batch):
        return {
            "note_id": batch["note_id"],
            "comment_embeddings": [[token * -2 for token in tokens] for tokens in batch["comment_input_id"]],
        }

    sink = (
        context.from_values(
            {"note_id": "111", "comment_list": ["abc", "abcde"]},
            {"note_id": "222", "comment_list": ["abcdef"]},
        )
        .map_reduce(
            key_selector=lambda row: row["note_id"],
            preprocess_fn=preprocess,
            batch_process_fn=infer,
            postprocess_fn=lambda row: row,
            concurrency=(1, 2, 2),
            batch_process_size=3,
        )
        .take_all()
    )
    actual = execute_terminal(sink, job_name="map-reduce-batches")

    assert sorted(actual, key=lambda row: row["note_id"][0]) == [
        {"note_id": ["111", "111"], "comment_embeddings": [[0, -2, -4], [0, -2, -4, -6, -8]]},
        {"note_id": ["222"], "comment_embeddings": [[0, -2, -4, -6, -8, -10]]},
    ]


def test_map_ignores_only_failing_rows() -> None:
    context = _context(ignore_udf_errors=True)

    def require_adult(row):
        if row["age"] < 18:
            raise ValueError("age < 18")
        return row

    sink = context.from_values({"name": "Jack", "age": 3}, {"name": "Lucy", "age": 18}).map(require_adult).take_all()
    actual = execute_terminal(sink, job_name="ignore-map-failure")

    assert actual == [{"name": "Lucy", "age": 18}]


def test_flat_map_keeps_values_emitted_before_a_failure() -> None:
    context = _context(ignore_udf_errors=True)

    def emit_until_two(row):
        for item in row["items"]:
            if item == "2":
                raise ValueError("item == 2")
            yield {"name": row["name"], "item": item}

    sink = (
        context.from_values(
            {"name": "Jack", "items": ["1", "2", "3"]},
            {"name": "Lucy", "items": ["4", "5", "6"]},
        )
        .flat_map(emit_until_two)
        .take_all()
    )
    actual = execute_terminal(sink, job_name="partial-flat-map-failure")

    assert actual == [
        {"name": "Jack", "item": "1"},
        {"name": "Lucy", "item": "4"},
        {"name": "Lucy", "item": "5"},
        {"name": "Lucy", "item": "6"},
    ]


def test_map_batches_drops_only_the_failing_batch() -> None:
    context = _context(ignore_udf_errors=True)

    def adults_only(batch):
        if any(age < 18 for age in batch["age"]):
            raise ValueError("batch contains age < 18")
        return batch

    sink = (
        context.from_values(
            {"name": "Jack", "age": 3},
            {"name": "Lucy", "age": 18},
            {"name": "Tom", "age": 25},
        )
        .map_batches(adults_only, batch_size=2)
        .take_all()
    )
    actual = execute_terminal(sink, job_name="ignore-map-batch-failure")

    assert actual == [{"name": "Tom", "age": 25}]


def test_async_map_drops_only_the_failing_row() -> None:
    context = _context(ignore_udf_errors=True)

    async def double(row):
        await asyncio.sleep(0)
        if row["value"] == 1:
            raise ValueError("value == 1")
        return {"value": row["value"] * 2}

    sink = context.from_values({"value": 1}, {"value": 2}, {"value": 3}).map(double, async_buffer_size=10).take_all()
    actual = execute_terminal(sink, job_name="ignore-async-map-failure")

    assert actual == [{"value": 4}, {"value": 6}]


def test_async_map_batches_drops_only_the_failing_batch() -> None:
    context = _context(ignore_udf_errors=True)

    async def double(batch):
        await asyncio.sleep(0)
        if 1 in batch["value"]:
            raise ValueError("batch contains value == 1")
        return {"value": [value * 2 for value in batch["value"]]}

    sink = (
        context.from_values({"value": 1}, {"value": 2}, {"value": 3}, {"value": 4})
        .map_batches(double, batch_size=2, async_buffer_size=10)
        .take_all()
    )
    actual = execute_terminal(sink, job_name="ignore-async-batch-failure")

    assert actual == [{"value": 6}, {"value": 8}]


def test_ignored_udf_failures_record_metrics() -> None:
    context = _context(ignore_udf_errors=True)

    def require_adult(row):
        if row["age"] < 18:
            raise ValueError("age < 18")
        return row

    with patch(
        "ray.klein.runtime.operator.operator.handle_udf_exception",
        wraps=handle_udf_exception,
    ) as mocked_handler:
        sink = (
            context.from_values(
                {"name": "Jack", "age": 3},
                {"name": "Tom", "age": 10},
                {"name": "Lucy", "age": 18},
            )
            .map(require_adult)
            .take_all()
        )
        actual = execute_terminal(sink, job_name="ignored-failure-metrics")

    assert actual == [{"name": "Lucy", "age": 18}]
    assert mocked_handler.call_count == 2


def test_record_threshold_triggers_source_snapshot() -> None:
    config = Configuration()
    config.set(CheckpointOptions.MAX_CONCURRENT, 500)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 50)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
    context = KleinContext(config)
    stream = context.source(
        LoopSourceFunction,
        concurrency=2,
        fn_constructor_kwargs={"record_num": 200, "sleep_interval": 0},
    )
    sink = stream.write(
        ConsoleSinkFunction,
        concurrency=2,
        fn_constructor_kwargs={"limit": 0},
    )

    snapshots: list[int] = []
    original_snapshot = LoopSourceFunction.snapshot_state

    def tracked_snapshot(source: LoopSourceFunction, checkpoint_id: int) -> int:
        # Replace the function with another descriptor instead of a MagicMock.
        # MagicMock does not bind ``self`` when installed on a class, which can
        # fail only after the source thread reaches its first checkpoint.
        snapshots.append(checkpoint_id)
        return original_snapshot(source, checkpoint_id)

    with patch.object(LoopSourceFunction, "snapshot_state", tracked_snapshot):
        client = context.execute("debug-progress-snapshot", sinks=(sink,))
        client.wait()

    assert snapshots
