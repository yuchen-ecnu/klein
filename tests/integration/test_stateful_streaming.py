# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta
from pathlib import Path

import pytest

from ray.klein import WatermarkStrategy
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.api.tumbling_window import TumblingWindow
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.config.state_options import StateOptions
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.state.object_store_snapshot_cache import ObjectStoreSnapshotCache
from ray.klein.state.value_state_descriptor import ValueStateDescriptor
from tests.support.terminal import execute_terminal


@pytest.fixture(autouse=True)
def debug_mode(monkeypatch) -> None:
    from ray.klein._internal.ray import KLEIN_DEBUG_OBJECT_STORE

    monkeypatch.setenv(EnvironmentVariables.DEBUG, "1")
    KLEIN_DEBUG_OBJECT_STORE.clear()
    yield
    KLEIN_DEBUG_OBJECT_STORE.clear()


def _context() -> KleinContext:
    config = Configuration()
    config.set(StateOptions.BACKEND, "memory")
    return KleinContext(config)


class _ReplayCollectionSource(SourceFunction):
    """Finite source that deliberately replays its input after restoration.

    This keeps the rescale test focused on managed keyed state. Production
    collection sources checkpoint their cursor and therefore correctly emit no
    duplicate records after a completed savepoint is restored.
    """

    def __init__(self, values):
        self._values = values

    def run(self, context: SourceContext) -> None:
        for value in self._values:
            context.collect(value)

    def cancel(self) -> None:
        return None

    def snapshot_state(self, checkpoint_id: int) -> None:
        return None

    def restore_state(self, state) -> None:
        if state is not None:
            raise ValueError("replay collection source state must be None")


def test_large_snapshot_uses_real_ray_object_store():
    import ray

    cache = ObjectStoreSnapshotCache(
        ray.put,
        ray.get,
        min_size_bytes=1,
    )

    reference = cache.cache(b"immutable-rocks-checkpoint")

    assert reference.inline_payload is None
    assert reference.object_ref is not None
    assert cache.materialize(reference) == b"immutable-rocks-checkpoint"


def test_keyed_process_public_api_uses_default_memory_backend():
    descriptor = ValueStateDescriptor("total")

    def running_total(row, context):
        state = context.state(descriptor)
        total = (state.value or 0) + row["value"]
        state.value = total
        return {"key": row["key"], "total": total}

    config = Configuration()
    context = KleinContext(config)
    sink = (
        context.from_values(
            {"key": "a", "value": 1},
            {"key": "a", "value": 2},
            {"key": "b", "value": 4},
        )
        .key_by(lambda row: row["key"])
        .process(running_total)
        .take_all()
    )
    result = execute_terminal(sink, job_name="keyed-process-default-backend")

    assert result == [
        {"key": "a", "total": 1},
        {"key": "a", "total": 3},
        {"key": "b", "total": 4},
    ]


def test_window_public_api():
    context = _context()
    windowed = (
        context.from_values(
            {"key": "a", "value": 1, "ts": 100},
            {"key": "a", "value": 2, "ts": 200},
        )
        .key_by(lambda row: row["key"])
        .window(
            TumblingWindow(timedelta(seconds=1)),
            timestamp_selector=lambda row: row["ts"],
        )
        .reduce(
            lambda left, right: {
                "key": left["key"],
                "value": left["value"] + right["value"],
                "ts": right["ts"],
            }
        )
    )
    result = execute_terminal(windowed.take_all(), job_name="tumbling-window")
    assert result == [{"key": "a", "value": 3, "ts": 200}]


def test_watermark_strategy_drives_multiple_event_time_windows():
    context = _context()
    sink = (
        context.from_values(
            {"key": "a", "value": 1, "ts": 100},
            {"key": "a", "value": 2, "ts": 1200},
        )
        .assign_timestamps_and_watermarks(WatermarkStrategy.for_monotonous_timestamps(lambda row: row["ts"]))
        .key_by(lambda row: row["key"])
        .window(
            TumblingWindow(timedelta(seconds=1)),
            timestamp_selector=lambda row: row["ts"],
        )
        .reduce(lambda left, right: right)
        .take_all()
    )
    result = execute_terminal(sink, job_name="watermark-windows")

    assert result == [
        {"key": "a", "value": 1, "ts": 100},
        {"key": "a", "value": 2, "ts": 1200},
    ]


def test_interval_join_public_api_handles_either_input_arrival_order():
    context = _context()
    left = context.from_values({"key": "a", "left": 1, "ts": 100})
    right = context.from_values(
        {"key": "a", "right": 10, "ts": 105},
        {"key": "a", "right": 100, "ts": 200},
    )
    sink = left.join(
        right,
        left_key=lambda row: row["key"],
        right_key=lambda row: row["key"],
        left_timestamp=lambda row: row["ts"],
        right_timestamp=lambda row: row["ts"],
        lower_bound=timedelta(milliseconds=-10),
        upper_bound=timedelta(milliseconds=10),
        join_function=lambda left_row, right_row: {
            "key": left_row["key"],
            "total": left_row["left"] + right_row["right"],
        },
    ).take_all()
    result = execute_terminal(sink, job_name="interval-join")

    assert result == [{"key": "a", "total": 11}]


def test_terminal_checkpoint_contains_durable_managed_state(tmp_path: Path):
    config = Configuration()
    config.set(StateOptions.BACKEND, "memory")
    config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    context = KleinContext(config)

    sink = (
        context.from_values(
            {"key": "a", "value": 1, "ts": 100},
            {"key": "a", "value": 2, "ts": 200},
        )
        .key_by(lambda row: row["key"])
        .window(
            TumblingWindow(timedelta(seconds=1)),
            timestamp_selector=lambda row: row["ts"],
        )
        .reduce(
            lambda left, right: {
                "key": left["key"],
                "value": left["value"] + right["value"],
                "ts": right["ts"],
            }
        )
        .take_all()
    )
    result = execute_terminal(sink, job_name="durable-managed-state")

    job_id = next(tmp_path.iterdir()).name
    checkpoint = checkpoint_io.latest_checkpoint(
        tmp_path.as_uri(),
        job_id,
    )
    entries = checkpoint_io.restore_operator_state_entries(checkpoint)
    assert result == [{"key": "a", "value": 3, "ts": 200}]
    assert len(entries) == 1
    entry = next(iter(entries.values()))
    assert checkpoint_io.read_operator_state(checkpoint, entry)


def test_durable_keyed_state_restores_after_parallelism_rescale(tmp_path: Path):
    descriptor = ValueStateDescriptor("rescale-total")

    def running_total(row, context):
        state = context.state(descriptor)
        total = (state.value or 0) + row["value"]
        state.value = total
        return {"key": row["key"], "total": total}

    rows = [{"key": f"key-{index}", "value": 1} for index in range(20)]
    first_config = Configuration()
    first_config.set(StateOptions.BACKEND, "memory")
    first_config.set(StateOptions.MAX_PARALLELISM, 16)
    first_config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    first_config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    first_context = KleinContext(first_config)
    first_sink = (
        first_context.source(
            _ReplayCollectionSource,
            fn_constructor_args=[rows],
        )
        .key_by(lambda row: row["key"])
        .process(running_total, concurrency=2)
        .take_all()
    )
    first_result = execute_terminal(first_sink, job_name="rescale-before-restore")
    assert {item["key"]: item["total"] for item in first_result} == {row["key"]: 1 for row in rows}

    first_job_id = next(tmp_path.iterdir()).name
    checkpoint = checkpoint_io.latest_checkpoint(
        tmp_path.as_uri(),
        first_job_id,
    )
    assert checkpoint is not None

    # Debug mode has one in-process actor registry without Ray namespaces. The
    # first job is complete, so clear its actors before submitting the restored
    # graph, matching the per-job namespace isolation used in production.
    from ray.klein._internal.ray import KLEIN_DEBUG_OBJECT_STORE

    KLEIN_DEBUG_OBJECT_STORE.clear()

    second_config = Configuration()
    second_config.set(StateOptions.BACKEND, "memory")
    second_config.set(StateOptions.MAX_PARALLELISM, 16)
    second_config.set(CheckpointOptions.DIRECTORY, tmp_path.as_uri())
    second_config.set(CheckpointOptions.RESTORE_PATH, checkpoint)
    second_context = KleinContext(second_config)
    second_sink = (
        second_context.source(
            _ReplayCollectionSource,
            fn_constructor_args=[rows],
        )
        .key_by(lambda row: row["key"])
        .process(running_total, concurrency=3)
        .take_all()
    )
    second_result = execute_terminal(second_sink, job_name="rescale-after-restore")

    assert {item["key"]: item["total"] for item in second_result} == {row["key"]: 2 for row in rows}
