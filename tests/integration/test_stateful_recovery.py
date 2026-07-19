# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path

import pytest

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.node_type import NodeType
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.runtime.coordinator import checkpoint_io
from ray.klein.state.value_state_descriptor import ValueStateDescriptor
from tests.support.streaming import ReplayCollectionSource


def _configuration(tmp_path: Path, backend: str, restore_path: str | None = None) -> Configuration:
    config = Configuration()
    config.set(StateOptions.BACKEND, backend)
    config.set(StateOptions.LOCAL_DIRECTORY, str(tmp_path / "local-state"))
    config.set(StateOptions.MAX_PARALLELISM, 16)
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    if restore_path is not None:
        config.set(CheckpointOptions.RESTORE_PATH, restore_path)
    return config


def _run_stateful_job(config: Configuration, rows: list[dict[str, object]], concurrency: int, job_name: str):
    descriptor = ValueStateDescriptor("running-total")

    def running_total(row, context):
        state = context.state(descriptor)
        total = (state.value or 0) + row["value"]
        state.value = total
        return {"key": row["key"], "total": total}

    context = KleinContext(config)
    result = (
        context.source(
            ReplayCollectionSource,
            fn_constructor_args=[rows],
            bounded=True,
            name="ReplayInput",
        )
        .key_by(lambda row: row["key"])
        .process(running_total, concurrency=concurrency, name="RunningTotal")
    )
    result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="CollectTotals")

    handle = context.execute(job_name)
    handle.wait()
    return handle.get()


@pytest.mark.parametrize("backend", ["memory", "rocksdb"])
def test_real_ray_restores_keyed_state_across_rescale(tmp_path: Path, backend: str) -> None:
    """Exercise durable state restore through real Ray actors, not debug mode."""

    rows = [{"key": f"key-{index}", "value": 1} for index in range(20)]
    first_config = _configuration(tmp_path, backend)
    first_result = _run_stateful_job(first_config, rows, concurrency=2, job_name=f"state-{backend}-before")
    assert {item["key"]: item["total"] for item in first_result} == {row["key"]: 1 for row in rows}

    checkpoint_root = tmp_path / "checkpoints"
    first_job_id = next(checkpoint_root.iterdir()).name
    checkpoint = checkpoint_io.latest_checkpoint(checkpoint_root.as_uri(), first_job_id)
    assert checkpoint is not None

    restored_config = _configuration(tmp_path, backend, checkpoint)
    restored_result = _run_stateful_job(
        restored_config,
        rows,
        concurrency=3,
        job_name=f"state-{backend}-after",
    )
    assert {item["key"]: item["total"] for item in restored_result} == {row["key"]: 2 for row in rows}
