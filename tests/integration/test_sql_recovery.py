# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.node_type import NodeType
from ray.klein.api.row_kind import RowKind
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.config.state_options import StateOptions
from ray.klein.runtime.coordinator import checkpoint_io
from tests.support.streaming import ReplayCollectionSource


def _configuration(tmp_path: Path, restore_path: str | None = None) -> Configuration:
    config = Configuration()
    config.set(ExecutionOptions.MODE, RuntimeExecutionMode.STREAMING)
    config.set(StateOptions.BACKEND, "memory")
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    if restore_path is not None:
        config.set(CheckpointOptions.RESTORE_PATH, restore_path)
    return config


def _run_sql_job(config: Configuration, job_name: str):
    context = KleinContext(config)
    events = context.source(
        ReplayCollectionSource,
        fn_constructor_args=[[{"key": "a", "amount": 1}, {"key": "a", "amount": 2}]],
        bounded=True,
        name="SqlReplayInput",
    )
    changes = context.sql(
        "SELECT key, SUM(amount) AS total FROM events GROUP BY key",
        tables={"events": events},
    )
    changes.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="CollectSqlChanges")

    handle = context.execute(job_name)
    handle.wait()
    return handle.get()


def test_streaming_sql_aggregate_restores_managed_state(tmp_path: Path) -> None:
    first_result = _run_sql_job(_configuration(tmp_path), "sql-state-before")
    assert [(row.row_kind, row["total"]) for row in first_result] == [
        (RowKind.INSERT, 1),
        (RowKind.UPDATE_BEFORE, 1),
        (RowKind.UPDATE_AFTER, 3),
    ]

    checkpoint_root = tmp_path / "checkpoints"
    first_job_id = next(checkpoint_root.iterdir()).name
    checkpoint = checkpoint_io.latest_checkpoint(checkpoint_root.as_uri(), first_job_id)
    assert checkpoint is not None

    restored_result = _run_sql_job(_configuration(tmp_path, checkpoint), "sql-state-after")
    assert [(row.row_kind, row["total"]) for row in restored_result] == [
        (RowKind.UPDATE_BEFORE, 3),
        (RowKind.UPDATE_AFTER, 4),
        (RowKind.UPDATE_BEFORE, 4),
        (RowKind.UPDATE_AFTER, 6),
    ]
