# SPDX-License-Identifier: Apache-2.0
import sqlite3
from datetime import timedelta
from functools import partial
from pathlib import Path

from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from tests.support.streaming import LoopSourceFunction


def _connect_sqlite(database: str) -> sqlite3.Connection:
    return sqlite3.connect(database, timeout=30)


def test_write_sql_runs_as_a_streaming_sink(ray_cluster, tmp_path: Path) -> None:
    database = tmp_path / "streaming-sql.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events(id)")

    config = Configuration("execution.runtime.mode=streaming; state.backend.type=memory")
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
    context = KleinContext(config)
    stream = context.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"record_num": 3, "sleep_interval": 0},
        bounded=False,
        num_cpus=0.1,
        concurrency=1,
    )
    stream.write_sql(
        "INSERT INTO events VALUES(?)",
        partial(_connect_sqlite, str(database)),
        ray_remote_args={"num_cpus": 0.1},
        concurrency=1,
    )

    handle = context.execute("streaming-sql-sink")
    handle.wait()

    assert handle.status == JobStatus.FINISHED
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT id FROM events ORDER BY id").fetchall() == [(1,), (2,), (3,)]
