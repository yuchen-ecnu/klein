# SPDX-License-Identifier: Apache-2.0
import json
from datetime import timedelta
from pathlib import Path

from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from tests.support.streaming import LoopSourceFunction


def test_streaming_json_sink_commits_checkpointed_parts(ray_cluster, tmp_path: Path) -> None:
    output = tmp_path / "output"
    config = Configuration()
    config.set(ExecutionOptions.MODE, RuntimeExecutionMode.STREAMING)
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 600)
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
    stream.write_json(str(output), max_rows_per_file=2, concurrency=1)

    handle = context.execute("streaming-transactional-file-sink")
    handle.wait()

    assert handle.status == JobStatus.FINISHED
    rows = [
        json.loads(line)
        for path in sorted(output.glob("*.json"))
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert rows == [{"idx": 1}, {"idx": 2}, {"idx": 3}]
    assert not list(output.rglob("*.pending"))
    assert not list(output.rglob("*.inprogress"))


def test_streaming_sql_filesystem_table_uses_transactional_sink(ray_cluster, tmp_path: Path) -> None:
    output = tmp_path / "sql-output"
    config = Configuration("execution.runtime.mode=streaming; state.backend.type=memory")
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "sql-checkpoints").as_uri())
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
    context = KleinContext(config)
    events = context.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"record_num": 2, "sleep_interval": 0},
        bounded=False,
        num_cpus=0.1,
        concurrency=1,
    )
    context.sql_session.create_temp_view("input_events", events)
    context.execute_sql(
        f"""
        CREATE TABLE output_events (idx BIGINT, scaled BIGINT) WITH (
            'connector'='filesystem',
            'path'='{output}',
            'format'='json',
            'sink.parallelism'='1',
            'sink.filename-prefix'='events',
            'sink.rolling-policy.file-size'='1 MiB'
        )
        """
    )
    context.execute_sql("INSERT INTO output_events SELECT idx, idx * 10 AS scaled FROM input_events")

    handle = context.execute("streaming-sql-transactional-file-sink")
    handle.wait()

    assert handle.status == JobStatus.FINISHED
    rows = [
        json.loads(line)
        for path in sorted(output.glob("events-*.json"))
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    assert rows == [{"idx": 1, "scaled": 10}, {"idx": 2, "scaled": 20}]
