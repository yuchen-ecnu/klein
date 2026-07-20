# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta
from pathlib import Path

from pyiceberg import schema as iceberg_schema
from pyiceberg import types as iceberg_types
from pyiceberg.catalog import load_catalog

from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from tests.support.streaming import LoopSourceFunction


def test_write_iceberg_runs_as_a_checkpointed_streaming_sink(ray_cluster, tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog_kwargs = {
        "name": "klein_integration",
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": warehouse.as_uri(),
    }
    options = dict(catalog_kwargs)
    catalog_name = options.pop("name")
    catalog = load_catalog(catalog_name, **options)
    catalog.create_namespace("analytics")
    catalog.create_table(
        "analytics.events",
        schema=iceberg_schema.Schema(
            iceberg_types.NestedField(
                field_id=1,
                name="idx",
                field_type=iceberg_types.LongType(),
                required=True,
            )
        ),
    )

    config = Configuration()
    config.set(ExecutionOptions.MODE, RuntimeExecutionMode.STREAMING)
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
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
    stream.write_iceberg(
        "analytics.events",
        catalog_kwargs=catalog_kwargs,
        snapshot_properties={"application": "klein-integration-test"},
        ray_remote_args={"num_cpus": 0.1},
        concurrency=1,
    )

    handle = context.execute("streaming-iceberg-sink")
    handle.wait()

    assert handle.status == JobStatus.FINISHED
    table = load_catalog(catalog_name, **options).load_table("analytics.events")
    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["idx"])
    assert rows == [{"idx": 1}, {"idx": 2}, {"idx": 3}]
    summaries = [snapshot.summary for snapshot in table.metadata.snapshots]
    assert all(summary.get("application") == "klein-integration-test" for summary in summaries)
