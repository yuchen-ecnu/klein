# SPDX-License-Identifier: Apache-2.0
import asyncio
import pickle
import threading
from types import SimpleNamespace

import pyarrow as pa
import pytest
from pyiceberg import schema as iceberg_schema
from pyiceberg import types as iceberg_types
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException
from pyiceberg.table import Table

from ray.klein.config.configuration import Configuration
from ray.klein.integrations.iceberg import iceberg_sink_committable
from ray.klein.integrations.iceberg.iceberg_global_committable import (
    IcebergGlobalCommittable,
    combine_iceberg_committables,
)
from ray.klein.integrations.iceberg.iceberg_sink_committable import (
    TRANSACTION_ID_SNAPSHOT_PROPERTY,
    IcebergSinkCommittable,
)
from ray.klein.integrations.iceberg.streaming_iceberg_sink import StreamingIcebergSink
from ray.klein.runtime.coordinator.checkpoint_coordinator import CheckpointCoordinator
from ray.klein.state.sink_committable_checkpoint_entry import SinkCommittableCheckpointEntry


def _catalog_kwargs(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return {
        "name": "klein_test",
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
        "warehouse": warehouse.as_uri(),
    }


def _create_table(tmp_path):
    catalog_kwargs = _catalog_kwargs(tmp_path)
    options = dict(catalog_kwargs)
    name = options.pop("name")
    catalog = load_catalog(name, **options)
    catalog.create_namespace("analytics")
    catalog.create_table(
        "analytics.events",
        schema=iceberg_schema.Schema(
            iceberg_types.NestedField(
                field_id=1,
                name="id",
                field_type=iceberg_types.LongType(),
                required=True,
            ),
            iceberg_types.NestedField(
                field_id=2,
                name="name",
                field_type=iceberg_types.StringType(),
                required=False,
            ),
            identifier_field_ids=[1],
        ),
    )
    return catalog_kwargs


def _opened_sink(catalog_kwargs, *, task_index=2, **options):
    sink = StreamingIcebergSink(
        "analytics.events",
        catalog_kwargs=catalog_kwargs,
        **options,
    )
    sink.open(SimpleNamespace(task_index=task_index, job_id="iceberg-job"))
    return sink


def _rows(catalog_kwargs):
    options = dict(catalog_kwargs)
    name = options.pop("name")
    table = load_catalog(name, **options).load_table("analytics.events")
    return sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])


def test_checkpoint_commit_is_invisible_until_commit_and_idempotent(tmp_path) -> None:
    catalog_kwargs = _create_table(tmp_path)
    sink = _opened_sink(catalog_kwargs, snapshot_properties={"application": "test"})
    sink.write({"id": 1, "name": "first"})
    sink.write({"id": 2, "name": "second"})

    committable = sink.prepare_commit(7)

    assert isinstance(committable, IcebergSinkCommittable)
    assert _rows(catalog_kwargs) == []
    pickle.loads(pickle.dumps(committable)).commit()
    committable.commit()

    assert _rows(catalog_kwargs) == [
        {"id": 1, "name": "first"},
        {"id": 2, "name": "second"},
    ]
    options = dict(catalog_kwargs)
    name = options.pop("name")
    table = load_catalog(name, **options).load_table("analytics.events")
    summaries = [snapshot.summary for snapshot in table.metadata.snapshots]
    assert (
        sum(summary.get(TRANSACTION_ID_SNAPSHOT_PROPERTY) == committable.transaction_id for summary in summaries) == 1
    )
    assert summaries[-1].get("application") == "test"


@pytest.mark.asyncio
async def test_overlapping_retry_serializes_check_and_append(monkeypatch, tmp_path) -> None:
    """A timed-out background append and its retry publish one snapshot."""

    catalog_kwargs = _create_table(tmp_path)
    sink = _opened_sink(catalog_kwargs)
    sink.write({"id": 1, "name": "first"})
    committable = sink.prepare_commit(7)
    assert isinstance(committable, IcebergSinkCommittable)

    original_append = Table.append
    activity_lock = threading.Lock()
    append_entered = threading.Event()
    release_append = threading.Event()
    retry_waiting = threading.Event()
    append_finished = threading.Event()
    table_lock = iceberg_sink_committable._table_commit_lock(committable.table_identifier)
    assert iceberg_sink_committable._table_commit_lock(committable.table_identifier) is table_lock

    lock_attempts = 0
    active = 0
    max_active = 0
    append_calls = 0

    class ObservedTableLock:
        def __enter__(self):
            nonlocal active, lock_attempts, max_active
            with activity_lock:
                lock_attempts += 1
                if lock_attempts == 2:
                    retry_waiting.set()
            table_lock.acquire()
            with activity_lock:
                active += 1
                max_active = max(max_active, active)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            nonlocal active
            with activity_lock:
                active -= 1
            table_lock.release()

    observed_table_lock = ObservedTableLock()

    def observed_table_commit_lock(table_identifier):
        assert table_identifier == committable.table_identifier
        return observed_table_lock

    def observed_append(table, *args, **kwargs):
        nonlocal append_calls
        with activity_lock:
            append_calls += 1
            call_number = append_calls
        if call_number == 1:
            append_entered.set()
            if not release_append.wait(timeout=5):
                raise TimeoutError("test did not release the timed-out Iceberg append")
        try:
            return original_append(table, *args, **kwargs)
        finally:
            append_finished.set()

    monkeypatch.setattr(iceberg_sink_committable, "_table_commit_lock", observed_table_commit_lock)
    monkeypatch.setattr(Table, "append", observed_append)

    first_commit = asyncio.create_task(
        asyncio.wait_for(
            asyncio.to_thread(committable.commit),
            timeout=0.1,
        )
    )
    assert await asyncio.to_thread(append_entered.wait, 2)
    with pytest.raises(asyncio.TimeoutError):
        await first_commit

    retry = asyncio.create_task(asyncio.to_thread(committable.commit))
    assert await asyncio.to_thread(retry_waiting.wait, 2)
    with activity_lock:
        assert active == 1
        assert max_active == 1

    release_append.set()
    await asyncio.wait_for(retry, timeout=5)

    assert append_finished.is_set()
    assert lock_attempts == 2
    assert append_calls == 1
    assert max_active == 1
    assert _rows(catalog_kwargs) == [{"id": 1, "name": "first"}]
    options = dict(catalog_kwargs)
    name = options.pop("name")
    table = load_catalog(name, **options).load_table("analytics.events")
    matching = [
        snapshot
        for snapshot in table.metadata.snapshots
        if snapshot.summary.get(TRANSACTION_ID_SNAPSHOT_PROPERTY) == committable.transaction_id
    ]
    assert len(matching) == 1


@pytest.mark.parametrize(
    "error_type",
    [CommitFailedException, CommitStateUnknownException],
)
@pytest.mark.parametrize("marker_exists", [True, False], ids=["marker-present", "marker-absent"])
def test_optimistic_commit_conflict_requires_fresh_transaction_marker(
    monkeypatch,
    error_type,
    marker_exists,
) -> None:
    transaction_id = "iceberg-transaction"
    error = error_type("optimistic conflict")
    arrow_table = SimpleNamespace(num_rows=1, schema=object())

    def fail_append(*args, **kwargs):
        raise error

    initial_table = SimpleNamespace(
        metadata=SimpleNamespace(snapshots=[]),
        append=fail_append,
    )
    refreshed_snapshots = (
        [SimpleNamespace(summary={TRANSACTION_ID_SNAPSHOT_PROPERTY: transaction_id})] if marker_exists else []
    )
    refreshed_table = SimpleNamespace(metadata=SimpleNamespace(snapshots=refreshed_snapshots))
    tables = iter((initial_table, refreshed_table))
    load_calls = 0

    class Catalog:
        def load_table(self, table_identifier):
            nonlocal load_calls
            assert table_identifier == "analytics.events"
            load_calls += 1
            return next(tables)

    monkeypatch.setattr(iceberg_sink_committable, "_load_catalog", lambda options: Catalog())
    monkeypatch.setattr(
        iceberg_sink_committable,
        "_concatenate_arrow_payloads",
        lambda payloads: arrow_table,
    )
    monkeypatch.setattr(iceberg_sink_committable, "_evolve_top_level_schema", lambda table, schema: False)
    monkeypatch.setattr(iceberg_sink_committable, "_align_arrow_schema", lambda table, incoming: incoming)

    def commit() -> None:
        iceberg_sink_committable._commit_arrow_payloads_locked(
            table_identifier="analytics.events",
            catalog_kwargs=None,
            snapshot_properties=None,
            arrow_ipcs=(b"payload",),
            transaction_id=transaction_id,
        )

    if marker_exists:
        commit()
    else:
        with pytest.raises(error_type) as raised:
            commit()
        assert raised.value is error

    assert load_calls == 2


def test_non_conflict_append_error_propagates_without_refresh(monkeypatch) -> None:
    error = RuntimeError("append failed")
    arrow_table = SimpleNamespace(num_rows=1, schema=object())

    def fail_append(*args, **kwargs):
        raise error

    table = SimpleNamespace(
        metadata=SimpleNamespace(snapshots=[]),
        append=fail_append,
    )
    load_calls = 0

    class Catalog:
        def load_table(self, table_identifier):
            nonlocal load_calls
            assert table_identifier == "analytics.events"
            load_calls += 1
            return table

    monkeypatch.setattr(iceberg_sink_committable, "_load_catalog", lambda options: Catalog())
    monkeypatch.setattr(
        iceberg_sink_committable,
        "_concatenate_arrow_payloads",
        lambda payloads: arrow_table,
    )
    monkeypatch.setattr(iceberg_sink_committable, "_evolve_top_level_schema", lambda current, schema: False)
    monkeypatch.setattr(iceberg_sink_committable, "_align_arrow_schema", lambda current, incoming: incoming)

    with pytest.raises(RuntimeError) as raised:
        iceberg_sink_committable._commit_arrow_payloads_locked(
            table_identifier="analytics.events",
            catalog_kwargs=None,
            snapshot_properties=None,
            arrow_ipcs=(b"payload",),
            transaction_id="iceberg-transaction",
        )

    assert raised.value is error
    assert load_calls == 1


def test_global_committable_publishes_all_writer_batches_in_one_snapshot(tmp_path) -> None:
    catalog_kwargs = _create_table(tmp_path)
    first_sink = _opened_sink(
        catalog_kwargs,
        task_index=0,
        snapshot_properties={"application": "test"},
    )
    second_sink = _opened_sink(
        catalog_kwargs,
        task_index=1,
        snapshot_properties={"application": "test"},
    )
    first_sink.write({"id": 1, "name": "first"})
    # Parallel writers can observe a schema transition at different record
    # boundaries. The global append must still union by name and evolve once.
    second_sink.write({"id": 2, "name": "second", "category": "new"})
    first = first_sink.prepare_commit(7)
    second = second_sink.prepare_commit(7)
    assert isinstance(first, IcebergSinkCommittable)
    assert isinstance(second, IcebergSinkCommittable)

    coordinator = CheckpointCoordinator(Configuration(include_environment=False), job_id="iceberg-job")
    coalesced = coordinator._coalesce_sink_committables(
        7,
        {
            "4:1": SinkCommittableCheckpointEntry("4:1", 7, second),
            "4:0": SinkCommittableCheckpointEntry("4:0", 7, first),
        },
    )
    assert tuple(coalesced) == ("4:global",)
    committable = coalesced["4:global"].committable

    assert isinstance(committable, IcebergGlobalCommittable)
    assert committable.writer_transaction_ids == tuple(sorted((first.transaction_id, second.transaction_id)))
    assert _rows(catalog_kwargs) == []
    pickle.loads(pickle.dumps(committable)).commit()
    committable.commit()

    assert _rows(catalog_kwargs) == [
        {"id": 1, "name": "first", "category": None},
        {"id": 2, "name": "second", "category": "new"},
    ]
    options = dict(catalog_kwargs)
    name = options.pop("name")
    table = load_catalog(name, **options).load_table("analytics.events")
    summaries = [snapshot.summary for snapshot in table.metadata.snapshots]
    assert len(summaries) == 1
    assert summaries[0].get(TRANSACTION_ID_SNAPSHOT_PROPERTY) == committable.transaction_id
    assert summaries[0].get("application") == "test"


def test_global_committable_rejects_incompatible_or_duplicate_writers(tmp_path) -> None:
    catalog_kwargs = _create_table(tmp_path)
    first_sink = _opened_sink(catalog_kwargs, task_index=0)
    second_sink = _opened_sink(
        catalog_kwargs,
        task_index=1,
        snapshot_properties={"application": "other"},
    )
    first_sink.write({"id": 1, "name": "first"})
    second_sink.write({"id": 2, "name": "second"})
    first = first_sink.prepare_commit(7)
    second = second_sink.prepare_commit(7)
    assert isinstance(first, IcebergSinkCommittable)
    assert isinstance(second, IcebergSinkCommittable)

    with pytest.raises(ValueError, match="different snapshot properties"):
        combine_iceberg_committables((first, second), transaction_id="global-7")
    with pytest.raises(ValueError, match="duplicate writer transactions"):
        combine_iceberg_committables((first, first), transaction_id="global-7")
    with pytest.raises(ValueError, match="empty set"):
        combine_iceberg_committables((), transaction_id="global-7")


def test_streaming_commit_evolves_new_top_level_columns(tmp_path) -> None:
    catalog_kwargs = _create_table(tmp_path)
    sink = _opened_sink(catalog_kwargs)
    sink.write({"id": 1, "name": "first", "category": "new"})

    committable = sink.prepare_commit(1)
    assert isinstance(committable, IcebergSinkCommittable)
    committable.commit()

    assert _rows(catalog_kwargs) == [{"id": 1, "name": "first", "category": "new"}]
    options = dict(catalog_kwargs)
    name = options.pop("name")
    table = load_catalog(name, **options).load_table("analytics.events")
    assert table.schema().find_field("id").required
    assert set(table.schema().identifier_field_ids) == {1}


def test_streaming_sink_rejects_non_append_modes_and_reserved_property() -> None:
    with pytest.raises(ValueError, match=r"only SaveMode\.APPEND"):
        StreamingIcebergSink("analytics.events", mode="overwrite")
    with pytest.raises(ValueError, match="reserved"):
        StreamingIcebergSink(
            "analytics.events",
            snapshot_properties={TRANSACTION_ID_SNAPSHOT_PROPERTY: "caller-value"},
        )


def test_streaming_sink_rejects_column_changes_and_aborts_buffer(tmp_path) -> None:
    catalog_kwargs = _create_table(tmp_path)
    sink = _opened_sink(catalog_kwargs)
    sink.write({"id": 1, "name": "first"})

    with pytest.raises(ValueError, match="row columns changed"):
        sink.write({"id": 2, "category": "other"})

    sink.abort_current_transaction()
    assert sink.prepare_commit(1) is None


def test_streaming_sink_requires_named_columns() -> None:
    sink = StreamingIcebergSink("analytics.events")

    with pytest.raises(ValueError, match="at least one column"):
        sink.write({})
    with pytest.raises(TypeError, match="column names must be strings"):
        sink.write({1: "value"})


def test_arrow_checkpoint_payload_preserves_typed_values(tmp_path) -> None:
    catalog_kwargs = _create_table(tmp_path)
    sink = _opened_sink(catalog_kwargs)
    sink.write({"id": 1, "name": "first"})
    committable = sink.prepare_commit(1)
    assert isinstance(committable, IcebergSinkCommittable)

    with pa.ipc.open_stream(pa.py_buffer(committable.arrow_ipc)) as reader:
        assert reader.read_all().to_pylist() == [{"id": 1, "name": "first"}]
