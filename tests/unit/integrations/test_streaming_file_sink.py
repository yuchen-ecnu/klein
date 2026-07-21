# SPDX-License-Identifier: Apache-2.0
import csv
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.klein.integrations.filesystem._file_part import FilePart
from ray.klein.integrations.filesystem.file_sink_committable import FileSinkCommittable
from ray.klein.integrations.filesystem.streaming_file_sink import StreamingFileSink


def _opened_sink(tmp_path: Path, data_format: str, **options) -> StreamingFileSink:
    sink = StreamingFileSink(str(tmp_path), data_format, **options)
    sink.open(SimpleNamespace(task_index=2, job_id="job/with spaces"))
    return sink


def _commit(sink: StreamingFileSink, checkpoint_id: int = 1) -> FileSinkCommittable:
    committable = sink.prepare_commit(checkpoint_id)
    assert isinstance(committable, FileSinkCommittable)
    committable.commit()
    return committable


def _json_parts(tmp_path: Path) -> list[list[dict]]:
    return [
        [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        for path in sorted(tmp_path.glob("*.json"))
    ]


def test_json_parts_become_visible_only_after_commit(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json", max_rows_per_file=1)
    sink.write({"id": 1, "name": "first"})
    sink.write({"id": 2, "name": "second"})

    committable = sink.prepare_commit(7)

    assert isinstance(committable, FileSinkCommittable)
    assert len(committable.parts) == 2
    assert not list(tmp_path.glob("*.json"))
    assert all((tmp_path / part.pending_path).is_file() for part in committable.parts)

    committable.commit()
    committable.commit()

    rows = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(tmp_path.glob("*.json"))]
    assert rows == [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]
    assert all(not (tmp_path / part.pending_path).exists() for part in committable.parts)


def test_prepared_transaction_can_be_aborted_idempotently(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")
    sink.write({"id": 1})
    committable = sink.prepare_commit(3)
    assert isinstance(committable, FileSinkCommittable)

    committable.abort()
    committable.abort()

    assert not list(tmp_path.rglob("*.pending"))
    assert not list(tmp_path.glob("*.json"))


def test_close_discards_unprepared_transaction(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")
    sink.write({"id": 1})

    sink.close()

    assert not list(tmp_path.rglob("*.inprogress"))
    assert not list(tmp_path.glob("*.json"))


def test_csv_part_has_schema_order_and_header(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "csv", columns=("name", "id"))
    sink.write({"id": 1, "name": "alice"})
    committable = sink.prepare_commit(1)
    assert isinstance(committable, FileSinkCommittable)
    committable.commit()

    with next(tmp_path.glob("*.csv")).open(newline="", encoding="utf-8") as stream:
        assert list(csv.DictReader(stream)) == [{"name": "alice", "id": "1"}]


def test_text_sink_requires_one_column(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "text", columns=("left", "right"))

    with pytest.raises(ValueError, match="exactly one column"):
        sink.write({"left": "a", "right": "b"})


def test_parquet_part_is_readable_after_commit(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "parquet")
    sink.write({"id": 1, "name": "alice"})
    sink.write({"id": 2, "name": "bob"})
    committable = sink.prepare_commit(9)
    assert isinstance(committable, FileSinkCommittable)

    committable.commit()

    assert pq.read_table(next(tmp_path.glob("*.parquet"))).to_pylist() == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_json_serializes_supported_typed_values(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")
    sink.write(
        {
            "timestamp": datetime(2025, 2, 3, 4, 5, 6, tzinfo=timezone.utc),
            "day": date(2025, 2, 3),
            "amount": Decimal("12.340"),
            "scalar": np.int64(7),
            "array": np.array([[1, 2], [3, 4]], dtype=np.int16),
        }
    )

    _commit(sink)

    assert _json_parts(tmp_path) == [
        [
            {
                "timestamp": "2025-02-03T04:05:06+00:00",
                "day": "2025-02-03",
                "amount": "12.340",
                "scalar": 7,
                "array": [[1, 2], [3, 4]],
            }
        ]
    ]


def test_json_rejects_an_unsupported_value_and_abort_removes_staging(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")

    with pytest.raises(TypeError, match="Object of type object is not JSON serializable"):
        sink.write({"value": object()})

    sink.abort_current_transaction()
    assert not list(tmp_path.rglob("*.inprogress"))
    assert not list(tmp_path.rglob("*.pending"))


@pytest.mark.parametrize(
    ("row", "missing", "extra"),
    [
        ({"id": 2}, ["name"], []),
        ({"id": 2, "name": "second", "category": "new"}, [], ["category"]),
    ],
    ids=["missing-column", "extra-column"],
)
def test_inferred_columns_reject_row_drift(
    tmp_path: Path,
    row: dict,
    missing: list[str],
    extra: list[str],
) -> None:
    sink = _opened_sink(tmp_path, "json")
    sink.write({"id": 1, "name": "first"})

    with pytest.raises(ValueError, match="file sink row does not match columns") as error:
        sink.write(row)

    assert f"missing={missing!r}" in str(error.value)
    assert f"extra={extra!r}" in str(error.value)
    sink.abort_current_transaction()


def test_parquet_schema_is_reused_across_parts_and_rejects_type_drift(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "parquet", max_rows_per_file=1)
    sink.write({"id": 1})
    sink.write({"id": 2})

    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        sink.write({"id": {"not": "an integer"}})

    sink.abort_current_transaction()
    assert not list(tmp_path.rglob("*.inprogress"))
    assert not list(tmp_path.rglob("*.pending"))


def test_row_count_policy_rolls_json_parts(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json", max_rows_per_file=2)
    for row_id in range(1, 4):
        sink.write({"id": row_id})

    _commit(sink)

    assert _json_parts(tmp_path) == [[{"id": 1}, {"id": 2}], [{"id": 3}]]


def test_encoded_byte_policy_rolls_json_parts(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json", max_bytes_per_file=1)
    sink.write({"id": 1})
    sink.write({"id": 2})

    _commit(sink)

    assert _json_parts(tmp_path) == [[{"id": 1}], [{"id": 2}]]


def test_rollover_interval_uses_part_age(monkeypatch, tmp_path: Path) -> None:
    timestamps = iter((10.0, 14.0, 16.0))
    monkeypatch.setattr(
        "ray.klein.integrations.filesystem.streaming_file_sink.time.monotonic",
        lambda: next(timestamps),
    )
    sink = _opened_sink(tmp_path, "json", rollover_interval_seconds=5)
    for row_id in range(1, 4):
        sink.write({"id": row_id})

    _commit(sink)

    assert _json_parts(tmp_path) == [[{"id": 1}, {"id": 2}], [{"id": 3}]]


def test_inactivity_interval_uses_time_since_last_write(monkeypatch, tmp_path: Path) -> None:
    timestamps = iter((10.0, 14.0, 20.0))
    monkeypatch.setattr(
        "ray.klein.integrations.filesystem.streaming_file_sink.time.monotonic",
        lambda: next(timestamps),
    )
    sink = _opened_sink(tmp_path, "json", inactivity_interval_seconds=5)
    for row_id in range(1, 4):
        sink.write({"id": row_id})

    _commit(sink)

    assert _json_parts(tmp_path) == [[{"id": 1}, {"id": 2}], [{"id": 3}]]


def test_empty_prepare_and_flush_are_noops(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")

    sink.flush()

    assert sink.prepare_commit(1) is None
    assert not list(tmp_path.rglob("*"))


def test_flush_delegates_to_an_open_part(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")
    sink.write({"id": 1})

    sink.flush()
    _commit(sink)

    assert _json_parts(tmp_path) == [[{"id": 1}]]


def test_write_requires_open(tmp_path: Path) -> None:
    sink = StreamingFileSink(str(tmp_path), "json")

    with pytest.raises(RuntimeError, match="file sink must be opened before use"):
        sink.write({"id": 1})


def test_abort_current_transaction_removes_open_and_rolled_parts(tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json", max_rows_per_file=2)
    sink.write({"id": 1})
    sink.write({"id": 2})
    sink.write({"id": 3})
    assert list(tmp_path.rglob("*.pending"))
    assert list(tmp_path.rglob("*.inprogress"))

    sink.abort_current_transaction()

    assert sink.prepare_commit(1) is None
    assert not list(tmp_path.rglob("*.pending"))
    assert not list(tmp_path.rglob("*.inprogress"))


def test_abort_current_transaction_continues_cleanup_and_raises_first_error(monkeypatch, tmp_path: Path) -> None:
    sink = _opened_sink(tmp_path, "json")
    sink.write({"id": 1})
    writer = sink._writer
    filesystem = sink._filesystem
    writer_path = sink._writer_path
    assert writer is not None
    assert filesystem is not None
    assert writer_path is not None
    pending = FilePart("staging/already-rolled.pending", "part.json")
    sink._pending_parts.append(pending)
    original_close = writer.close
    original_delete = filesystem.delete_file
    cleanup_calls = []

    def fail_close() -> None:
        cleanup_calls.append(call.close())
        raise RuntimeError("close failed first")

    def fail_delete(path: str) -> None:
        cleanup_calls.append(call.delete(path))
        raise OSError(f"delete failed for {path}")

    monkeypatch.setattr(writer, "close", fail_close)
    monkeypatch.setattr(filesystem, "delete_file", fail_delete)
    try:
        with pytest.raises(RuntimeError, match="close failed first"):
            sink.abort_current_transaction()
    finally:
        original_close()
        original_delete(writer_path)

    assert cleanup_calls == [call.close(), call.delete(writer_path), call.delete(pending.pending_path)]
    assert sink._writer is None
    assert sink._writer_path is None
    assert sink._pending_parts == []


def test_multi_part_commit_resumes_after_a_part_was_already_published(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "first.pending").write_text("stale first", encoding="utf-8")
    (staging / "second.pending").write_text("second", encoding="utf-8")
    (tmp_path / "first.json").write_text("published first", encoding="utf-8")
    committable = FileSinkCommittable(
        root_uri=str(tmp_path),
        storage_options=None,
        parts=(
            FilePart("staging/first.pending", "first.json"),
            FilePart("staging/second.pending", "second.json"),
        ),
        _transaction_id="multi-part-commit",
    )

    committable.commit()
    committable.commit()

    assert committable.transaction_id == "multi-part-commit"
    assert (tmp_path / "first.json").read_text(encoding="utf-8") == "published first"
    assert (tmp_path / "second.json").read_text(encoding="utf-8") == "second"
    assert not list(staging.glob("*.pending"))


def test_multi_part_abort_preserves_a_part_that_was_already_published(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "second.pending").write_text("second", encoding="utf-8")
    (tmp_path / "first.json").write_text("published first", encoding="utf-8")
    committable = FileSinkCommittable(
        root_uri=str(tmp_path),
        storage_options=None,
        parts=(
            FilePart("staging/first.pending", "first.json"),
            FilePart("staging/second.pending", "second.json"),
        ),
        _transaction_id="multi-part-abort",
    )

    committable.abort()
    committable.abort()

    assert (tmp_path / "first.json").read_text(encoding="utf-8") == "published first"
    assert not (tmp_path / "second.json").exists()
    assert not list(staging.glob("*.pending"))
