# SPDX-License-Identifier: Apache-2.0
import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from ray.klein.integrations.filesystem.file_sink_committable import FileSinkCommittable
from ray.klein.integrations.filesystem.streaming_file_sink import StreamingFileSink


def _opened_sink(tmp_path: Path, data_format: str, **options) -> StreamingFileSink:
    sink = StreamingFileSink(str(tmp_path), data_format, **options)
    sink.open(SimpleNamespace(task_index=2, job_id="job/with spaces"))
    return sink


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
