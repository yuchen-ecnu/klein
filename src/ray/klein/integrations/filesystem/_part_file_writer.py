# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import csv
import io
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from ray.klein.state.checkpoint_file_system import CheckpointFileSystem


class PartFileWriter:
    """Encode one immutable part file into a PyArrow filesystem stream."""

    def __init__(
        self,
        filesystem: CheckpointFileSystem,
        path: str,
        data_format: str,
        *,
        columns: Sequence[str] | None = None,
        parquet_schema: Any = None,
    ) -> None:
        self._stream = filesystem.open_output_stream(path)
        self._format = data_format
        self._columns = tuple(columns) if columns else None
        self._parquet_schema = parquet_schema
        self._parquet_writer = None
        self._closed = False
        self._csv_header_written = False

    @property
    def columns(self) -> tuple[str, ...] | None:
        return self._columns

    @property
    def parquet_schema(self) -> Any:
        return self._parquet_schema

    @property
    def size_bytes(self) -> int:
        try:
            return int(self._stream.tell())
        except (AttributeError, OSError):
            return 0

    def write(self, value: Mapping[str, Any]) -> None:
        row = dict(value)
        if self._columns is None:
            self._columns = tuple(row)
        else:
            expected = set(self._columns)
            missing = expected - row.keys()
            extra = row.keys() - expected
            if missing or extra:
                raise ValueError(
                    f"file sink row does not match columns; missing={sorted(missing)}, extra={sorted(extra)}"
                )
            row = {column: row[column] for column in self._columns}
        if self._format == "json":
            self._stream.write(_json_bytes(row) + b"\n")
        elif self._format == "csv":
            self._write_csv(row)
        elif self._format == "text":
            self._write_text(row)
        elif self._format == "parquet":
            self._write_parquet(row)
        else:  # pragma: no cover - constructor validation owns this boundary.
            raise ValueError(f"unsupported file format {self._format!r}")

    def flush(self) -> None:
        if self._closed:
            return
        if self._parquet_writer is None and hasattr(self._stream, "flush"):
            self._stream.flush()

    def close(self) -> None:
        if self._closed:
            return
        try:
            if self._parquet_writer is not None:
                self._parquet_writer.close()
            else:
                self.flush()
        finally:
            self._stream.close()
            self._closed = True

    def _write_csv(self, row: dict[str, Any]) -> None:
        if self._columns is None:
            self._columns = tuple(row)
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=self._columns, extrasaction="raise", lineterminator="\n")
        if not self._csv_header_written:
            writer.writeheader()
            self._csv_header_written = True
        writer.writerow(row)
        self._stream.write(buffer.getvalue().encode("utf-8"))

    def _write_text(self, row: dict[str, Any]) -> None:
        if self._columns is None:
            self._columns = tuple(row)
        if len(self._columns) != 1:
            raise ValueError("text file sink requires exactly one column")
        value = row.get(self._columns[0])
        self._stream.write(f"{'' if value is None else value}\n".encode())

    def _write_parquet(self, row: dict[str, Any]) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        if self._parquet_schema is None:
            table = pa.Table.from_pylist([row])
            self._parquet_schema = table.schema
            self._columns = tuple(table.column_names)
            self._parquet_writer = pq.ParquetWriter(self._stream, self._parquet_schema)
        else:
            table = pa.Table.from_pylist([row], schema=self._parquet_schema)
            if self._parquet_writer is None:
                self._parquet_writer = pq.ParquetWriter(self._stream, self._parquet_schema)
        self._parquet_writer.write_table(table)


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_default).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
