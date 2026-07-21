# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.api.two_phase_commit_sink_function import TwoPhaseCommitSinkFunction
from ray.klein.integrations.iceberg.iceberg_sink_committable import (
    TRANSACTION_ID_SNAPSHOT_PROPERTY,
    IcebergSinkCommittable,
)


class StreamingIcebergSink(TwoPhaseCommitSinkFunction):
    """Checkpoint-transactional append sink for an existing Iceberg table."""

    def __init__(
        self,
        table_identifier: str,
        *,
        catalog_kwargs: dict[str, Any] | None = None,
        snapshot_properties: dict[str, str] | None = None,
        mode: Any = "append",
        overwrite_filter: Any = None,
        upsert_kwargs: dict[str, Any] | None = None,
        overwrite_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(table_identifier, str) or not table_identifier.strip():
            raise ValueError("Iceberg table_identifier must be a non-empty string")
        normalized_mode = getattr(mode, "value", mode)
        if normalized_mode != "append":
            raise ValueError("streaming Iceberg output currently supports only SaveMode.APPEND")
        if overwrite_filter is not None or upsert_kwargs or overwrite_kwargs:
            raise ValueError("overwrite and upsert options are not supported by streaming Iceberg output")
        properties = dict(snapshot_properties or {})
        if TRANSACTION_ID_SNAPSHOT_PROPERTY in properties:
            raise ValueError(
                f"snapshot property {TRANSACTION_ID_SNAPSHOT_PROPERTY!r} is reserved for checkpoint deduplication"
            )

        self._table_identifier = table_identifier
        self._catalog_kwargs = dict(catalog_kwargs) if catalog_kwargs is not None else None
        self._snapshot_properties = properties or None
        self._rows: list[dict[str, Any]] = []
        self._columns: tuple[str, ...] | None = None
        self._task_index = 0
        self._job_id = "default"
        self._attempt_id = uuid4().hex

    def open(self, runtime_context: RuntimeContext) -> None:
        try:
            import pyiceberg  # noqa: F401
        except ModuleNotFoundError as error:
            raise ModuleNotFoundError("Iceberg output requires `ray-klein[iceberg]`.") from error
        self._task_index = runtime_context.task_index
        self._job_id = runtime_context.job_id

    def write(self, value: dict[str, Any]) -> None:
        if not isinstance(value, Mapping):
            raise TypeError("Iceberg sink records must be mappings")
        if not value:
            raise ValueError("Iceberg sink records must contain at least one column")
        if not all(isinstance(column, str) for column in value):
            raise TypeError("Iceberg sink column names must be strings")
        if self._columns is None:
            self._columns = tuple(value)
        expected = set(self._columns)
        missing = expected - value.keys()
        extra = value.keys() - expected
        if missing or extra:
            raise ValueError(f"Iceberg sink row columns changed; missing={sorted(missing)}, extra={sorted(extra)}")
        self._rows.append({column: value[column] for column in self._columns})

    def flush(self) -> None:
        # Visibility is tied to prepare_commit(), not to an eager external flush.
        return None

    def prepare_commit(self, checkpoint_id: int) -> SinkCommittable | None:
        if not self._rows:
            return None

        arrow_ipc = _serialize_rows(self._rows)
        self._rows.clear()
        transaction_id = f"{self._job_id}-{self._task_index}-{self._attempt_id}-iceberg-chk-{checkpoint_id}"
        return IcebergSinkCommittable(
            table_identifier=self._table_identifier,
            catalog_kwargs=self._catalog_kwargs,
            snapshot_properties=self._snapshot_properties,
            arrow_ipc=arrow_ipc,
            _transaction_id=transaction_id,
        )

    def abort_current_transaction(self) -> None:
        self._rows.clear()

    def close(self) -> None:
        self.abort_current_transaction()


def _serialize_rows(rows: list[dict[str, Any]]) -> bytes:
    import pyarrow as pa

    table = pa.Table.from_pylist(rows)
    output = pa.BufferOutputStream()
    options = pa.ipc.IpcWriteOptions(compression="zstd")
    with pa.ipc.new_stream(output, table.schema, options=options) as writer:
        writer.write_table(table)
    return output.getvalue().to_pybytes()
