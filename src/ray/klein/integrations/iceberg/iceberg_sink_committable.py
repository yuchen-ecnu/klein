# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ray.klein.api.sink_committable import SinkCommittable

TRANSACTION_ID_SNAPSHOT_PROPERTY = "ray-klein.transaction-id"


@dataclass(frozen=True, slots=True)
class IcebergSinkCommittable(SinkCommittable):
    """An Arrow batch published as one idempotent Iceberg snapshot."""

    table_identifier: str
    catalog_kwargs: dict[str, Any] | None
    snapshot_properties: dict[str, str] | None
    arrow_ipc: bytes
    _transaction_id: str

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        _commit_arrow_payloads(
            table_identifier=self.table_identifier,
            catalog_kwargs=self.catalog_kwargs,
            snapshot_properties=self.snapshot_properties,
            arrow_ipcs=(self.arrow_ipc,),
            transaction_id=self.transaction_id,
        )

    def abort(self) -> None:
        # Prepared rows live only in durable Klein checkpoint metadata. If the
        # checkpoint is aborted, dropping this value discards the transaction.
        return None


def _commit_arrow_payloads(
    *,
    table_identifier: str,
    catalog_kwargs: dict[str, Any] | None,
    snapshot_properties: dict[str, str] | None,
    arrow_ipcs: tuple[bytes, ...],
    transaction_id: str,
) -> None:
    catalog = _load_catalog(catalog_kwargs)
    table = catalog.load_table(table_identifier)
    if _snapshot_exists(table, transaction_id):
        return

    arrow_table = _concatenate_arrow_payloads(arrow_ipcs)
    if arrow_table.num_rows == 0:
        return

    if _evolve_top_level_schema(table, arrow_table.schema):
        table = catalog.load_table(table_identifier)
        if _snapshot_exists(table, transaction_id):
            return

    properties = dict(snapshot_properties or {})
    properties[TRANSACTION_ID_SNAPSHOT_PROPERTY] = transaction_id
    table.append(_align_arrow_schema(table, arrow_table), snapshot_properties=properties)


def _concatenate_arrow_payloads(payloads: tuple[bytes, ...]) -> Any:
    import pyarrow as pa

    tables = tuple(_deserialize_arrow_table(payload) for payload in payloads)
    if len(tables) == 1:
        return tables[0]
    return pa.concat_tables(tables, promote_options="default")


def _load_catalog(catalog_kwargs: dict[str, Any] | None) -> Any:
    try:
        from pyiceberg.catalog import load_catalog
    except ModuleNotFoundError as error:
        raise ModuleNotFoundError("Iceberg output requires `ray-klein[iceberg]`.") from error

    options = dict(catalog_kwargs or {})
    name = options.pop("name", "default")
    return load_catalog(name, **options)


def _deserialize_arrow_table(payload: bytes) -> Any:
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(payload)) as reader:
        return reader.read_all()


def _evolve_top_level_schema(table: Any, incoming_schema: Any) -> bool:
    """Add new top-level columns while preserving existing requirements."""

    import pyarrow as pa

    current_schema = table.schema().as_arrow()
    current_names = frozenset(current_schema.names)
    new_fields = [field for field in incoming_schema if field.name not in current_names]
    if not new_fields:
        return False

    # Feeding only the incoming Arrow schema to union_by_name would make
    # required identifier fields optional. Start from Iceberg's exact current
    # schema, then add the genuinely new nullable fields.
    merged_schema = pa.schema([*current_schema, *new_fields], metadata=current_schema.metadata)
    with table.update_schema() as update:
        update.union_by_name(merged_schema)
    return True


def _align_arrow_schema(table: Any, incoming_table: Any) -> Any:
    """Use Iceberg types and nullability for fields already in the table."""

    import pyarrow as pa

    current_fields = {field.name: field for field in table.schema().as_arrow()}
    aligned_fields = []
    for field in incoming_table.schema:
        current = current_fields.get(field.name)
        if current is None:
            aligned_fields.append(field)
            continue
        column = incoming_table[field.name]
        if not current.nullable and column.null_count:
            raise ValueError(f"Iceberg required field {field.name!r} contains null values")
        aligned_fields.append(
            pa.field(
                field.name,
                current.type,
                nullable=current.nullable,
                metadata=current.metadata,
            )
        )
    aligned_schema = pa.schema(aligned_fields, metadata=incoming_table.schema.metadata)
    return incoming_table.cast(aligned_schema, safe=True)


def _snapshot_exists(table: Any, transaction_id: str) -> bool:
    for snapshot in reversed(table.metadata.snapshots):
        summary = snapshot.summary
        if summary is None:
            continue
        if summary.get(TRANSACTION_ID_SNAPSHOT_PROPERTY) == transaction_id:
            return True
    return False
