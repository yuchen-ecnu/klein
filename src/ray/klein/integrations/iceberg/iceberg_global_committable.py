# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.integrations.iceberg.iceberg_sink_committable import (
    IcebergSinkCommittable,
    _commit_arrow_payloads,
)


@dataclass(frozen=True, slots=True)
class IcebergGlobalCommittable(SinkCommittable):
    """Writer batches from one logical sink published in one Iceberg snapshot."""

    table_identifier: str
    catalog_kwargs: dict[str, Any] | None
    snapshot_properties: dict[str, str] | None
    arrow_ipcs: tuple[bytes, ...]
    writer_transaction_ids: tuple[str, ...]
    _transaction_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "arrow_ipcs", tuple(self.arrow_ipcs))
        object.__setattr__(self, "writer_transaction_ids", tuple(self.writer_transaction_ids))
        if not isinstance(self._transaction_id, str) or not self._transaction_id.strip():
            raise ValueError("global Iceberg transaction_id must be a non-empty string")
        if not self.arrow_ipcs:
            raise ValueError("global Iceberg committable requires at least one writer batch")
        if len(self.arrow_ipcs) != len(self.writer_transaction_ids):
            raise ValueError("global Iceberg writer payloads and transaction IDs must have equal lengths")
        if len(self.writer_transaction_ids) != len(set(self.writer_transaction_ids)):
            raise ValueError("global Iceberg committable contains duplicate writer transactions")

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        _commit_arrow_payloads(
            table_identifier=self.table_identifier,
            catalog_kwargs=self.catalog_kwargs,
            snapshot_properties=self.snapshot_properties,
            arrow_ipcs=self.arrow_ipcs,
            transaction_id=self.transaction_id,
        )

    def abort(self) -> None:
        # Prepared rows live only in durable Klein checkpoint metadata. If the
        # checkpoint is aborted, dropping this value discards the transaction.
        return None


def combine_iceberg_committables(
    committables: tuple[IcebergSinkCommittable, ...],
    *,
    transaction_id: str,
) -> IcebergGlobalCommittable:
    """Combine one epoch's writer batches for a single logical Iceberg sink."""

    items = tuple(committables)
    if not items:
        raise ValueError("cannot combine an empty set of Iceberg writer committables")
    if not all(isinstance(item, IcebergSinkCommittable) for item in items):
        raise TypeError("all Iceberg writer committables must be IcebergSinkCommittable values")

    first = items[0]
    for item in items[1:]:
        if item.table_identifier != first.table_identifier:
            raise ValueError("cannot combine Iceberg writer committables for different tables")
        if item.catalog_kwargs != first.catalog_kwargs:
            raise ValueError("cannot combine Iceberg writer committables with different catalogs")
        if item.snapshot_properties != first.snapshot_properties:
            raise ValueError("cannot combine Iceberg writer committables with different snapshot properties")

    ordered = tuple(sorted(items, key=lambda item: item.transaction_id))
    return IcebergGlobalCommittable(
        table_identifier=first.table_identifier,
        catalog_kwargs=first.catalog_kwargs,
        snapshot_properties=first.snapshot_properties,
        arrow_ipcs=tuple(item.arrow_ipc for item in ordered),
        writer_transaction_ids=tuple(item.transaction_id for item in ordered),
        _transaction_id=transaction_id,
    )
