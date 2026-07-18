# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ray.klein.api.row_kind import RowKind


class ChangelogRow(dict[str, Any]):
    """A row plus Flink-compatible changelog metadata.

    ``row_kind`` is metadata rather than a user column, so normal mapping and
    connector serialization continue to see only the SQL result columns.
    """

    def __init__(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        row_kind: RowKind = RowKind.INSERT,
        **columns: Any,
    ) -> None:
        super().__init__(values or {}, **columns)
        if not isinstance(row_kind, RowKind):
            raise TypeError("row_kind must be a RowKind")
        self.row_kind = row_kind

    def with_kind(self, row_kind: RowKind) -> ChangelogRow:
        return ChangelogRow(self, row_kind=row_kind)

    @classmethod
    def insert(cls, values: Mapping[str, Any]) -> ChangelogRow:
        return cls(values, row_kind=RowKind.INSERT)

    @classmethod
    def update_before(cls, values: Mapping[str, Any]) -> ChangelogRow:
        return cls(values, row_kind=RowKind.UPDATE_BEFORE)

    @classmethod
    def update_after(cls, values: Mapping[str, Any]) -> ChangelogRow:
        return cls(values, row_kind=RowKind.UPDATE_AFTER)

    @classmethod
    def delete(cls, values: Mapping[str, Any]) -> ChangelogRow:
        return cls(values, row_kind=RowKind.DELETE)


def row_kind_of(row: Mapping[str, Any]) -> RowKind:
    """Treat ordinary source mappings as append-only INSERT rows."""

    return row.row_kind if isinstance(row, ChangelogRow) else RowKind.INSERT
