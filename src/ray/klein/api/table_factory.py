# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from ray.klein.api.catalog_table import CatalogTable
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext
    from ray.klein.api.row_kind import RowKind


class TableFactory:
    """Validate catalog metadata and bind it directly to a source or sink."""

    identifier: str

    def validate(self, table: CatalogTable) -> None:
        del table

    def create_source(self, context: KleinContext, table: CatalogTable) -> DataStream:
        del context, table
        raise SQLQueryError(f"Table factory {self.identifier!r} does not provide a source")

    def create_sink(self, stream: DataStream, table: CatalogTable) -> Any:
        del stream, table
        raise SQLQueryError(f"Table factory {self.identifier!r} does not provide a sink")

    def supported_sink_row_kinds(self, table: CatalogTable) -> frozenset[RowKind]:
        """Return changelog changes accepted by this connector sink."""

        from ray.klein.api.row_kind import RowKind

        del table
        return frozenset({RowKind.INSERT})

    def validate_sink_changelog(self, stream: DataStream, table: CatalogTable) -> None:
        supported = self.supported_sink_row_kinds(table)
        unsupported = stream.changelog_mode - supported
        if unsupported:
            changes = ", ".join(sorted(row_kind.value for row_kind in unsupported))
            raise SQLQueryError(
                f"Connector {self.identifier!r} cannot consume SQL changelog kinds {changes}; "
                "use an upsert/retract-capable sink"
            )
