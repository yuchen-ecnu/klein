# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ray.klein._internal.sql.connector_options import parse_option_value, reject_unknown_options
from ray.klein.api.table_factory import TableFactory

if TYPE_CHECKING:
    from ray.klein.api.catalog_table import CatalogTable
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.row_kind import RowKind


class PrintTableFactory(TableFactory):
    identifier = "print"

    def validate(self, table: CatalogTable) -> None:
        reject_unknown_options(
            table.options,
            connector=self.identifier,
            supported={"connector", "limit"},
        )

    def create_sink(self, stream: DataStream, table: CatalogTable) -> Any:
        limit = int(parse_option_value(table.options.get("limit", "20")))
        return stream.show(limit=limit, name=f"Print[{table.name}]")

    def supported_sink_row_kinds(self, table: CatalogTable) -> frozenset[RowKind]:
        from ray.klein.api.row_kind import RowKind

        del table
        return frozenset(RowKind)
