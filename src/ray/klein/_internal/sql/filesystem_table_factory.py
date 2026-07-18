# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from ray.klein._internal.sql.connector_options import prefixed_options, reject_unknown_options, require_option
from ray.klein._internal.sql.filesystem_sink_options import FilesystemSinkOptions
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.api.table_factory import TableFactory

if TYPE_CHECKING:
    from ray.klein.api.catalog_table import CatalogTable
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext


class FilesystemTableFactory(TableFactory):
    identifier = "filesystem"
    _SOURCE_FORMATS: ClassVar[frozenset[str]] = frozenset({"csv", "json", "parquet", "text"})
    _SINK_FORMATS: ClassVar[frozenset[str]] = frozenset({"csv", "json", "parquet", "text"})

    def validate(self, table: CatalogTable) -> None:
        require_option(table.options, "path", self.identifier)
        data_format = require_option(table.options, "format", self.identifier)
        if data_format not in self._SOURCE_FORMATS:
            raise SQLQueryError(
                f"Unsupported filesystem format {data_format!r}; expected one of {sorted(self._SOURCE_FORMATS)}"
            )
        reject_unknown_options(
            table.options,
            connector=self.identifier,
            supported={"connector", "path", "format", *FilesystemSinkOptions.OPTION_NAMES},
            prefixes=("source.",),
        )
        FilesystemSinkOptions.from_mapping(table.options)

    def create_source(self, context: KleinContext, table: CatalogTable) -> DataStream:
        method = f"read_{table.options['format']}"
        path = table.options["path"]
        options = prefixed_options(table.options, "source.")
        return context.data.source(method, path, **options)

    def create_sink(self, stream: DataStream, table: CatalogTable) -> Any:
        data_format = table.options["format"]
        if data_format not in self._SINK_FORMATS:
            raise SQLQueryError(f"Filesystem format {data_format!r} cannot be used as a sink")
        columns = tuple(column.name for column in table.columns)
        if data_format == "text" and len(columns) != 1:
            raise SQLQueryError("Filesystem text sinks require exactly one table column")
        options = FilesystemSinkOptions.from_mapping(table.options)
        return stream.write_files(
            table.options["path"],
            data_format,
            columns=columns,
            storage_options=options.storage_options,
            filename_prefix=options.filename_prefix,
            max_rows_per_file=options.max_rows_per_file,
            max_bytes_per_file=options.max_bytes_per_file,
            rollover_interval=options.rollover_interval,
            inactivity_interval=options.inactivity_interval,
            concurrency=options.parallelism,
            ray_data_options=options.ray_data_options,
        )
