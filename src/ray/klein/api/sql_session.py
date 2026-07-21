# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ray.klein._internal.sql.catalog_parser import parse_catalog_table
from ray.klein._internal.sql.execution import sql_source, sql_transform
from ray.klein._internal.sql.scalar_function_registry import ScalarFunction, ScalarFunctionRegistry
from ray.klein._internal.sql.table_factory_registry import TableFactoryRegistry
from ray.klein._internal.sql.validation import (
    SQL_DIALECT,
    parse_statement,
    referenced_table_names,
    validate_read_query,
    validate_table_name,
)
from ray.klein.api.node_type import NodeType
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode

if TYPE_CHECKING:
    from ray.klein.api.catalog_table import CatalogTable
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext
    from ray.klein.api.table_factory import TableFactory


class SQLSession:
    """A scoped catalog and planner for lazy, Ray-native SQL operations."""

    def __init__(self, context: KleinContext) -> None:
        self._context = context
        self._views: OrderedDict[str, DataStream] = OrderedDict()
        self._tables: OrderedDict[str, CatalogTable] = OrderedDict()
        self._table_factories = TableFactoryRegistry.with_defaults()
        self._scalar_functions = ScalarFunctionRegistry()

    def create_temp_view(self, name: str, dataframe: DataStream) -> DataStream:
        """Create or replace a named, non-materialized DataStream view."""

        self._validate_binding(name, dataframe)
        self._views[name] = dataframe
        return dataframe

    def drop_temp_view(self, name: str) -> None:
        try:
            del self._views[name]
        except KeyError as exc:
            raise SQLQueryError(f"Unknown temporary view {name!r}") from exc

    @property
    def temp_views(self) -> tuple[str, ...]:
        return tuple(self._views)

    def register_table_factory(self, factory: TableFactory, *, replace: bool = False) -> None:
        """Register a session-local table factory."""

        self._table_factories.register(factory, replace=replace)

    def register_scalar_function(
        self,
        name: str,
        function: ScalarFunction,
        *,
        replace: bool = False,
    ) -> None:
        """Register a session-local Python scalar function for SQL queries."""

        self._scalar_functions.register(name, function, replace=replace)

    def drop_scalar_function(self, name: str) -> None:
        """Remove a session-local SQL scalar function."""

        self._scalar_functions.drop(name)

    @property
    def scalar_functions(self) -> tuple[str, ...]:
        return self._scalar_functions.identifiers

    def _scalar_function_bindings(self) -> Mapping[str, ScalarFunction]:
        """Return an immutable snapshot for a one-query child session."""

        return self._scalar_functions.snapshot()

    @property
    def table_factories(self) -> tuple[str, ...]:
        return self._table_factories.identifiers

    def table(self, name: str) -> CatalogTable:
        try:
            return self._tables[name]
        except KeyError as exc:
            raise SQLQueryError(f"Unknown catalog table {name!r}") from exc

    @property
    def tables(self) -> tuple[str, ...]:
        return tuple(self._tables)

    def execute_sql(self, statement: str, *, num_cpus: float = 1.0) -> Any:
        """Execute catalog DDL/DML or build a lazy query.

        ``CREATE/DROP TABLE`` update only session metadata. ``INSERT INTO``
        builds the query graph and returns its connector sink; normal queries
        return a :class:`DataStream`.
        """

        from sqlglot import exp

        parsed = parse_statement(statement)
        if isinstance(parsed, exp.Query):
            return self.sql(statement, num_cpus=num_cpus)
        if isinstance(parsed, exp.Create):
            return self._create_table(parsed)
        if isinstance(parsed, exp.Drop):
            self._drop_table(parsed)
            return None
        if isinstance(parsed, exp.Insert):
            return self._insert(parsed, num_cpus=num_cpus)
        raise SQLQueryError("Klein SQL supports SELECT, CREATE TABLE, DROP TABLE, and INSERT INTO")

    def sql(
        self,
        query: str,
        *,
        tables: Mapping[str, DataStream] | None = None,
        functions: Mapping[str, ScalarFunction] | None = None,
        num_cpus: float = 1.0,
    ) -> DataStream:
        """Build a lazy, bounded DataStream from one Ray-native SQL query."""

        statement = validate_read_query(query)
        if not isinstance(num_cpus, (int, float)) or num_cpus <= 0:
            raise SQLQueryError("num_cpus must be positive")
        scalar_functions = self._scalar_functions.bind(statement, functions)

        bindings: dict[str, DataStream] = dict(self._views)
        for name, dataframe in (tables or {}).items():
            self._validate_binding(name, dataframe)
            bindings[name] = dataframe

        referenced = referenced_table_names(statement)
        unknown = referenced - bindings.keys() - self._tables.keys()
        if unknown:
            unknown_names = ", ".join(sorted(unknown))
            raise SQLQueryError(f"SQL query references unbound table(s): {unknown_names}")
        for name in referenced - bindings.keys():
            table = self._tables[name]
            factory = self._table_factories.get(table.connector)
            dataframe = factory.create_source(self._context, table)
            self._validate_binding(name, dataframe)
            bindings[name] = dataframe
        bindings = {name: dataframe for name, dataframe in bindings.items() if name in referenced}

        mode = self._context.config.get(ExecutionOptions.MODE)
        has_unbounded_input = any(not self._is_bounded(dataframe) for dataframe in bindings.values())
        if mode == RuntimeExecutionMode.BATCH and has_unbounded_input:
            raise SQLQueryError("Batch SQL cannot consume an unbounded table; use execution.runtime.mode=streaming")
        use_streaming = mode == RuntimeExecutionMode.STREAMING or has_unbounded_input
        if use_streaming:
            from ray.klein._internal.sql.streaming import build_streaming_query

            return build_streaming_query(
                self._context,
                query,
                bindings,
                functions=scalar_functions,
                num_cpus=num_cpus,
            )

        if not bindings:
            options: dict[str, Any] = {"num_cpus": num_cpus}
            if scalar_functions:
                options["functions"] = scalar_functions
            return self._context.data.source(
                sql_source,
                query,
                **options,
            )

        table_names = tuple(bindings)
        dataframes = tuple(bindings.values())
        primary, others = dataframes[0], dataframes[1:]
        options = {"num_cpus": num_cpus}
        if scalar_functions:
            options["functions"] = scalar_functions
        return primary.data.transform(
            sql_transform,
            query,
            table_names,
            *others,
            **options,
        )

    def _create_table(self, statement) -> CatalogTable:
        table = parse_catalog_table(statement)
        validate_table_name(table.name)
        factory = self._table_factories.get(table.connector)
        factory.validate(table)
        exists = table.name in self._tables
        if exists and statement.args.get("exists"):
            return self._tables[table.name]
        if exists and not statement.args.get("replace"):
            raise SQLQueryError(f"Catalog table {table.name!r} already exists")
        self._tables[table.name] = table
        return table

    def _drop_table(self, statement) -> None:
        from sqlglot import exp

        if statement.args.get("kind") != "TABLE" or not isinstance(statement.this, exp.Table):
            raise SQLQueryError("Klein DROP currently supports TABLE objects only")
        name = statement.this.name
        if statement.this.catalog or statement.this.db:
            raise SQLQueryError("Catalog-qualified SQL table names are not supported yet")
        if name not in self._tables:
            if statement.args.get("exists"):
                return
            raise SQLQueryError(f"Unknown catalog table {name!r}")
        del self._tables[name]

    def _insert(self, statement: Any, *, num_cpus: float) -> Any:
        from sqlglot import exp

        if not isinstance(statement.this, exp.Table):
            raise SQLQueryError("INSERT column lists and partitions are not supported yet")
        target_name = statement.this.name
        try:
            target = self._tables[target_name]
        except KeyError as exc:
            raise SQLQueryError(f"INSERT target {target_name!r} is not a catalog table") from exc
        if not isinstance(statement.expression, exp.Query):
            raise SQLQueryError("INSERT INTO requires a SELECT query")
        query = statement.expression.sql(dialect=SQL_DIALECT)
        stream = self.sql(query, num_cpus=num_cpus)
        factory = self._table_factories.get(target.connector)
        factory.validate_sink_changelog(stream, target)
        return factory.create_sink(stream, target)

    def _validate_binding(self, name: str, dataframe: DataStream) -> None:
        from ray.klein.api.data_stream import DataStream

        validate_table_name(name)
        if not isinstance(dataframe, DataStream):
            raise SQLQueryError(f"SQL table {name!r} must be a DataStream, got {type(dataframe).__name__}")
        if dataframe.context is not self._context:
            raise SQLQueryError("All SQL tables must belong to the SQLSession's KleinContext")

    def _is_bounded(self, dataframe: DataStream) -> bool:
        if dataframe.node_type == NodeType.SOURCE:
            return bool(dataframe.stream_operator.bounded)
        return bool(dataframe.input_streams) and all(self._is_bounded(parent) for parent in dataframe.input_streams)
