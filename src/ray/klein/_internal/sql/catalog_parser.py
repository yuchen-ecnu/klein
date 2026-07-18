# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Sequence

from sqlglot import exp

from ray.klein.api.catalog_table import CatalogTable
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.api.table_column import TableColumn


def parse_catalog_table(statement: exp.Create) -> CatalogTable:
    """Convert a Flink-style CREATE TABLE AST into immutable catalog metadata."""

    if statement.args.get("kind") != "TABLE":
        raise SQLQueryError("Klein CREATE supports TABLE objects only")
    if statement.expression is not None:
        raise SQLQueryError("CREATE TABLE AS SELECT is not supported; use CREATE TABLE followed by INSERT INTO")
    if not isinstance(statement.this, exp.Schema) or not isinstance(statement.this.this, exp.Table):
        raise SQLQueryError("CREATE TABLE requires an explicit physical column schema")

    table_expression = statement.this.this
    if table_expression.catalog or table_expression.db:
        raise SQLQueryError("Catalog-qualified SQL table names are not supported yet")

    options, temporary = _parse_properties(statement.args.get("properties"))
    if "connector" not in options:
        raise SQLQueryError("CREATE TABLE WITH requires a 'connector' option")
    return CatalogTable(
        name=table_expression.name,
        columns=_parse_columns(statement.this.expressions),
        options=options,
        temporary=temporary,
    )


def _parse_columns(definitions: Sequence[exp.Expression]) -> tuple[TableColumn, ...]:
    return tuple(_parse_column(definition) for definition in definitions)


def _parse_column(definition: exp.Expression) -> TableColumn:
    if not isinstance(definition, exp.ColumnDef):
        raise SQLQueryError(f"Unsupported CREATE TABLE schema element: {definition.sql()}")
    data_type = definition.args.get("kind")
    if data_type is None:
        raise SQLQueryError(f"Column {definition.name!r} requires a data type")
    nullable = not any(
        isinstance(constraint.args.get("kind"), exp.NotNullColumnConstraint)
        for constraint in definition.args.get("constraints") or ()
    )
    return TableColumn(definition.name, data_type.sql(), nullable=nullable)


def _parse_properties(properties: exp.Properties | None) -> tuple[dict[str, str], bool]:
    options: dict[str, str] = {}
    temporary = False
    for property_expression in properties.expressions if properties is not None else ():
        if isinstance(property_expression, exp.TemporaryProperty):
            temporary = True
            continue
        name, value = _parse_option(property_expression)
        if name in options:
            raise SQLQueryError(f"Duplicate CREATE TABLE option {name!r}")
        options[name] = value
    return options, temporary


def _parse_option(property_expression: exp.Expression) -> tuple[str, str]:
    if not isinstance(property_expression, exp.Property):
        raise SQLQueryError(f"Unsupported CREATE TABLE property: {property_expression.sql()}")
    key = property_expression.this
    value = property_expression.args.get("value")
    if not isinstance(key, exp.Literal) or not key.is_string:
        raise SQLQueryError("CREATE TABLE WITH option names must be string literals")
    if not isinstance(value, exp.Literal) or not value.is_string:
        raise SQLQueryError(f"CREATE TABLE option {key.this!r} must have a string literal value")
    return key.this, value.this
