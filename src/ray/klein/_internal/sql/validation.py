# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from sqlglot import exp

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SQL_DIALECT = "spark"
BUILTIN_ANONYMOUS_FUNCTIONS = frozenset({"download", "monotonically_increasing_id"})


def validate_table_name(name: str) -> None:
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        raise SQLQueryError(
            f"Invalid SQL table name {name!r}; use an unquoted identifier containing letters, numbers, and underscores"
        )


def validate_scalar_function_name(name: str) -> None:
    """Validate one unquoted, non-built-in SQL scalar-function name."""

    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        raise SQLQueryError(
            f"Invalid SQL scalar function name {name!r}; use an unquoted identifier "
            "containing letters, numbers, and underscores"
        )
    if name.casefold() in BUILTIN_ANONYMOUS_FUNCTIONS:
        raise SQLQueryError(f"SQL scalar function name {name!r} is reserved by Klein")

    from sqlglot import exp

    try:
        parsed = parse_statement(f"SELECT {name}(_klein_argument)")
    except SQLQueryError as error:
        raise SQLQueryError(f"SQL scalar function name {name!r} is reserved by the SQL dialect") from error
    value = parsed.expressions[0] if isinstance(parsed, exp.Select) else None
    if not isinstance(value, exp.Anonymous):
        raise SQLQueryError(f"SQL scalar function name {name!r} is reserved by the SQL dialect")


def parse_statement(sql: str) -> exp.Expression:
    """Parse exactly one statement with SQLGlot's Spark-compatible grammar."""

    if not isinstance(sql, str) or not sql.strip():
        raise SQLQueryError("SQL statement must be a non-empty string")

    from sqlglot import errors, parse

    try:
        statements = [statement for statement in parse(sql, read=SQL_DIALECT) if statement is not None]
    except errors.ParseError as exc:
        raise SQLQueryError(f"Invalid SQL statement: {exc}") from exc
    if len(statements) != 1:
        raise SQLQueryError("Klein SQL accepts exactly one statement")
    return statements[0]


def validate_read_query(query: str) -> exp.Query:
    from sqlglot import exp

    statement = parse_statement(query)
    if not isinstance(statement, exp.Query):
        raise SQLQueryError("Klein SQL queries accept only SELECT or WITH ... SELECT statements")
    return statement


def referenced_table_names(query: str | exp.Expression) -> set[str]:
    """Return physical table names, excluding names introduced by CTEs."""

    from sqlglot import exp

    statement = parse_statement(query) if isinstance(query, str) else query
    cte_names = {cte.alias for cte in statement.find_all(exp.CTE)}
    names: set[str] = set()
    for table in statement.find_all(exp.Table):
        if table.name in cte_names:
            continue
        if table.catalog or table.db:
            raise SQLQueryError("Catalog-qualified SQL table names are not supported yet")
        validate_table_name(table.name)
        names.add(table.name)
    return names


def referenced_scalar_function_calls(query: str | exp.Expression) -> tuple[exp.Anonymous, ...]:
    """Return user-defined scalar calls, excluding Klein built-ins and hints."""

    from sqlglot import exp

    statement = parse_statement(query) if isinstance(query, str) else query
    calls = []
    for function in statement.find_all(exp.Anonymous):
        if function.find_ancestor(exp.Hint) is not None:
            continue
        if function.name.casefold() in BUILTIN_ANONYMOUS_FUNCTIONS:
            continue
        calls.append(function)
    return tuple(calls)
