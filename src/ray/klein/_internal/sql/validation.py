# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from sqlglot import exp

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SQL_DIALECT = "spark"


def validate_table_name(name: str) -> None:
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        raise SQLQueryError(
            f"Invalid SQL table name {name!r}; use an unquoted identifier containing letters, numbers, and underscores"
        )


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
