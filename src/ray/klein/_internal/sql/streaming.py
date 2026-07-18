# SPDX-License-Identifier: Apache-2.0
"""Lower SQLGlot plans to Klein's checkpointed streaming operators."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from ray.klein._internal.duration import parse_duration
from ray.klein._internal.sql.execution import (
    _FilterRow,
    _has_aggregates,
    _join_keys,
    _join_type,
    _ProjectRow,
)
from ray.klein._internal.sql.validation import validate_read_query
from ray.klein.api.changelog_row import ChangelogRow, row_kind_of
from ray.klein.api.data_stream import DataStream
from ray.klein.api.node_type import NodeType
from ray.klein.api.row_kind import RowKind
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.runtime.operator.input_tag_operator import InputTagOperator
from ray.klein.runtime.operator.sql_aggregate_operator import (
    SQLAggregateOperator,
    SQLGroupKeySelector,
)
from ray.klein.runtime.operator.sql_join_operator import SQLRegularJoinOperator
from ray.klein.runtime.operator.sql_top_n_operator import SQLTopNOperator, global_top_n_key
from ray.klein.runtime.partitioning.key_partitioner import KeyPartitioner
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.klein_context import KleinContext


class _QualifyChangelogRow:
    def __init__(self, alias: str) -> None:
        self._alias = alias

    def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        values = {f"{self._alias}.{name}": value for name, value in row.items()}
        return ChangelogRow(values, row_kind=row_kind_of(row))


class _ProjectChangelogRow:
    def __init__(self, projections: Sequence[exp.Expression]) -> None:
        self._project = _ProjectRow(projections)

    def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        return ChangelogRow(self._project(dict(row)), row_kind=row_kind_of(row))


class _FieldTupleSelector:
    def __init__(self, fields: Sequence[str]) -> None:
        self._fields = tuple(fields)

    def __call__(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        return tuple(row[field] for field in self._fields)


def build_streaming_query(
    context: KleinContext,
    query: str,
    bindings: Mapping[str, DataStream],
    *,
    num_cpus: float,
) -> DataStream:
    """Build a continuous query using Klein operators and managed state."""

    statement = validate_read_query(query)
    if not isinstance(statement, exp.Select):
        raise SQLQueryError("Streaming SQL currently supports one SELECT query block; CTE and UNION require batch mode")
    _validate_streaming_select(statement)

    from_expression = statement.args.get("from_")
    if from_expression is None:
        if statement.args.get("joins"):
            raise SQLQueryError("JOIN requires a FROM relation")
        scalar = context.from_values({"_klein_scalar_row": True}, name="SQLScalarSource")
        return scalar.map(
            _ProjectChangelogRow,
            fn_constructor_args=[statement.expressions],
            num_cpus=num_cpus,
            name="SQLProject",
        )

    stream, alias = _streaming_relation(from_expression.this, bindings, num_cpus=num_cpus)
    aliases = [alias]
    ttl_hints = _state_ttl_hints(statement)
    for join in statement.args.get("joins") or ():
        stream, aliases = _apply_streaming_join(
            stream,
            aliases,
            join,
            bindings,
            ttl_hints,
            num_cpus=num_cpus,
        )
    unknown_ttl_aliases = ttl_hints.keys() - set(aliases)
    if unknown_ttl_aliases:
        names = ", ".join(sorted(unknown_ttl_aliases))
        raise SQLQueryError(f"STATE_TTL references unknown table alias(es): {names}")

    where = statement.args.get("where")
    if where is not None:
        stream = stream.filter(
            _FilterRow,
            fn_constructor_args=[where.this],
            num_cpus=num_cpus,
            name="SQLFilter",
        )

    if _has_aggregates(statement):
        stream = _apply_streaming_aggregate(
            stream,
            statement,
            ttl_hints,
            aliases,
            num_cpus=num_cpus,
        )
    else:
        stream = stream.map(
            _ProjectChangelogRow,
            fn_constructor_args=[statement.expressions],
            num_cpus=num_cpus,
            name="SQLProject",
        )
    if statement.args.get("order") is not None:
        stream = _apply_streaming_top_n(stream, statement, num_cpus=num_cpus)
    return stream


def _streaming_relation(
    table: exp.Expression,
    bindings: Mapping[str, DataStream],
    *,
    num_cpus: float,
) -> tuple[DataStream, str]:
    if not isinstance(table, exp.Table):
        raise SQLQueryError(f"Unsupported streaming SQL relation: {table.sql()}")
    try:
        stream = bindings[table.name]
    except KeyError as error:
        raise SQLQueryError(f"SQL query references unbound table {table.name!r}") from error
    alias = table.alias_or_name
    qualified = stream.map(
        _QualifyChangelogRow,
        fn_constructor_args=[alias],
        num_cpus=num_cpus,
        name=f"SQLQualify[{alias}]",
    )
    qualified._set_changelog_mode(stream.changelog_mode)
    return qualified, alias


def _apply_streaming_join(
    left: DataStream,
    left_aliases: list[str],
    join: exp.Join,
    bindings: Mapping[str, DataStream],
    ttl_hints: Mapping[str, timedelta],
    *,
    num_cpus: float,
) -> tuple[DataStream, list[str]]:
    join_type = _join_type(join)
    if join_type != "inner":
        raise SQLQueryError("Streaming SQL currently supports regular INNER JOIN only")
    right, right_alias = _streaming_relation(join.this, bindings, num_cpus=num_cpus)
    left_keys, right_keys = _join_keys(join, left_aliases, right_alias)

    left_tag = left._transform(InputTagOperator(input_tag=0), "SQLJoinLeft", left.resources)
    right_tag = right._transform(InputTagOperator(input_tag=1), "SQLJoinRight", right.resources)
    left_tag.partition_by(KeyPartitioner(key_selector=_FieldTupleSelector(left_keys)))
    right_tag.partition_by(KeyPartitioner(key_selector=_FieldTupleSelector(right_keys)))
    resources = Resources(num_cpus, None, None)
    result = DataStream(
        [left_tag, right_tag],
        SQLRegularJoinOperator(
            left_keys=left_keys,
            right_keys=right_keys,
            left_state_ttl=_left_join_ttl(left_aliases, ttl_hints),
            right_state_ttl=ttl_hints.get(right_alias),
        ),
        "SQLRegularJoin",
        NodeType.TRANSFORM,
        resources=resources,
    )
    result._set_changelog_mode(frozenset({RowKind.INSERT, RowKind.DELETE}))
    return result, [*left_aliases, right_alias]


def _left_join_ttl(left_aliases: Sequence[str], hints: Mapping[str, timedelta]) -> timedelta | None:
    values = [hints[alias] for alias in left_aliases if alias in hints]
    return max(values, default=None)


def _apply_streaming_aggregate(
    stream: DataStream,
    select: exp.Select,
    ttl_hints: Mapping[str, timedelta],
    aliases: Sequence[str],
    *,
    num_cpus: float,
) -> DataStream:
    group = select.args.get("group")
    group_expressions = tuple(group.expressions) if group is not None else ()
    selector = SQLGroupKeySelector(group_expressions)
    stream.partition_by(KeyPartitioner(key_selector=selector))
    hinted_ttls = [ttl_hints[alias] for alias in aliases if alias in ttl_hints]
    resources = Resources(num_cpus, None, None)
    result = DataStream(
        stream,
        SQLAggregateOperator(
            group_expressions=group_expressions,
            projections=select.expressions,
            state_ttl=max(hinted_ttls, default=None),
        ),
        "SQLGroupAggregate",
        NodeType.TRANSFORM,
        resources=resources,
    )
    result._set_changelog_mode(frozenset(RowKind))
    return result


def _state_ttl_hints(select: exp.Select) -> dict[str, timedelta]:
    hint = select.args.get("hint")
    result: dict[str, timedelta] = {}
    for expression in hint.expressions if isinstance(hint, exp.Hint) else ():
        if not isinstance(expression, exp.Anonymous) or expression.name.upper() != "STATE_TTL":
            continue
        for assignment in expression.expressions:
            if not (
                isinstance(assignment, exp.EQ)
                and isinstance(assignment.this, exp.Literal)
                and assignment.this.is_string
                and isinstance(assignment.expression, exp.Literal)
                and assignment.expression.is_string
            ):
                raise SQLQueryError("STATE_TTL hint entries must use 'table-or-alias'='duration'")
            alias = assignment.this.this
            try:
                result[alias] = parse_duration(assignment.expression.this)
            except ValueError as error:
                raise SQLQueryError(
                    f"Invalid STATE_TTL duration for {alias!r}: {assignment.expression.this!r}"
                ) from error
    return result


def _apply_streaming_top_n(stream: DataStream, select: exp.Select, *, num_cpus: float) -> DataStream:
    order = select.args["order"]
    limit_expression = select.args["limit"].expression
    limit = int(limit_expression.this)
    stream.partition_by(KeyPartitioner(key_selector=global_top_n_key))
    result = DataStream(
        stream,
        SQLTopNOperator(order=order.expressions, limit=limit),
        "SQLTopN",
        NodeType.TRANSFORM,
        resources=Resources(num_cpus, None, 1),
    )
    result._set_changelog_mode(frozenset({RowKind.INSERT, RowKind.DELETE}))
    return result


def _validate_streaming_select(select: exp.Select) -> None:
    if select.args.get("having") is not None:
        raise SQLQueryError("HAVING is not supported yet")
    if select.args.get("distinct") is not None:
        raise SQLQueryError("SELECT DISTINCT is not supported yet")
    order = select.args.get("order")
    limit = select.args.get("limit")
    if order is not None and limit is None:
        raise SQLQueryError(
            "Flink streaming ORDER BY requires an ascending time attribute as the primary key; "
            "Klein SQL time-attribute DDL is not implemented yet"
        )
    if order is not None:
        expression = limit.expression
        if not isinstance(expression, exp.Literal) or expression.is_string or int(expression.this) < 0:
            raise SQLQueryError("LIMIT must be a non-negative integer literal")
    elif limit is not None:
        raise SQLQueryError("Streaming LIMIT without ORDER BY is not supported")
