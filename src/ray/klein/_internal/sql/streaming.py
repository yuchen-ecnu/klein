# SPDX-License-Identifier: Apache-2.0
"""Lower SQLGlot plans to Klein's checkpointed streaming operators."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from ray.klein._internal.duration import parse_duration
from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.execution import (
    _FilterRow,
    _has_aggregates,
    _join_keys,
    _join_type,
    _parse_limit_literal,
    _ProjectRow,
)
from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein._internal.sql.ray_data_expression import (
    is_ray_data_only_expression,
    to_ray_data_expression,
)
from ray.klein._internal.sql.scalar_function_registry import (
    ScalarFunction,
    contains_scalar_function,
)
from ray.klein._internal.sql.validation import validate_read_query
from ray.klein._internal.streaming_expression import (
    DEFAULT_EXPRESSION_ASYNC_BUFFER_SIZE,
    StreamingExpressionEvaluator,
    StreamingExpressionFilter,
)
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
    def __init__(
        self,
        projections: Sequence[exp.Expression],
        functions: Mapping[str, ScalarFunction] | None = None,
    ) -> None:
        self._project = _ProjectRow(projections, functions)

    def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        return ChangelogRow(self._project(dict(row)), row_kind=row_kind_of(row))


class _RayProjectChangelogRow:
    """Project SQL expressions that need task-local Ray expression state."""

    def __init__(
        self,
        projections: Sequence[exp.Expression],
        aliases: Sequence[str],
        functions: Mapping[str, ScalarFunction],
        runtime_context,
    ) -> None:
        self._projections = tuple(projections)
        self._functions = FrozenMapping(functions)
        self._evaluators = _compile_ray_evaluators(self._projections, aliases, runtime_context)
        if any(evaluator.is_async for evaluator in self._evaluators.values()):
            raise TypeError("DownloadExpr requires _AsyncRayProjectChangelogRow")

    def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        values = {index: evaluator.evaluate(row) for index, evaluator in self._evaluators.items()}
        return _render_projection(self._projections, row, values, self._functions)


class _AsyncRayProjectChangelogRow:
    """Project SQL expressions containing one or more DOWNLOAD calls."""

    def __init__(
        self,
        projections: Sequence[exp.Expression],
        aliases: Sequence[str],
        functions: Mapping[str, ScalarFunction],
        runtime_context,
    ) -> None:
        self._projections = tuple(projections)
        self._functions = FrozenMapping(functions)
        self._evaluators = _compile_ray_evaluators(self._projections, aliases, runtime_context)

    async def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        values: dict[int, Any] = {}
        for index, evaluator in self._evaluators.items():
            if not evaluator.is_async:
                values[index] = evaluator.evaluate(row)
        for index, evaluator in self._evaluators.items():
            if evaluator.is_async:
                values[index] = await evaluator.evaluate_async(row)
        return _render_projection(self._projections, row, values, self._functions)


class _AddStreamingExpressions:
    """Append precomputed SQL fields before a stateful aggregate."""

    def __init__(
        self,
        expressions: Sequence[tuple[str, exp.Expression]],
        aliases: Sequence[str],
        functions: Mapping[str, ScalarFunction],
        runtime_context,
    ) -> None:
        self._expressions = tuple(expressions)
        self._functions = FrozenMapping(functions)
        projections = tuple(expression for _, expression in self._expressions)
        self._evaluators = _compile_ray_evaluators(projections, aliases, runtime_context)
        if any(evaluator.is_async for evaluator in self._evaluators.values()):
            raise TypeError("DownloadExpr requires _AsyncAddStreamingExpressions")

    def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        result = dict(row)
        for index, (name, expression) in enumerate(self._expressions):
            evaluator = self._evaluators.get(index)
            result[name] = (
                evaluator.evaluate(row)
                if evaluator is not None
                else evaluate_expression(expression, row, self._functions)
            )
        return ChangelogRow(result, row_kind=row_kind_of(row))


class _AsyncAddStreamingExpressions:
    """Asynchronously append fields when an aggregate input uses DOWNLOAD."""

    def __init__(
        self,
        expressions: Sequence[tuple[str, exp.Expression]],
        aliases: Sequence[str],
        functions: Mapping[str, ScalarFunction],
        runtime_context,
    ) -> None:
        self._expressions = tuple(expressions)
        self._functions = FrozenMapping(functions)
        projections = tuple(expression for _, expression in self._expressions)
        self._evaluators = _compile_ray_evaluators(projections, aliases, runtime_context)

    async def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        result = dict(row)
        for index, (name, expression) in enumerate(self._expressions):
            evaluator = self._evaluators.get(index)
            if evaluator is None:
                result[name] = evaluate_expression(expression, row, self._functions)
            elif not evaluator.is_async:
                result[name] = evaluator.evaluate(row)
        for index, (name, _expression) in enumerate(self._expressions):
            evaluator = self._evaluators.get(index)
            if evaluator is not None and evaluator.is_async:
                result[name] = await evaluator.evaluate_async(row)
        return ChangelogRow(result, row_kind=row_kind_of(row))


def _compile_ray_evaluators(
    expressions: Sequence[exp.Expression],
    aliases: Sequence[str],
    runtime_context,
) -> dict[int, StreamingExpressionEvaluator]:
    evaluators: dict[int, StreamingExpressionEvaluator] = {}
    for index, expression in enumerate(expressions):
        value = expression.this if isinstance(expression, exp.Alias) else expression
        if isinstance(value, exp.Star) or (isinstance(value, exp.Column) and value.name == "*"):
            continue
        ray_expression = to_ray_data_expression(expression, aliases)
        if ray_expression is None and is_ray_data_only_expression(expression):
            raise SQLQueryError(f"Unsupported streaming Ray Data expression: {expression.sql()}")
        if ray_expression is not None:
            evaluators[index] = StreamingExpressionEvaluator(ray_expression, runtime_context)
    return evaluators


def _render_projection(
    projections: Sequence[exp.Expression],
    row: Mapping[str, Any],
    ray_values: Mapping[int, Any],
    functions: Mapping[str, ScalarFunction],
) -> ChangelogRow:
    result: dict[str, Any] = {}
    for index, projection in enumerate(projections):
        value_expression = projection.this if isinstance(projection, exp.Alias) else projection
        if isinstance(value_expression, exp.Star):
            _ProjectRow._copy_star(result, row, table=None)
            continue
        if isinstance(value_expression, exp.Column) and value_expression.name == "*":
            _ProjectRow._copy_star(result, row, table=value_expression.table or None)
            continue
        name = projection.alias_or_name or projection.sql()
        if name in result:
            raise SQLQueryError(f"Duplicate SQL output column {name!r}; add an explicit alias")
        result[name] = (
            ray_values[index] if index in ray_values else evaluate_expression(value_expression, row, functions)
        )
    return ChangelogRow(result, row_kind=row_kind_of(row))


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
    functions: Mapping[str, ScalarFunction],
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
        return _apply_streaming_projection(
            scalar,
            statement.expressions,
            (),
            functions=functions,
            num_cpus=num_cpus,
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
        stream = _apply_streaming_filter(
            stream,
            where.this,
            aliases,
            functions=functions,
            num_cpus=num_cpus,
        )

    if _has_aggregates(statement):
        stream = _apply_streaming_aggregate(
            stream,
            statement,
            ttl_hints,
            aliases,
            functions=functions,
            num_cpus=num_cpus,
        )
    else:
        stream = _apply_streaming_projection(
            stream,
            statement.expressions,
            aliases,
            functions=functions,
            num_cpus=num_cpus,
        )
    if statement.args.get("order") is not None:
        stream = _apply_streaming_top_n(stream, statement, num_cpus=num_cpus)
    return stream


def _apply_streaming_filter(
    stream: DataStream,
    expression: exp.Expression,
    aliases: Sequence[str],
    *,
    functions: Mapping[str, ScalarFunction],
    num_cpus: float,
) -> DataStream:
    if not is_ray_data_only_expression(expression):
        return stream.filter(
            _FilterRow,
            fn_constructor_args=[expression, functions],
            num_cpus=num_cpus,
            name="SQLFilter",
        )
    predicate = to_ray_data_expression(expression, aliases, predicate=True)
    if predicate is None:
        raise SQLQueryError(f"Unsupported streaming Ray Data predicate: {expression.sql()}")
    return stream.filter(
        StreamingExpressionFilter,
        fn_constructor_args=[predicate],
        num_cpus=num_cpus,
        name="SQLFilter",
    )


def _apply_streaming_projection(
    stream: DataStream,
    projections: Sequence[exp.Expression],
    aliases: Sequence[str],
    *,
    functions: Mapping[str, ScalarFunction],
    num_cpus: float,
) -> DataStream:
    ray_expressions = [expression for expression in projections if is_ray_data_only_expression(expression)]
    if not ray_expressions:
        return stream.map(
            _ProjectChangelogRow,
            fn_constructor_args=[projections, functions],
            num_cpus=num_cpus,
            name="SQLProject",
        )
    requires_async = _expressions_require_download(ray_expressions, aliases)
    return stream.map(
        _AsyncRayProjectChangelogRow if requires_async else _RayProjectChangelogRow,
        fn_constructor_args=[projections, aliases, functions],
        num_cpus=num_cpus,
        async_buffer_size=DEFAULT_EXPRESSION_ASYNC_BUFFER_SIZE if requires_async else None,
        name="SQLProject",
    )


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
    functions: Mapping[str, ScalarFunction],
    num_cpus: float,
) -> DataStream:
    group = select.args.get("group")
    original_groups = tuple(group.expressions) if group is not None else ()
    aggregate_arguments = tuple(
        value.this
        for projection in select.expressions
        if isinstance(
            (value := projection.this if isinstance(projection, exp.Alias) else projection),
            exp.AggFunc,
        )
        and value.this is not None
        and not isinstance(value.this, exp.Star)
    )
    requires_precompute = any(
        is_ray_data_only_expression(expression) or contains_scalar_function(expression, functions)
        for expression in (*original_groups, *aggregate_arguments)
    )
    if requires_precompute:
        computed, group_expressions, rewritten_projections = _streaming_aggregate_expression_plan(
            original_groups,
            select.expressions,
        )
        stream = _add_streaming_expressions(
            stream,
            computed,
            aliases,
            functions=functions,
            num_cpus=num_cpus,
        )
    else:
        group_expressions = original_groups
        rewritten_projections = list(select.expressions)
    selector = SQLGroupKeySelector(group_expressions)
    stream.partition_by(KeyPartitioner(key_selector=selector))
    hinted_ttls = [ttl_hints[alias] for alias in aliases if alias in ttl_hints]
    resources = Resources(num_cpus, None, None)
    result = DataStream(
        stream,
        SQLAggregateOperator(
            group_expressions=group_expressions,
            projections=rewritten_projections,
            state_ttl=max(hinted_ttls, default=None),
        ),
        "SQLGroupAggregate",
        NodeType.TRANSFORM,
        resources=resources,
    )
    result._set_changelog_mode(frozenset(RowKind))
    return result


def _streaming_aggregate_expression_plan(
    original_groups: Sequence[exp.Expression],
    projections: Sequence[exp.Expression],
) -> tuple[
    list[tuple[str, exp.Expression]],
    tuple[exp.Expression, ...],
    list[exp.Expression],
]:
    computed: list[tuple[str, exp.Expression]] = [
        (f"_klein_stream_group_{index}", expression) for index, expression in enumerate(original_groups)
    ]
    group_lookup = {expression.sql(): field_name for field_name, expression in computed}
    rewritten_projections: list[exp.Expression] = []
    aggregate_index = 0
    for projection in projections:
        value = projection.this if isinstance(projection, exp.Alias) else projection
        output_name = projection.alias_or_name or projection.sql()
        if isinstance(value, exp.AggFunc):
            rewritten = value.copy()
            argument = value.this
            if argument is not None and not isinstance(argument, exp.Star):
                input_name = f"_klein_stream_aggregate_input_{aggregate_index}"
                computed.append((input_name, argument))
                rewritten.set("this", exp.column(input_name))
            rewritten_projections.append(exp.alias_(rewritten, output_name, quoted=True))
            aggregate_index += 1
            continue
        try:
            group_name = group_lookup[value.sql()]
        except KeyError as error:
            raise SQLQueryError(f"Non-aggregate projection {value.sql()!r} must appear in GROUP BY") from error
        rewritten_projections.append(exp.alias_(exp.column(group_name), output_name, quoted=True))
    group_expressions = tuple(exp.column(name) for name, _ in computed[: len(original_groups)])
    return computed, group_expressions, rewritten_projections


def _add_streaming_expressions(
    stream: DataStream,
    expressions: Sequence[tuple[str, exp.Expression]],
    aliases: Sequence[str],
    *,
    functions: Mapping[str, ScalarFunction],
    num_cpus: float,
) -> DataStream:
    requires_async = _expressions_require_download(
        [expression for _, expression in expressions],
        aliases,
    )
    return stream.map(
        _AsyncAddStreamingExpressions if requires_async else _AddStreamingExpressions,
        fn_constructor_args=[expressions, aliases, functions],
        num_cpus=num_cpus,
        async_buffer_size=DEFAULT_EXPRESSION_ASYNC_BUFFER_SIZE if requires_async else None,
        name="SQLAggregateInputs",
    )


def _expressions_require_download(
    expressions: Sequence[exp.Expression],
    aliases: Sequence[str],
) -> bool:
    from ray.data.expressions import DownloadExpr

    for expression in expressions:
        if not is_ray_data_only_expression(expression):
            continue
        translated = to_ray_data_expression(expression, aliases)
        if translated is None:
            raise SQLQueryError(f"Unsupported streaming Ray Data expression: {expression.sql()}")
        if isinstance(translated, DownloadExpr):
            return True
    return False


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
    limit = _parse_limit_literal(select.args["limit"].expression)
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
        _parse_limit_literal(limit.expression)
    elif limit is not None:
        raise SQLQueryError("Streaming LIMIT without ORDER BY is not supported")
