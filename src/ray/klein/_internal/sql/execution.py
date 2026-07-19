# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlglot import exp

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein._internal.sql.ray_data_expression import to_ray_data_expression
from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from ray.data import Dataset


class _QualifyRow:
    def __init__(self, alias: str) -> None:
        self._alias = alias

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        return {f"{self._alias}.{name}": value for name, value in row.items()}


class _FilterRow:
    def __init__(self, expression: exp.Expression) -> None:
        self._expression = expression

    def __call__(self, row: dict[str, Any]) -> bool:
        return evaluate_expression(self._expression, row) is True


class _ProjectRow:
    def __init__(self, projections: Sequence[exp.Expression]) -> None:
        self._projections = tuple(projections)

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        from sqlglot import exp

        result: dict[str, Any] = {}
        for projection in self._projections:
            value_expression = projection.this if isinstance(projection, exp.Alias) else projection
            if isinstance(value_expression, exp.Star):
                self._copy_star(result, row, table=None)
                continue
            if isinstance(value_expression, exp.Column) and value_expression.name == "*":
                self._copy_star(result, row, table=value_expression.table or None)
                continue
            name = projection.alias_or_name or projection.sql()
            if name in result:
                raise SQLQueryError(f"Duplicate SQL output column {name!r}; add an explicit alias")
            result[name] = evaluate_expression(value_expression, row)
        return result

    @staticmethod
    def _copy_star(result: dict[str, Any], row: Mapping[str, Any], table: str | None) -> None:
        for source_name, value in row.items():
            if source_name.startswith("_klein_"):
                continue
            if table is not None and not source_name.startswith(f"{table}."):
                continue
            output_name = source_name.split(".", 1)[-1]
            if output_name in result:
                output_name = source_name
            result[output_name] = value


class _FinalizeProjection:
    """Select already-computed fields while preserving SQL star behavior."""

    def __init__(self, fields: Sequence[tuple[str | None, str | None]]) -> None:
        self._fields = tuple(fields)

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for output_name, source_name in self._fields:
            if output_name is None:
                _ProjectRow._copy_star(result, row, table=source_name)
            else:
                if output_name in result:
                    raise SQLQueryError(f"Duplicate SQL output column {output_name!r}; add an explicit alias")
                if source_name is None:
                    raise RuntimeError(f"SQL projection {output_name!r} has no computed source")
                result[output_name] = row[source_name]
        return result


class _AddExpressions:
    def __init__(self, expressions: Sequence[tuple[str, exp.Expression]]) -> None:
        self._expressions = tuple(expressions)

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        for name, expression in self._expressions:
            result[name] = evaluate_expression(expression, row)
        return result


class _AddConstant:
    def __init__(self, name: str, value: Any) -> None:
        self._name = name
        self._value = value

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        return {**row, self._name: self._value}


class _FinalizeAggregate:
    def __init__(self, outputs: Sequence[tuple[str, str]]) -> None:
        self._outputs = tuple(outputs)

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        return {output: row[source] for output, source in self._outputs}


def _table_relation(
    table: exp.Expression,
    datasets: Mapping[str, Dataset],
    ctes: Mapping[str, Dataset],
    *,
    num_cpus: float,
) -> tuple[Dataset, str, int | None]:
    from sqlglot import exp

    if not isinstance(table, exp.Table):
        raise SQLQueryError(f"Unsupported SQL relation: {table.sql()}")
    name = table.name
    try:
        dataset = ctes[name] if name in ctes else datasets[name]
    except KeyError as exc:
        raise SQLQueryError(f"SQL query references unbound table {name!r}") from exc
    alias = table.alias_or_name
    # Capture the public block count before map() turns a materialized input
    # into a lazy Dataset. Carrying the estimate alongside the SQL relation
    # avoids either materializing the plan or reaching into Ray internals.
    input_blocks = _estimated_num_blocks(dataset)
    return dataset.map(_QualifyRow(alias), num_cpus=num_cpus), alias, input_blocks


def _join_type(join: exp.Join) -> str:
    side = (join.args.get("side") or "").upper()
    kind = (join.args.get("kind") or "").upper()
    if kind == "CROSS":
        return "cross"
    if not side:
        return "inner"
    try:
        return {"LEFT": "left_outer", "RIGHT": "right_outer", "FULL": "full_outer"}[side]
    except KeyError as exc:
        raise SQLQueryError(f"Unsupported SQL join type: {join.sql()}") from exc


def _flatten_and(expression: exp.Expression) -> list[exp.Expression]:
    from sqlglot import exp

    if isinstance(expression, exp.And):
        return [*_flatten_and(expression.this), *_flatten_and(expression.expression)]
    return [expression]


def _join_keys(
    join: exp.Join,
    left_aliases: Sequence[str],
    right_alias: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    using = join.args.get("using") or ()
    if using:
        return _using_join_keys(using, left_aliases[-1], right_alias)

    condition = join.args.get("on")
    if condition is None:
        raise SQLQueryError("JOIN requires ON or USING; use CROSS JOIN for a Cartesian product")
    left_keys: list[str] = []
    right_keys: list[str] = []
    for predicate in _flatten_and(condition):
        first, second = _join_columns(predicate)
        first, second = _orient_join_columns(first, second, left_aliases, right_alias)
        left_keys.append(f"{first.table}.{first.name}")
        right_keys.append(f"{second.table}.{second.name}")
    return tuple(left_keys), tuple(right_keys)


def _using_join_keys(
    identifiers: Sequence[exp.Identifier],
    left_alias: str,
    right_alias: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    names = tuple(identifier.name for identifier in identifiers)
    return tuple(f"{left_alias}.{name}" for name in names), tuple(f"{right_alias}.{name}" for name in names)


def _join_columns(predicate: exp.Expression) -> tuple[exp.Column, exp.Column]:
    if (
        not isinstance(predicate, exp.EQ)
        or not isinstance(predicate.this, exp.Column)
        or not isinstance(predicate.expression, exp.Column)
    ):
        raise SQLQueryError("Ray-native SQL JOIN supports conjunctions of column equality predicates")
    return predicate.this, predicate.expression


def _orient_join_columns(
    first: exp.Column,
    second: exp.Column,
    left_aliases: Sequence[str],
    right_alias: str,
) -> tuple[exp.Column, exp.Column]:
    if first.table == right_alias and second.table in left_aliases:
        first, second = second, first
    if first.table not in left_aliases or second.table != right_alias:
        raise SQLQueryError("JOIN columns must be qualified with their left and right table aliases")
    return first, second


@dataclass(frozen=True, slots=True)
class _JoinContext:
    datasets: Mapping[str, Dataset]
    ctes: Mapping[str, Dataset]
    num_cpus: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "datasets", FrozenMapping(self.datasets))
        object.__setattr__(self, "ctes", FrozenMapping(self.ctes))


def _apply_joins(
    select: exp.Select,
    dataset: Dataset,
    aliases: list[str],
    input_blocks: int | None,
    context: _JoinContext,
) -> tuple[Dataset, list[str], int]:
    for join in select.args.get("joins") or ():
        right, right_alias, right_blocks = _table_relation(
            join.this,
            context.datasets,
            context.ctes,
            num_cpus=context.num_cpus,
        )
        join_type = _join_type(join)
        partitions = _shuffle_partitions(
            dataset,
            right,
            known_blocks=(input_blocks, right_blocks),
        )
        if join_type == "cross":
            key = "_klein_cross_join_key"
            dataset = dataset.map(_AddConstant(key, 1), num_cpus=context.num_cpus).join(
                right.map(_AddConstant(key, 1), num_cpus=context.num_cpus),
                join_type="inner",
                num_partitions=partitions,
                on=(key,),
            )
            dataset = dataset.drop_columns([key])
        else:
            left_keys, right_keys = _join_keys(join, aliases, right_alias)
            dataset = dataset.join(
                right,
                join_type=join_type,
                num_partitions=partitions,
                on=left_keys,
                right_on=right_keys,
            )
        input_blocks = partitions
        aliases.append(right_alias)
    return dataset, aliases, max(1, input_blocks or _cluster_parallelism())


def _shuffle_partitions(
    *datasets: Dataset,
    known_blocks: Sequence[int | None] = (),
) -> int:
    """Bound SQL shuffle width by both Ray's default and actual input blocks."""

    from ray.data import DataContext

    configured = max(1, int(DataContext.get_current().min_parallelism))
    estimates = [estimate for estimate in known_blocks if estimate is not None]
    estimates.extend(estimate for dataset in datasets if (estimate := _estimated_num_blocks(dataset)) is not None)
    input_blocks = max(estimates, default=_cluster_parallelism())
    return max(1, min(configured, input_blocks))


def _cluster_parallelism() -> int:
    """Return a resource-safe fallback when a lazy plan has no public size."""

    import ray

    if not ray.is_initialized():
        return 1
    return max(1, int(ray.cluster_resources().get("CPU", 1)))


def _estimated_num_blocks(dataset: Dataset) -> int | None:
    """Return a public block count, or ``None`` for an unmaterialized Dataset."""

    try:
        return max(1, dataset.num_blocks())
    except NotImplementedError:
        # Official Ray exposes num_blocks only on MaterializedDataset. Do not
        # inspect the private execution plan or materialize just for a tuning
        # estimate; the configured shuffle width is the safe unknown fallback.
        return None


def _aggregate_function(
    aggregate: exp.AggFunc,
    input_name: str | None,
    output_name: str,
) -> Any:
    from ray.data.aggregate import Count, Max, Mean, Min, Sum
    from sqlglot import exp

    if isinstance(aggregate, exp.Count):
        return Count(on=input_name, ignore_nulls=input_name is not None, alias_name=output_name)
    if input_name is None:
        raise SQLQueryError(f"{aggregate.key.upper()} requires an input expression")
    if isinstance(aggregate, exp.Sum):
        return Sum(on=input_name, alias_name=output_name)
    if isinstance(aggregate, exp.Min):
        return Min(on=input_name, alias_name=output_name)
    if isinstance(aggregate, exp.Max):
        return Max(on=input_name, alias_name=output_name)
    if isinstance(aggregate, exp.Avg):
        return Mean(on=input_name, alias_name=output_name)
    raise SQLQueryError(f"Unsupported Ray-native SQL aggregate {aggregate.key.upper()}")


@dataclass(frozen=True, slots=True)
class _AggregateProjection:
    computed: tuple[str, exp.Expression] | None
    aggregator: Any | None
    output: tuple[str, str]


def _plan_aggregate_projection(
    projection: exp.Expression,
    aggregate_index: int,
    group_lookup: Mapping[str, str],
) -> _AggregateProjection:
    value = projection.this if isinstance(projection, exp.Alias) else projection
    output_name = projection.alias_or_name or projection.sql()
    if not isinstance(value, exp.AggFunc):
        try:
            group_field = group_lookup[value.sql()]
        except KeyError as exc:
            raise SQLQueryError(f"Non-aggregate projection {value.sql()!r} must appear in GROUP BY") from exc
        return _AggregateProjection(None, None, (output_name, group_field))

    input_expression = value.this
    input_name = None
    computed = None
    if input_expression is not None and not isinstance(input_expression, exp.Star):
        input_name = f"_klein_aggregate_input_{aggregate_index}"
        computed = (input_name, input_expression)
    aggregate_name = f"_klein_aggregate_{aggregate_index}"
    return _AggregateProjection(
        computed,
        _aggregate_function(value, input_name, aggregate_name),
        (output_name, aggregate_name),
    )


def _build_aggregate_plan(
    projections: Sequence[exp.Expression],
    group_fields: Sequence[tuple[str, exp.Expression]],
) -> tuple[list[tuple[str, exp.Expression]], list[Any], list[tuple[str, str]]]:
    group_lookup = {expression.sql(): field for field, expression in group_fields}
    computed = list(group_fields)
    aggregators = []
    outputs: list[tuple[str, str]] = []
    for projection in projections:
        plan = _plan_aggregate_projection(projection, len(aggregators), group_lookup)
        if plan.computed is not None:
            computed.append(plan.computed)
        if plan.aggregator is not None:
            aggregators.append(plan.aggregator)
        outputs.append(plan.output)
    if not aggregators:
        aggregators.append(_aggregate_function(exp.Count(), None, "_klein_group_count"))
    return computed, aggregators, outputs


def _group_key(group_fields: Sequence[tuple[str, exp.Expression]]) -> str | list[str] | None:
    keys = [name for name, _ in group_fields]
    if not keys:
        return None
    return keys[0] if len(keys) == 1 else keys


def _aggregate_select(
    select: exp.Select,
    dataset: Dataset,
    *,
    aliases: Sequence[str],
    input_blocks: int | None,
    num_cpus: float,
) -> Dataset:
    group = select.args.get("group")
    group_expressions = tuple(group.expressions) if group is not None else ()
    group_fields = [(f"_klein_group_{index}", expression) for index, expression in enumerate(group_expressions)]
    computed, aggregators, outputs = _build_aggregate_plan(select.expressions, group_fields)
    if computed:
        dataset = _add_sql_expressions(
            dataset,
            computed,
            aliases=aliases,
            num_cpus=num_cpus,
        )
    grouped = dataset.groupby(
        _group_key(group_fields),
        num_partitions=_shuffle_partitions(dataset, known_blocks=(input_blocks,)),
    )
    dataset = grouped.aggregate(*aggregators)
    return dataset.map(_FinalizeAggregate(outputs), num_cpus=num_cpus)


def _has_aggregates(select: exp.Select) -> bool:
    from sqlglot import exp

    return select.args.get("group") is not None or any(
        projection.find(exp.AggFunc) for projection in select.expressions
    )


def _add_sql_expressions(
    dataset: Dataset,
    expressions: Sequence[tuple[str, exp.Expression]],
    *,
    aliases: Sequence[str],
    num_cpus: float,
) -> Dataset:
    fallback: list[tuple[str, exp.Expression]] = []
    for name, expression in expressions:
        ray_expression = to_ray_data_expression(expression, aliases)
        if ray_expression is None:
            fallback.append((name, expression))
        else:
            dataset = dataset.with_column(name, ray_expression, num_cpus=num_cpus)
    if fallback:
        dataset = dataset.map(_AddExpressions(fallback), num_cpus=num_cpus)
    return dataset


def _project_select(
    projections: Sequence[exp.Expression],
    dataset: Dataset,
    *,
    aliases: Sequence[str],
    num_cpus: float,
) -> Dataset:
    computed: list[tuple[str, exp.Expression]] = []
    fields: list[tuple[str | None, str | None]] = []
    output_names: set[str] = set()
    for index, projection in enumerate(projections):
        value = projection.this if isinstance(projection, exp.Alias) else projection
        if isinstance(value, exp.Star):
            fields.append((None, None))
            continue
        if isinstance(value, exp.Column) and value.name == "*":
            fields.append((None, value.table or None))
            continue

        output_name = projection.alias_or_name or projection.sql()
        if output_name in output_names:
            raise SQLQueryError(f"Duplicate SQL output column {output_name!r}; add an explicit alias")
        output_names.add(output_name)
        temporary_name = f"_klein_projection_{index}"
        computed.append((temporary_name, value))
        fields.append((output_name, temporary_name))

    dataset = _add_sql_expressions(
        dataset,
        computed,
        aliases=aliases,
        num_cpus=num_cpus,
    )
    return dataset.map(_FinalizeProjection(fields), num_cpus=num_cpus)


def _apply_order_and_limit(select: exp.Select, dataset: Dataset) -> Dataset:
    from sqlglot import exp

    order = select.args.get("order")
    if order is not None:
        keys: list[str] = []
        descending: list[bool] = []
        for ordered in order.expressions:
            expression = ordered.this
            if not isinstance(expression, exp.Column):
                raise SQLQueryError("ORDER BY currently supports output column names only")
            keys.append(expression.name)
            descending.append(bool(ordered.args.get("desc")))
        dataset = dataset.sort(
            keys[0] if len(keys) == 1 else keys, descending=descending[0] if len(keys) == 1 else descending
        )

    limit = select.args.get("limit")
    if limit is not None:
        expression = limit.expression
        if not isinstance(expression, exp.Literal) or expression.is_string:
            raise SQLQueryError("LIMIT must be a non-negative integer literal")
        value = int(expression.this)
        if value < 0:
            raise SQLQueryError("LIMIT must be a non-negative integer literal")
        dataset = dataset.limit(value)
    return dataset


def _execute_select(
    select: exp.Select,
    datasets: Mapping[str, Dataset],
    inherited_ctes: Mapping[str, Dataset],
    *,
    num_cpus: float,
) -> Dataset:
    import ray.data

    ctes = dict(inherited_ctes)

    from_expression = select.args.get("from_")
    if from_expression is None:
        if select.args.get("joins"):
            raise SQLQueryError("JOIN requires a FROM relation")
        # Ray Data cannot construct an Arrow block from an empty mapping. The
        # private sentinel gives scalar SELECTs one logical row and is removed
        # by projection.
        dataset = ray.data.from_items([{"_klein_scalar_row": True}])
        aliases: list[str] = []
        input_blocks: int | None = 1
    else:
        dataset, alias, input_blocks = _table_relation(
            from_expression.this,
            datasets,
            ctes,
            num_cpus=num_cpus,
        )
        aliases = [alias]
        dataset, aliases, input_blocks = _apply_joins(
            select,
            dataset,
            aliases,
            input_blocks,
            _JoinContext(datasets, ctes, num_cpus),
        )

    where = select.args.get("where")
    if where is not None:
        predicate = to_ray_data_expression(where.this, aliases, predicate=True)
        if predicate is None:
            dataset = dataset.filter(_FilterRow(where.this), num_cpus=num_cpus)
        else:
            dataset = dataset.filter(expr=predicate, num_cpus=num_cpus)
    if select.args.get("having") is not None:
        raise SQLQueryError("HAVING is not supported yet")
    if select.args.get("distinct") is not None:
        raise SQLQueryError("SELECT DISTINCT is not supported yet")

    if _has_aggregates(select):
        dataset = _aggregate_select(
            select,
            dataset,
            aliases=aliases,
            input_blocks=input_blocks,
            num_cpus=num_cpus,
        )
    else:
        dataset = _project_select(
            select.expressions,
            dataset,
            aliases=aliases,
            num_cpus=num_cpus,
        )
    return _apply_order_and_limit(select, dataset)


def _execute_query_ast(
    query: exp.Query,
    datasets: Mapping[str, Dataset],
    ctes: Mapping[str, Dataset],
    *,
    num_cpus: float,
) -> Dataset:
    from sqlglot import exp

    with_expression = query.args.get("with_")
    if with_expression is not None:
        if with_expression.args.get("recursive"):
            raise SQLQueryError("Recursive CTEs are not supported")
        nested_ctes = dict(ctes)
        for cte in with_expression.expressions:
            nested_ctes[cte.alias] = _execute_query_ast(
                cte.this,
                datasets,
                nested_ctes,
                num_cpus=num_cpus,
            )
        query = query.copy()
        query.set("with_", None)
        ctes = nested_ctes

    if isinstance(query, exp.Select):
        return _execute_select(query, datasets, ctes, num_cpus=num_cpus)
    if isinstance(query, exp.Union):
        if query.args.get("distinct") is not False:
            raise SQLQueryError("UNION DISTINCT is not supported yet; use UNION ALL")
        left = _execute_query_ast(query.this, datasets, ctes, num_cpus=num_cpus)
        right = _execute_query_ast(query.expression, datasets, ctes, num_cpus=num_cpus)
        return left.union(right)
    raise SQLQueryError(f"Unsupported SQL query form: {query.key.upper()}")


def execute_sql_query(
    query: str,
    table_names: Sequence[str],
    datasets: Sequence[Dataset],
    *,
    num_cpus: float,
) -> Dataset:
    """Lower a SQLGlot query AST to a native, lazy Ray Dataset DAG."""

    from ray.klein._internal.sql.validation import validate_read_query

    statement = validate_read_query(query)
    if len(table_names) != len(datasets):
        raise ValueError(f"SQL has {len(table_names)} table names but {len(datasets)} input datasets")
    return _execute_query_ast(
        statement,
        dict(zip(table_names, datasets, strict=True)),
        {},
        num_cpus=num_cpus,
    )


def sql_source(query: str, *, num_cpus: float) -> Dataset:
    return execute_sql_query(query, (), (), num_cpus=num_cpus)


def sql_transform(
    primary: Dataset,
    query: str,
    table_names: Sequence[str],
    *other_datasets: Dataset,
    num_cpus: float,
) -> Dataset:
    return execute_sql_query(
        query,
        table_names,
        (primary, *other_datasets),
        num_cpus=num_cpus,
    )
