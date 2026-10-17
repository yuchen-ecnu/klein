# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ray.data.expressions import DownloadExpr
from sqlglot import exp

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.ai_function_execution import apply_batch_ai_projections
from ray.klein._internal.sql.ai_function_registry import AIFunctionSpec
from ray.klein._internal.sql.batch_join import JOIN_KEY_COLUMN, coalesced_join_key_expression, join_key_expression
from ray.klein._internal.sql.download_runtime import SQLDownloadPolicy, apply_batch_downloads
from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein._internal.sql.media_function import plan_media_projections, validate_media_function_calls
from ray.klein._internal.sql.media_function_execution import apply_batch_media_computations
from ray.klein._internal.sql.ray_data_expression import to_ray_data_expression
from ray.klein._internal.sql.scalar_function_registry import ScalarFunction
from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from ray.data import Dataset


def _function_options(
    functions: Mapping[str, ScalarFunction] | None,
) -> dict[str, Any]:
    return {"functions": functions} if functions else {}


def _ai_function_options(
    ai_functions: Mapping[str, AIFunctionSpec] | None,
) -> dict[str, Any]:
    return {"ai_functions": ai_functions} if ai_functions else {}


def _download_policy_options(
    download_policy: SQLDownloadPolicy | None,
) -> dict[str, Any]:
    return {"download_policy": download_policy} if download_policy is not None else {}


class _QualifyRow:
    def __init__(self, alias: str) -> None:
        self._alias = alias

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        return {f"{self._alias}.{name}": value for name, value in row.items()}


class _FilterRow:
    def __init__(
        self,
        expression: exp.Expression,
        functions: Mapping[str, ScalarFunction] | None = None,
    ) -> None:
        self._expression = expression
        self._functions = FrozenMapping(functions or {})

    def __call__(self, row: dict[str, Any]) -> bool:
        return evaluate_expression(self._expression, row, self._functions) is True


class _ProjectRow:
    def __init__(
        self,
        projections: Sequence[exp.Expression],
        functions: Mapping[str, ScalarFunction] | None = None,
    ) -> None:
        self._projections = tuple(projections)
        self._functions = FrozenMapping(functions or {})

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
            result[name] = evaluate_expression(value_expression, row, self._functions)
        return result

    @staticmethod
    def _copy_star(result: dict[str, Any], row: Mapping[str, Any], table: str | None) -> None:
        for source_name, value in row.items():
            if source_name.startswith("_klein_"):
                continue
            if table is not None and not source_name.startswith(f"{table}."):
                continue
            output_name = source_name.split(".", 1)[-1]
            if table is None and "." in source_name and output_name in row:
                # USING exposes one coalesced unqualified key in SELECT *,
                # while alias.* still exposes that side's original key.
                continue
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
    def __init__(
        self,
        expressions: Sequence[tuple[str, exp.Expression]],
        functions: Mapping[str, ScalarFunction] | None = None,
    ) -> None:
        self._expressions = tuple(expressions)
        self._functions = FrozenMapping(functions or {})

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        for name, expression in self._expressions:
            result[name] = evaluate_expression(expression, row, self._functions)
        return result


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
    schema = dataset.schema()
    if schema is not None and schema.names:
        # Native projection retains Arrow types and the schema of an empty
        # relation; a row map cannot qualify columns when there are no rows.
        qualified = dataset.rename_columns({name: f"{alias}.{name}" for name in schema.names}, num_cpus=num_cpus)
    else:
        qualified = dataset.map(_QualifyRow(alias), num_cpus=num_cpus)
    return qualified, alias, input_blocks


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
    from ray.data.expressions import lit

    using_columns: set[str] = set()
    for index, join in enumerate(select.args.get("joins") or ()):
        if index:
            # Ray reserves resources for every hash-shuffle actor pool in a
            # Dataset DAG up front. Finish the preceding join before another
            # pool can exhaust the resources needed by its input tasks.
            dataset = dataset.materialize()
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
            dataset = dataset.with_column(key, lit(1), num_cpus=context.num_cpus).join(
                right.with_column(key, lit(1), num_cpus=context.num_cpus),
                join_type="inner",
                num_partitions=partitions,
                on=(key,),
            )
            dataset = dataset.drop_columns([key])
        else:
            left_keys, right_keys = _join_keys(join, aliases, right_alias)
            using_names = tuple(identifier.name for identifier in join.args.get("using") or ())
            if using_names:
                left_keys = tuple(
                    name if name in using_columns else key for name, key in zip(using_names, left_keys, strict=True)
                )
            dataset = dataset.with_column(
                JOIN_KEY_COLUMN, join_key_expression(left_keys, left=True), num_cpus=context.num_cpus
            ).join(
                right.with_column(
                    JOIN_KEY_COLUMN, join_key_expression(right_keys, left=False), num_cpus=context.num_cpus
                ),
                join_type=join_type,
                num_partitions=partitions,
                on=(JOIN_KEY_COLUMN,),
            )
            # Ray coalesces its join keys. Joining on a private key preserves
            # both original SQL columns, including NULLs on unmatched sides.
            dataset = dataset.drop_columns([JOIN_KEY_COLUMN])
            if using_names:
                for name, left_key, right_key in zip(using_names, left_keys, right_keys, strict=True):
                    dataset = dataset.with_column(
                        name, coalesced_join_key_expression(left_key, right_key), num_cpus=context.num_cpus
                    )
                using_columns.update(using_names)
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


def _extract_aggregate_expressions(select: exp.Select) -> tuple[exp.Select, list[exp.Expression] | None]:
    """Lower scalar expressions around aggregates to a final SQL projection."""
    values = [projection.this if isinstance(projection, exp.Alias) else projection for projection in select.expressions]
    if not any(value.find(exp.AggFunc) and not isinstance(value, exp.AggFunc) for value in values):
        return select, None
    group = select.args.get("group")
    group_expressions = tuple(group.expressions) if group is not None else ()
    group_names = {expression.sql(): f"_klein_group_output_{i}" for i, expression in enumerate(group_expressions)}
    flattened = [exp.alias_(expression.copy(), group_names[expression.sql()]) for expression in group_expressions]
    aggregate_names: dict[str, str] = {}

    def rewrite(node: exp.Expression) -> exp.Expression:
        key = node.sql()
        if key in group_names:
            return exp.column(group_names[key])
        if isinstance(node, exp.AggFunc):
            if key not in aggregate_names:
                name = f"_klein_aggregate_output_{len(aggregate_names)}"
                aggregate_names[key] = name
                flattened.append(exp.alias_(node.copy(), name))
            return exp.column(aggregate_names[key])
        if isinstance(node, exp.Column):
            raise SQLQueryError(f"Non-aggregate projection {node.sql()!r} must appear in GROUP BY")
        return node

    projected = [
        exp.alias_(value.transform(rewrite), projection.alias_or_name or projection.sql(), quoted=True)
        for projection, value in zip(select.expressions, values, strict=True)
    ]
    lowered = select.copy()
    lowered.set("expressions", flattened)
    return lowered, projected


def _aggregate_select(
    select: exp.Select,
    dataset: Dataset,
    *,
    aliases: Sequence[str],
    input_blocks: int | None,
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    select, final_projection = _extract_aggregate_expressions(select)
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
            **_function_options(functions),
            **_download_policy_options(download_policy),
        )
    # Empty hash aggregates in Ray have no output blocks and therefore no
    # schema. Keep the input distributed while obtaining a definitive row
    # count and expression types; this also releases preceding shuffle actors.
    dataset = dataset.materialize()
    if dataset.count() == 0:
        result = _empty_aggregate(select, outputs, dataset)
    else:
        grouped = dataset.groupby(
            _group_key(group_fields),
            num_partitions=_shuffle_partitions(dataset, known_blocks=(input_blocks,)),
        )
        result = _select_sql_fields(grouped.aggregate(*aggregators), outputs, num_cpus=num_cpus)
    if final_projection is None:
        return result
    return _project_select(
        final_projection,
        result,
        aliases=(),
        num_cpus=num_cpus,
        **_function_options(functions),
        **_download_policy_options(download_policy),
    )


def _empty_aggregate(select: exp.Select, outputs: Sequence[tuple[str, str]], dataset: Dataset) -> Dataset:
    """Represent SQL's empty grouped/global aggregate with explicit columns."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import ray.data

    schema = dataset.schema()
    types = dict(zip(schema.names, schema.types, strict=True)) if schema is not None else {}
    grouped = select.args.get("group") is not None
    arrays = []
    names = []
    aggregate_index = 0
    for projection, (name, source) in zip(select.expressions, outputs, strict=True):
        expression = projection.this if isinstance(projection, exp.Alias) else projection
        dtype = types.get(source, pa.null())
        value = None
        if isinstance(expression, exp.AggFunc):
            if isinstance(expression, exp.Count):
                dtype, value = pa.int64(), 0
            else:
                input_type = types.get(f"_klein_aggregate_input_{aggregate_index}", pa.null())
                reduce = {exp.Sum: pc.sum, exp.Min: pc.min, exp.Max: pc.max, exp.Avg: pc.mean}[type(expression)]
                dtype = reduce(pa.array([], type=input_type)).type
            aggregate_index += 1
        arrays.append(pa.array([] if grouped else [value], type=dtype))
        names.append(name)
    return ray.data.from_arrow(pa.Table.from_arrays(arrays, names=names))


def _select_sql_fields(dataset: Dataset, outputs: Sequence[tuple[str, str]], *, num_cpus: float) -> Dataset:
    """Project and alias natively so zero-row relations keep their schema."""
    from ray.data.expressions import col

    temporary = []
    names = {}
    for index, (output, source) in enumerate(outputs):
        name = f"_klein_output_{index}"
        dataset = dataset.with_column(name, col(source), num_cpus=num_cpus)
        temporary.append(name)
        names[name] = output
    return dataset.select_columns(temporary, num_cpus=num_cpus).rename_columns(names, num_cpus=num_cpus)


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
    functions: Mapping[str, ScalarFunction] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    fallback: list[tuple[str, exp.Expression]] = []
    downloads: list[tuple[str, str, Any]] = []
    for name, expression in expressions:
        ray_expression = to_ray_data_expression(expression, aliases)
        if ray_expression is None:
            fallback.append((name, expression))
        elif isinstance(ray_expression, DownloadExpr):
            downloads.append((name, ray_expression.uri_column_name, ray_expression.filesystem))
        else:
            dataset = dataset.with_column(name, ray_expression, num_cpus=num_cpus)
    if downloads:
        dataset = apply_batch_downloads(
            dataset,
            downloads,
            policy=download_policy or SQLDownloadPolicy(),
            num_cpus=num_cpus,
        )
    if fallback:
        dataset = _add_row_expressions(dataset, fallback, functions, num_cpus=num_cpus)
    return dataset


def _add_row_expressions(
    dataset: Dataset,
    expressions: Sequence[tuple[str, exp.Expression]],
    functions: Mapping[str, ScalarFunction] | None,
    *,
    num_cpus: float,
) -> Dataset:
    from ray.data.expressions import lit

    # A row UDF cannot run on an empty relation; Ray's map then loses even its
    # input columns. Stage blocks without collecting rows on the driver and
    # keep empty expression columns explicit, without invoking user functions.
    dataset = dataset.materialize()
    if dataset.count() == 0:
        for name, _ in expressions:
            dataset = dataset.with_column(name, lit(None), num_cpus=num_cpus)
        return dataset
    return dataset.map(_AddExpressions(expressions, functions), num_cpus=num_cpus)


def _project_select(
    projections: Sequence[exp.Expression],
    dataset: Dataset,
    *,
    aliases: Sequence[str],
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    ai_functions: Mapping[str, AIFunctionSpec] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    media_plan = plan_media_projections(projections)
    if media_plan.downloads:
        dataset = _add_sql_expressions(
            dataset,
            media_plan.downloads,
            aliases=aliases,
            num_cpus=num_cpus,
            **_function_options(functions),
            **_download_policy_options(download_policy),
        )
    dataset = apply_batch_media_computations(
        dataset,
        media_plan.computations,
        functions=functions or {},
        num_cpus=num_cpus,
    )
    projections = media_plan.projections
    dataset, projections = apply_batch_ai_projections(
        dataset,
        projections,
        functions=functions or {},
        ai_functions=ai_functions or {},
    )
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
        **_function_options(functions),
        **_download_policy_options(download_policy),
    )
    if all(output is not None and source is not None for output, source in fields):
        return _select_sql_fields(
            dataset,
            [(output, source) for output, source in fields if output is not None and source is not None],
            num_cpus=num_cpus,
        )
    schema = dataset.schema()
    if schema is not None:
        # Resolve star expansion from column names, without sampling any rows.
        sources = _FinalizeProjection(fields)({name: name for name in schema.names})
        return _select_sql_fields(dataset, tuple(sources.items()), num_cpus=num_cpus)
    return dataset.map(_FinalizeProjection(fields), num_cpus=num_cpus)


def _parse_limit_literal(expression: exp.Expression) -> int:
    if not isinstance(expression, exp.Literal) or expression.is_string:
        raise SQLQueryError("LIMIT must be a non-negative integer literal")
    try:
        value = int(expression.this)
    except (TypeError, ValueError) as error:
        raise SQLQueryError("LIMIT must be a non-negative integer literal") from error
    if value < 0:
        raise SQLQueryError("LIMIT must be a non-negative integer literal")
    return value


def _apply_order_and_limit(select: exp.Select, dataset: Dataset) -> Dataset:
    from ray.data.expressions import col
    from sqlglot import exp

    order = select.args.get("order")
    if order is not None:
        keys: list[str] = []
        descending: list[bool] = []
        null_keys: list[str] = []
        for index, ordered in enumerate(order.expressions):
            expression = ordered.this
            if not isinstance(expression, exp.Column):
                raise SQLQueryError("ORDER BY currently supports output column names only")
            if ordered.args.get("nulls_first"):
                # Ray sorts NULLs last regardless of direction. An explicit
                # non-null Boolean prefix implements SQL's per-column order.
                null_key = f"_klein_sort_null_{index}"
                dataset = dataset.with_column(null_key, col(expression.name).is_null())
                null_keys.append(null_key)
                keys.append(null_key)
                descending.append(True)
            keys.append(expression.name)
            descending.append(bool(ordered.args.get("desc")))
        dataset = dataset.sort(
            keys[0] if len(keys) == 1 else keys, descending=descending[0] if len(keys) == 1 else descending
        )
        if null_keys:
            dataset = dataset.drop_columns(null_keys)

    limit = select.args.get("limit")
    if limit is not None:
        dataset = dataset.limit(_parse_limit_literal(limit.expression))
    return dataset


def _execute_select(
    select: exp.Select,
    datasets: Mapping[str, Dataset],
    inherited_ctes: Mapping[str, Dataset],
    *,
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    ai_functions: Mapping[str, AIFunctionSpec] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
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
            dataset = dataset.filter(_FilterRow(where.this, functions), num_cpus=num_cpus)
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
            **_function_options(functions),
            **_download_policy_options(download_policy),
        )
    else:
        dataset = _project_select(
            select.expressions,
            dataset,
            aliases=aliases,
            num_cpus=num_cpus,
            **_function_options(functions),
            **_ai_function_options(ai_functions),
            **_download_policy_options(download_policy),
        )
    return _apply_order_and_limit(select, dataset)


def _materialize_shuffled_subquery(query: exp.Query, dataset: Dataset) -> Dataset:
    """Release a subquery's shuffle actors before a consuming SQL stage."""

    if query.find(exp.Join, exp.Group, exp.AggFunc) is not None:
        return dataset.materialize()
    return dataset


def _execute_query_ast(
    query: exp.Query,
    datasets: Mapping[str, Dataset],
    ctes: Mapping[str, Dataset],
    *,
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    ai_functions: Mapping[str, AIFunctionSpec] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    from sqlglot import exp

    with_expression = query.args.get("with_")
    if with_expression is not None:
        if with_expression.args.get("recursive"):
            raise SQLQueryError("Recursive CTEs are not supported")
        nested_ctes = dict(ctes)
        for cte in with_expression.expressions:
            result = _execute_query_ast(
                cte.this,
                datasets,
                nested_ctes,
                num_cpus=num_cpus,
                **_function_options(functions),
                **_ai_function_options(ai_functions),
                **_download_policy_options(download_policy),
            )
            nested_ctes[cte.alias] = _materialize_shuffled_subquery(cte.this, result)
        query = query.copy()
        query.set("with_", None)
        ctes = nested_ctes

    if isinstance(query, exp.Select):
        return _execute_select(
            query,
            datasets,
            ctes,
            num_cpus=num_cpus,
            **_function_options(functions),
            **_ai_function_options(ai_functions),
            **_download_policy_options(download_policy),
        )
    if isinstance(query, exp.Union):
        if query.args.get("distinct") is not False:
            raise SQLQueryError("UNION DISTINCT is not supported yet; use UNION ALL")
        left = _execute_query_ast(
            query.this,
            datasets,
            ctes,
            num_cpus=num_cpus,
            **_function_options(functions),
            **_ai_function_options(ai_functions),
            **_download_policy_options(download_policy),
        )
        left = _materialize_shuffled_subquery(query.this, left)
        right = _execute_query_ast(
            query.expression,
            datasets,
            ctes,
            num_cpus=num_cpus,
            **_function_options(functions),
            **_ai_function_options(ai_functions),
            **_download_policy_options(download_policy),
        )
        right = _materialize_shuffled_subquery(query.expression, right)
        return left.union(right)
    raise SQLQueryError(f"Unsupported SQL query form: {query.key.upper()}")


def execute_sql_query(
    query: str,
    table_names: Sequence[str],
    datasets: Sequence[Dataset],
    *,
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    ai_functions: Mapping[str, AIFunctionSpec] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    """Lower SQL to native Ray operators, staging dependent hash shuffles.

    Intermediate shuffle results stay in Ray's distributed object store, where
    they can spill, instead of holding multiple actor pools' resource
    reservations for the lifetime of one combined Dataset DAG.
    """

    from ray.klein._internal.sql.validation import validate_read_query

    statement = validate_read_query(query)
    validate_media_function_calls(statement)
    if len(table_names) != len(datasets):
        raise ValueError(f"SQL has {len(table_names)} table names but {len(datasets)} input datasets")
    return _execute_query_ast(
        statement,
        dict(zip(table_names, datasets, strict=True)),
        {},
        num_cpus=num_cpus,
        **_function_options(functions),
        **_ai_function_options(ai_functions),
        **_download_policy_options(download_policy),
    )


def sql_source(
    query: str,
    *,
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    ai_functions: Mapping[str, AIFunctionSpec] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    return execute_sql_query(
        query,
        (),
        (),
        num_cpus=num_cpus,
        **_function_options(functions),
        **_ai_function_options(ai_functions),
        **_download_policy_options(download_policy),
    )


def sql_transform(
    primary: Dataset,
    query: str,
    table_names: Sequence[str],
    *other_datasets: Dataset,
    num_cpus: float,
    functions: Mapping[str, ScalarFunction] | None = None,
    ai_functions: Mapping[str, AIFunctionSpec] | None = None,
    download_policy: SQLDownloadPolicy | None = None,
) -> Dataset:
    return execute_sql_query(
        query,
        table_names,
        (primary, *other_datasets),
        num_cpus=num_cpus,
        **_function_options(functions),
        **_ai_function_options(ai_functions),
        **_download_policy_options(download_policy),
    )
