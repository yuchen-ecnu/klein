# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlglot import exp, parse_one

from ray.klein._internal.sql import execution
from ray.klein.api.sql_query_error import SQLQueryError


class _Dataset:
    """Small call-recording Ray Dataset stand-in; it never starts Ray."""

    def __init__(self, *, blocks: int | None = 2) -> None:
        self.blocks = blocks
        self.calls: list[tuple[object, ...]] = []

    def num_blocks(self) -> int:
        if self.blocks is None:
            raise NotImplementedError
        return self.blocks

    def map(self, function, **kwargs):
        self.calls.append(("map", function, kwargs))
        return self

    def with_column(self, name, expression, **kwargs):
        self.calls.append(("with_column", name, expression, kwargs))
        return self

    def filter(self, function=None, **kwargs):
        self.calls.append(("filter", function, kwargs))
        return self

    def join(self, other, **kwargs):
        self.calls.append(("join", other, kwargs))
        return self

    def drop_columns(self, columns):
        self.calls.append(("drop_columns", columns))
        return self

    def groupby(self, key, **kwargs):
        self.calls.append(("groupby", key, kwargs))
        return _Grouped(self)

    def sort(self, key, **kwargs):
        self.calls.append(("sort", key, kwargs))
        return self

    def limit(self, count):
        self.calls.append(("limit", count))
        return self

    def union(self, other):
        self.calls.append(("union", other))
        return self


class _Grouped:
    def __init__(self, dataset: _Dataset) -> None:
        self.dataset = dataset

    def aggregate(self, *aggregators):
        self.dataset.calls.append(("aggregate", aggregators))
        return self.dataset


def _select(query: str) -> exp.Select:
    statement = parse_one(query)
    assert isinstance(statement, exp.Select)
    return statement


def test_row_adapters_preserve_sql_data_semantics() -> None:
    assert execution._QualifyRow("orders")({"id": 1, "value": 2}) == {
        "orders.id": 1,
        "orders.value": 2,
    }
    assert execution._FilterRow(parse_one("x > 1"))({"x": 2}) is True
    assert execution._FilterRow(parse_one("x > 1"))({"x": 1}) is False
    assert execution._FilterRow(parse_one("x > 1"))({"x": None}) is False
    assert execution._AddConstant("x", 3)({"x": 1, "y": 2}) == {"x": 3, "y": 2}
    assert execution._FinalizeAggregate((("total", "_sum"),))({"_sum": 7}) == {"total": 7}


def test_project_row_handles_aliases_stars_internal_fields_and_collisions() -> None:
    projections = _select("SELECT *, orders.*, orders.amount + 1 AS adjusted FROM orders").expressions
    row = {
        "orders.id": 1,
        "orders.amount": 4,
        "customers.id": 9,
        "_klein_private": True,
    }

    assert execution._ProjectRow(projections)(row) == {
        "id": 1,
        "amount": 4,
        "customers.id": 9,
        "orders.id": 1,
        "orders.amount": 4,
        "adjusted": 5,
    }


def test_project_row_rejects_duplicate_output_names() -> None:
    projections = _select("SELECT a.id, b.id FROM a").expressions
    with pytest.raises(SQLQueryError, match="Duplicate SQL output column 'id'"):
        execution._ProjectRow(projections)({"a.id": 1, "b.id": 2})


def test_finalize_projection_handles_star_and_validation() -> None:
    row = {"a.id": 1, "b.id": 2, "_klein_projection_0": 5, "_klein_hidden": 9}
    finalize = execution._FinalizeProjection(((None, "a"), ("value", "_klein_projection_0")))
    assert finalize(row) == {"id": 1, "value": 5}

    with pytest.raises(SQLQueryError, match="Duplicate SQL output column 'id'"):
        execution._FinalizeProjection(((None, None), ("id", "a.id")))(row)
    with pytest.raises(RuntimeError, match="has no computed source"):
        execution._FinalizeProjection((("missing", None),))(row)


def test_add_expressions_reads_the_original_row_for_each_expression() -> None:
    expressions = _select("SELECT x + 1, x * 2 FROM rows").expressions
    add = execution._AddExpressions((("next", expressions[0]), ("double", expressions[1])))

    assert add({"x": 3}) == {"x": 3, "next": 4, "double": 6}


def test_table_relation_qualifies_rows_prefers_ctes_and_validates_inputs() -> None:
    input_dataset = _Dataset(blocks=3)
    cte_dataset = _Dataset(blocks=4)
    table = exp.Table(this=exp.Identifier(this="items"), alias=exp.TableAlias(this=exp.Identifier(this="i")))

    result, alias, blocks = execution._table_relation(
        table,
        {"items": input_dataset},
        {"items": cte_dataset},
        num_cpus=0.25,
    )

    assert result is cte_dataset
    assert alias == "i"
    assert blocks == 4
    operation, qualifier, options = cte_dataset.calls[0]
    assert operation == "map"
    assert qualifier({"id": 1}) == {"i.id": 1}
    assert options == {"num_cpus": 0.25}

    with pytest.raises(SQLQueryError, match="unbound table 'missing'"):
        execution._table_relation(exp.to_table("missing"), {}, {}, num_cpus=1)
    with pytest.raises(SQLQueryError, match="Unsupported SQL relation"):
        execution._table_relation(parse_one("SELECT 1"), {}, {}, num_cpus=1)


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM a JOIN b ON a.id = b.id", "inner"),
        ("SELECT * FROM a LEFT JOIN b ON a.id = b.id", "left_outer"),
        ("SELECT * FROM a RIGHT JOIN b ON a.id = b.id", "right_outer"),
        ("SELECT * FROM a FULL JOIN b ON a.id = b.id", "full_outer"),
        ("SELECT * FROM a CROSS JOIN b", "cross"),
    ],
)
def test_join_type_maps_sqlglot_join_variants(sql: str, expected: str) -> None:
    join = _select(sql).args["joins"][0]
    assert execution._join_type(join) == expected


def test_join_type_rejects_unknown_side() -> None:
    with pytest.raises(SQLQueryError, match="Unsupported SQL join type"):
        execution._join_type(exp.Join(this=exp.to_table("b"), side="SIDEWAYS"))


def test_join_keys_support_using_reversed_and_conjunctive_on() -> None:
    using = _select("SELECT * FROM a JOIN b USING (id, version)").args["joins"][0]
    assert execution._join_keys(using, ["a"], "b") == (
        ("a.id", "a.version"),
        ("b.id", "b.version"),
    )

    on = _select("SELECT * FROM a JOIN b ON b.id = a.id AND a.version = b.version").args["joins"][0]
    assert execution._join_keys(on, ["a"], "b") == (
        ("a.id", "a.version"),
        ("b.id", "b.version"),
    )


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("SELECT * FROM a JOIN b", "requires ON or USING"),
        ("SELECT * FROM a JOIN b ON a.id > b.id", "column equality predicates"),
        ("SELECT * FROM a JOIN b ON a.id = 1", "column equality predicates"),
        ("SELECT * FROM a JOIN b ON id = b.id", "must be qualified"),
        ("SELECT * FROM a JOIN b ON b.id = b.other", "must be qualified"),
    ],
)
def test_join_keys_reject_ambiguous_or_non_equi_conditions(sql: str, message: str) -> None:
    join = _select(sql).args["joins"][0]
    with pytest.raises(SQLQueryError, match=message):
        execution._join_keys(join, ["a"], "b")


def test_join_context_snapshots_bindings() -> None:
    datasets = {"a": _Dataset()}
    context = execution._JoinContext(datasets, {}, 0.5)
    datasets["b"] = _Dataset()

    assert tuple(context.datasets) == ("a",)
    with pytest.raises(TypeError):
        context.datasets["b"] = _Dataset()  # type: ignore[index]


def test_apply_joins_lowers_cross_join_without_join_keys(monkeypatch) -> None:
    left, right = _Dataset(), _Dataset()
    select = _select("SELECT * FROM a CROSS JOIN b")
    monkeypatch.setattr(execution, "_table_relation", lambda *_args, **_kwargs: (right, "b", 3))
    monkeypatch.setattr(execution, "_shuffle_partitions", lambda *_args, **_kwargs: 3)

    result, aliases, blocks = execution._apply_joins(
        select,
        left,
        ["a"],
        2,
        execution._JoinContext({}, {}, 0.5),
    )

    assert result is left
    assert aliases == ["a", "b"]
    assert blocks == 3
    assert [call[0] for call in left.calls] == ["map", "join", "drop_columns"]
    assert left.calls[1][2] == {"join_type": "inner", "num_partitions": 3, "on": ("_klein_cross_join_key",)}
    assert right.calls[0][1]({"id": 1}) == {"id": 1, "_klein_cross_join_key": 1}


def test_apply_joins_lowers_outer_join_keys(monkeypatch) -> None:
    left, right = _Dataset(), _Dataset()
    select = _select("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
    monkeypatch.setattr(execution, "_table_relation", lambda *_args, **_kwargs: (right, "b", None))
    monkeypatch.setattr(execution, "_shuffle_partitions", lambda *_args, **_kwargs: 4)

    result, aliases, blocks = execution._apply_joins(
        select,
        left,
        ["a"],
        None,
        execution._JoinContext({}, {}, 1),
    )

    assert (result, aliases, blocks) == (left, ["a", "b"], 4)
    assert left.calls == [
        (
            "join",
            right,
            {
                "join_type": "left_outer",
                "num_partitions": 4,
                "on": ("a.id",),
                "right_on": ("b.id",),
            },
        )
    ]


def test_apply_joins_without_joins_uses_cluster_fallback(monkeypatch) -> None:
    monkeypatch.setattr(execution, "_cluster_parallelism", lambda: 6)
    dataset = _Dataset()
    assert execution._apply_joins(
        _select("SELECT * FROM a"), dataset, ["a"], None, execution._JoinContext({}, {}, 1)
    ) == (dataset, ["a"], 6)


def test_estimated_blocks_and_cluster_parallelism_are_safe(monkeypatch) -> None:
    import ray

    assert execution._estimated_num_blocks(_Dataset(blocks=0)) == 1
    assert execution._estimated_num_blocks(_Dataset(blocks=None)) is None
    monkeypatch.setattr(ray, "is_initialized", lambda: False)
    assert execution._cluster_parallelism() == 1
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 0})
    assert execution._cluster_parallelism() == 1


@pytest.mark.parametrize(
    ("sql", "class_name"),
    [
        ("COUNT(*)", "Count"),
        ("COUNT(x)", "Count"),
        ("SUM(x)", "Sum"),
        ("MIN(x)", "Min"),
        ("MAX(x)", "Max"),
        ("AVG(x)", "Mean"),
    ],
)
def test_aggregate_function_maps_supported_functions(sql: str, class_name: str) -> None:
    aggregate = parse_one(sql)
    assert isinstance(aggregate, exp.AggFunc)
    input_name = None if isinstance(aggregate.this, exp.Star) else "input"
    result = execution._aggregate_function(aggregate, input_name, "output")
    assert type(result).__name__ == class_name


def test_aggregate_function_validates_missing_and_unsupported_inputs() -> None:
    with pytest.raises(SQLQueryError, match="SUM requires an input expression"):
        execution._aggregate_function(exp.Sum(), None, "output")
    with pytest.raises(SQLQueryError, match="Unsupported Ray-native SQL aggregate MEDIAN"):
        execution._aggregate_function(exp.Median(this=exp.column("x")), "input", "output")


def test_aggregate_planning_covers_group_only_count_and_validation() -> None:
    group_only = _select("SELECT category FROM rows GROUP BY category")
    group_expression = group_only.args["group"].expressions[0]
    computed, aggregators, outputs = execution._build_aggregate_plan(
        group_only.expressions,
        [("_group", group_expression)],
    )
    assert computed == [("_group", group_expression)]
    assert [type(item).__name__ for item in aggregators] == ["Count"]
    assert outputs == [("category", "_group")]

    count = _select("SELECT COUNT(*) AS count FROM rows")
    computed, aggregators, outputs = execution._build_aggregate_plan(count.expressions, [])
    assert computed == []
    assert [type(item).__name__ for item in aggregators] == ["Count"]
    assert outputs == [("count", "_klein_aggregate_0")]

    invalid = _select("SELECT category, SUM(value) FROM rows")
    with pytest.raises(SQLQueryError, match="must appear in GROUP BY"):
        execution._build_aggregate_plan(invalid.expressions, [])


@pytest.mark.parametrize(
    ("fields", "expected"),
    [([], None), ([("a", exp.column("a"))], "a"), ([("a", exp.column("a")), ("b", exp.column("b"))], ["a", "b"])],
)
def test_group_key_shape(fields, expected) -> None:
    assert execution._group_key(fields) == expected


def test_aggregate_select_builds_grouped_dataset_plan(monkeypatch) -> None:
    dataset = _Dataset()
    select = _select("SELECT category, SUM(value) AS total FROM rows GROUP BY category")
    additions: list[object] = []
    monkeypatch.setattr(
        execution,
        "_add_sql_expressions",
        lambda current, expressions, **kwargs: additions.append((expressions, kwargs)) or current,
    )
    monkeypatch.setattr(execution, "_shuffle_partitions", lambda *_args, **_kwargs: 5)

    result = execution._aggregate_select(select, dataset, aliases=["rows"], input_blocks=2, num_cpus=0.2)

    assert result is dataset
    assert len(additions) == 1
    assert [call[0] for call in dataset.calls] == ["groupby", "aggregate", "map"]
    assert dataset.calls[0][1:] == ("_klein_group_0", {"num_partitions": 5})
    assert isinstance(dataset.calls[-1][1], execution._FinalizeAggregate)


def test_aggregate_select_skips_computation_for_count_star(monkeypatch) -> None:
    dataset = _Dataset()
    add = MagicMock()
    monkeypatch.setattr(execution, "_add_sql_expressions", add)
    monkeypatch.setattr(execution, "_shuffle_partitions", lambda *_args, **_kwargs: 1)

    assert (
        execution._aggregate_select(
            _select("SELECT COUNT(*) AS count FROM rows"),
            dataset,
            aliases=["rows"],
            input_blocks=1,
            num_cpus=1,
        )
        is dataset
    )
    add.assert_not_called()


def test_has_aggregates_recognizes_group_and_nested_aggregate() -> None:
    assert execution._has_aggregates(_select("SELECT category FROM rows GROUP BY category"))
    assert execution._has_aggregates(_select("SELECT SUM(value) + 1 FROM rows"))
    assert not execution._has_aggregates(_select("SELECT value + 1 FROM rows"))


def test_add_sql_expressions_combines_native_and_row_fallback(monkeypatch) -> None:
    dataset = _Dataset()
    first, second = _select("SELECT x + 1, y + 2 FROM rows").expressions
    native = object()
    monkeypatch.setattr(
        execution,
        "to_ray_data_expression",
        lambda expression, _aliases: native if expression is first else None,
    )

    result = execution._add_sql_expressions(
        dataset,
        (("first", first), ("second", second)),
        aliases=["rows"],
        num_cpus=0.4,
    )

    assert result is dataset
    assert dataset.calls[0] == ("with_column", "first", native, {"num_cpus": 0.4})
    assert dataset.calls[1][0] == "map"
    assert dataset.calls[1][1]({"x": 2, "y": 3}) == {"x": 2, "y": 3, "second": 5}


def test_add_sql_expressions_skips_map_when_every_expression_is_native(monkeypatch) -> None:
    dataset = _Dataset()
    expression = parse_one("x + 1")
    monkeypatch.setattr(execution, "to_ray_data_expression", lambda *_args: object())
    execution._add_sql_expressions(dataset, (("x", expression),), aliases=[], num_cpus=1)
    assert [call[0] for call in dataset.calls] == ["with_column"]


def test_project_select_plans_stars_expressions_and_duplicate_validation(monkeypatch) -> None:
    dataset = _Dataset()
    select = _select("SELECT *, rows.*, value + 1 AS next FROM rows")
    captured: list[object] = []
    monkeypatch.setattr(
        execution,
        "_add_sql_expressions",
        lambda current, expressions, **kwargs: captured.append((expressions, kwargs)) or current,
    )

    result = execution._project_select(select.expressions, dataset, aliases=["rows"], num_cpus=0.3)

    assert result is dataset
    assert [name for name, _ in captured[0][0]] == ["_klein_projection_2"]
    finalize = dataset.calls[-1][1]
    assert finalize({"rows.id": 1, "_klein_projection_2": 4}) == {"id": 1, "rows.id": 1, "next": 4}

    duplicate = _select("SELECT a.id, b.id FROM a").expressions
    with pytest.raises(SQLQueryError, match="Duplicate SQL output column"):
        execution._project_select(duplicate, dataset, aliases=["a", "b"], num_cpus=1)


def test_order_and_limit_lower_single_and_multiple_keys() -> None:
    single = _Dataset()
    execution._apply_order_and_limit(_select("SELECT x FROM rows ORDER BY x DESC LIMIT 2"), single)
    assert single.calls == [("sort", "x", {"descending": True}), ("limit", 2)]

    multiple = _Dataset()
    execution._apply_order_and_limit(_select("SELECT x, y FROM rows ORDER BY x, y DESC"), multiple)
    assert multiple.calls == [("sort", ["x", "y"], {"descending": [False, True]})]

    unchanged = _Dataset()
    assert execution._apply_order_and_limit(_select("SELECT x FROM rows"), unchanged) is unchanged
    assert unchanged.calls == []


def test_order_by_rejects_non_output_expression() -> None:
    with pytest.raises(SQLQueryError, match="output column names only"):
        execution._apply_order_and_limit(_select("SELECT x FROM rows ORDER BY x + 1"), _Dataset())


def test_limit_rejects_direct_negative_literal() -> None:
    with pytest.raises(SQLQueryError, match="non-negative integer literal"):
        execution._parse_limit_literal(exp.Literal(this="-1", is_string=False))


def test_execute_select_scalar_projection_uses_private_seed(monkeypatch) -> None:
    dataset = _Dataset(blocks=1)
    from_items = MagicMock(return_value=dataset)
    monkeypatch.setattr("ray.data.from_items", from_items)
    project = MagicMock(return_value=dataset)
    order = MagicMock(return_value=dataset)
    monkeypatch.setattr(execution, "_project_select", project)
    monkeypatch.setattr(execution, "_apply_order_and_limit", order)

    result = execution._execute_select(_select("SELECT 1 AS answer"), {}, {}, num_cpus=0.1)

    assert result is dataset
    from_items.assert_called_once_with([{"_klein_scalar_row": True}])
    assert project.call_args.kwargs == {"aliases": [], "num_cpus": 0.1}


def test_execute_select_requires_from_for_join() -> None:
    select = _select("SELECT 1")
    select.set("joins", [exp.Join(this=exp.to_table("b"), kind="CROSS")])
    with pytest.raises(SQLQueryError, match="JOIN requires a FROM relation"):
        execution._execute_select(select, {}, {}, num_cpus=1)


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("SELECT x FROM rows HAVING x > 1", "HAVING is not supported"),
        ("SELECT DISTINCT x FROM rows", "SELECT DISTINCT is not supported"),
    ],
)
def test_execute_select_rejects_unsupported_clauses(monkeypatch, query: str, message: str) -> None:
    monkeypatch.setattr(execution, "_table_relation", lambda *_args, **_kwargs: (_Dataset(), "rows", 1))
    monkeypatch.setattr(execution, "_apply_joins", lambda _s, d, a, b, _c: (d, a, b))
    with pytest.raises(SQLQueryError, match=message):
        execution._execute_select(_select(query), {}, {}, num_cpus=1)


@pytest.mark.parametrize("native", [True, False])
def test_execute_select_routes_native_and_fallback_filters(monkeypatch, native: bool) -> None:
    dataset = _Dataset()
    predicate = object() if native else None
    monkeypatch.setattr(execution, "_table_relation", lambda *_args, **_kwargs: (dataset, "r", 2))
    monkeypatch.setattr(execution, "_apply_joins", lambda _s, d, a, b, _c: (d, a, b or 1))
    monkeypatch.setattr(execution, "to_ray_data_expression", lambda *_args, **_kwargs: predicate)
    monkeypatch.setattr(execution, "_project_select", lambda _p, d, **_kwargs: d)

    execution._execute_select(_select("SELECT x FROM rows AS r WHERE x > 1"), {}, {}, num_cpus=0.5)

    call = dataset.calls[0]
    assert call[0] == "filter"
    if native:
        assert call[1] is None
        assert call[2] == {"expr": predicate, "num_cpus": 0.5}
    else:
        assert isinstance(call[1], execution._FilterRow)
        assert call[2] == {"num_cpus": 0.5}


def test_execute_select_routes_aggregate_plan(monkeypatch) -> None:
    dataset = _Dataset()
    monkeypatch.setattr(execution, "_table_relation", lambda *_args, **_kwargs: (dataset, "rows", 2))
    monkeypatch.setattr(execution, "_apply_joins", lambda _s, d, a, b, _c: (d, a, b or 1))
    aggregate = MagicMock(return_value=dataset)
    monkeypatch.setattr(execution, "_aggregate_select", aggregate)

    assert execution._execute_select(_select("SELECT COUNT(*) FROM rows"), {}, {}, num_cpus=0.2) is dataset
    assert aggregate.call_args.kwargs == {"aliases": ["rows"], "input_blocks": 2, "num_cpus": 0.2}


def test_query_ast_executes_ordered_ctes_and_strips_with(monkeypatch) -> None:
    statement = parse_one("WITH first AS (SELECT * FROM source), second AS (SELECT * FROM first) SELECT * FROM second")
    seen: list[tuple[str, tuple[str, ...]]] = []

    def fake_select(select, _datasets, ctes, **_kwargs):
        source = select.args.get("from_")
        name = source.this.name if source is not None else "scalar"
        seen.append((name, tuple(ctes)))
        return _Dataset()

    monkeypatch.setattr(execution, "_execute_select", fake_select)
    result = execution._execute_query_ast(statement, {"source": _Dataset()}, {}, num_cpus=1)

    assert isinstance(result, _Dataset)
    assert seen == [("source", ()), ("first", ("first",)), ("second", ("first", "second"))]


def test_query_ast_rejects_recursive_cte() -> None:
    statement = parse_one("WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x")
    with pytest.raises(SQLQueryError, match="Recursive CTEs are not supported"):
        execution._execute_query_ast(statement, {}, {}, num_cpus=1)


def test_query_ast_lowers_union_all_and_rejects_union_distinct(monkeypatch) -> None:
    left, right = _Dataset(), _Dataset()
    results = iter((left, right))
    monkeypatch.setattr(execution, "_execute_select", lambda *_args, **_kwargs: next(results))
    assert execution._execute_query_ast(parse_one("SELECT 1 UNION ALL SELECT 2"), {}, {}, num_cpus=1) is left
    assert left.calls == [("union", right)]

    with pytest.raises(SQLQueryError, match="UNION DISTINCT is not supported"):
        execution._execute_query_ast(parse_one("SELECT 1 UNION SELECT 2"), {}, {}, num_cpus=1)


def test_query_ast_rejects_unsupported_query_form() -> None:
    with pytest.raises(SQLQueryError, match="Unsupported SQL query form"):
        execution._execute_query_ast(
            exp.Intersect(this=_select("SELECT 1"), expression=_select("SELECT 1")), {}, {}, num_cpus=1
        )


def test_execute_sql_query_validates_dataset_arity_and_forwards_bindings(monkeypatch) -> None:
    with pytest.raises(ValueError, match="2 table names but 1 input datasets"):
        execution.execute_sql_query("SELECT * FROM a", ("a", "b"), (_Dataset(),), num_cpus=1)

    statement = _select("SELECT * FROM a")
    monkeypatch.setattr("ray.klein._internal.sql.validation.validate_read_query", lambda query: statement)
    execute = MagicMock(return_value=_Dataset())
    monkeypatch.setattr(execution, "_execute_query_ast", execute)
    first, second = _Dataset(), _Dataset()

    result = execution.execute_sql_query("ignored", ("a", "b"), (first, second), num_cpus=0.75)

    assert isinstance(result, _Dataset)
    assert execute.call_args.args == (statement, {"a": first, "b": second}, {})
    assert execute.call_args.kwargs == {"num_cpus": 0.75}


def test_sql_source_and_transform_forward_dataset_sequences(monkeypatch) -> None:
    execute = MagicMock(return_value=_Dataset())
    monkeypatch.setattr(execution, "execute_sql_query", execute)
    primary, other = _Dataset(), _Dataset()

    execution.sql_source("SELECT 1", num_cpus=0.1)
    execute.assert_called_once_with("SELECT 1", (), (), num_cpus=0.1)
    execute.reset_mock()
    execution.sql_transform(primary, "SELECT * FROM a", ("a", "b"), other, num_cpus=0.2)
    execute.assert_called_once_with(
        "SELECT * FROM a",
        ("a", "b"),
        (primary, other),
        num_cpus=0.2,
    )
