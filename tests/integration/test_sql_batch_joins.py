# SPDX-License-Identifier: Apache-2.0
"""Batch join results must be independent of NULL placement and block layout."""

import random
import sqlite3
from collections import Counter

import pyarrow as pa
import pytest

from ray.klein.api.klein_context import KleinContext

# A large development host can hide overlapping hash-shuffle reservations.
# Advertise a bounded heap budget so chained queries also exercise small nodes.
pytestmark = pytest.mark.parametrize(
    "ray_cluster",
    [{"_memory": 4 * 1024**3, "object_store_memory": 512 * 1024**2}],
    indirect=True,
    scope="module",
    ids=["4GiB"],
)


def _oracle(query, tables):
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        for name, (columns, rows) in tables.items():
            connection.execute(f"CREATE TABLE {name} ({', '.join(columns)})")
            connection.executemany(
                f"INSERT INTO {name} VALUES ({', '.join('?' for _ in columns)})",
                [tuple(row[column] for column in columns) for row in rows],
            )
        return [dict(row) for row in connection.execute(query)]


def _rows(rows):
    return Counter(tuple(sorted(row.items())) for row in rows)


def _execute(context, query, tables):
    sink = context.sql(query, tables=tables).take_all()
    return context.execute("sql-batch-join-regression", sinks=(sink,)).get()


def _source(context, rows, layout):
    if layout == "typed-arrow":
        return context.data.from_arrow(
            pa.Table.from_pylist(rows, schema=pa.schema([("id", pa.int64()), ("key", pa.int64())]))
        )
    return context.data.from_items(rows, override_num_blocks=1 if layout == "single-block" else 4)


@pytest.mark.parametrize("layout", ["inferred", "single-block", "typed-arrow"])
def test_nullable_integer_join_does_not_drop_matches(layout):
    context = KleinContext({"execution.runtime.mode": "batch"})
    left = [{"id": i, "key": None if i % 11 == 0 else i % 3} for i in range(120)]
    right = [{"id": i, "key": i} for i in range(3)]
    actual = _execute(
        context,
        "SELECT l.id, r.id AS matched FROM left_rows l JOIN right_rows r ON l.key = r.key",
        {"left_rows": _source(context, left, layout), "right_rows": _source(context, right, "inferred")},
    )
    assert _rows(actual) == _rows([{"id": row["id"], "matched": row["key"]} for row in left if row["key"] is not None])


@pytest.mark.parametrize("join", ["INNER", "LEFT", "RIGHT", "FULL"])
@pytest.mark.parametrize("layout", ["inferred", "single-block", "typed-arrow"])
def test_nullable_join_preserves_both_key_columns_and_multiplicity(join, layout):
    left = [{"id": i, "key": key} for i, key in enumerate([None, None, 1, 2, 2, 3, 0])]
    right = [{"id": i, "key": key} for i, key in enumerate([2, None, 0, 4, 2, None])]
    query = f"""SELECT l.id AS lid, r.id AS rid, l.key AS lkey, r.key AS rkey
        FROM left_rows l {join} JOIN right_rows r ON l.key = r.key"""
    expected = _oracle(query, {"left_rows": (("id", "key"), left), "right_rows": (("id", "key"), right)})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(
        context, query, {"left_rows": _source(context, left, layout), "right_rows": _source(context, right, layout)}
    )
    assert _rows(actual) == _rows(expected)


@pytest.mark.parametrize("join", ["INNER", "LEFT", "RIGHT", "FULL"])
@pytest.mark.parametrize("missing", ["left-null", "both-null", "left-empty", "right-empty"])
def test_join_handles_all_null_and_empty_inputs(join, missing):
    left = [{"id": 0, "key": None}, {"id": 1, "key": None}]
    right = [{"id": 2, "key": None}, {"id": 3, "key": 1}]
    if missing == "both-null":
        right[1]["key"] = None
    elif missing == "left-empty":
        left = []
    elif missing == "right-empty":
        right = []
    query = f"""SELECT l.id AS lid, r.id AS rid, l.key AS lkey, r.key AS rkey
        FROM left_rows l {join} JOIN right_rows r ON l.key = r.key"""
    expected = _oracle(query, {"left_rows": (("id", "key"), left), "right_rows": (("id", "key"), right)})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(
        context,
        query,
        {"left_rows": _source(context, left, "typed-arrow"), "right_rows": _source(context, right, "typed-arrow")},
    )
    assert _rows(actual) == _rows(expected)


@pytest.mark.parametrize("join", ["INNER", "LEFT", "RIGHT", "FULL"])
def test_composite_join_keys_match_sqlite(join):
    left = [
        {"id": i, "key": key, "tag": tag}
        for i, (key, tag) in enumerate([(None, "x"), (1, None), (1, "x"), (2, "y"), (1, "x")])
    ]
    right = [
        {"id": i, "key": key, "tag": tag}
        for i, (key, tag) in enumerate([(None, "x"), (1, None), (1, "x"), (2, "x"), (1, "x")])
    ]
    query = f"""SELECT l.id AS lid, r.id AS rid, l.key AS lkey, r.key AS rkey
        FROM left_rows l {join} JOIN right_rows r ON l.key = r.key AND l.tag = r.tag"""
    expected = _oracle(query, {"left_rows": (("id", "key", "tag"), left), "right_rows": (("id", "key", "tag"), right)})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(context, query, {"left_rows": context.from_items(left), "right_rows": context.from_items(right)})
    assert _rows(actual) == _rows(expected)


@pytest.mark.parametrize("seed", [19, 73, 431])
@pytest.mark.parametrize("three_way", [False, True])
def test_join_aggregate_topn_matches_sqlite(seed, three_way):
    rng = random.Random(seed)
    orders = [
        {
            "id": i,
            "cid": None if i % 11 == 0 else rng.randrange(15),
            "amount": None if i % 7 == 0 else rng.randrange(-20, 101),
        }
        for i in range(120)
    ]
    customers = [{"cid": i, "region": "r" + str(i % 5), "tier": i % 3} for i in range(15)]
    discounts = [{"tier": i, "rate": i + 1} for i in range(3)]
    query = """SELECT c.region, d.rate, COUNT(*) AS n, SUM(o.amount * d.rate) AS adjusted
        FROM orders o JOIN customers c ON o.cid = c.cid
        JOIN discounts d ON c.tier = d.tier
        WHERE o.amount IS NOT NULL GROUP BY c.region, d.rate
        ORDER BY adjusted DESC, c.region ASC, d.rate ASC LIMIT 4"""
    if not three_way:
        query = """SELECT c.region, COUNT(*) AS n, COUNT(o.amount) AS nonnull,
            SUM(COALESCE(o.amount, 0)) AS total, AVG(o.amount) AS mean,
            MIN(o.amount) AS lo, MAX(o.amount) AS hi
            FROM orders o JOIN customers c ON o.cid = c.cid
            WHERE o.id % 3 <> 0 GROUP BY c.region ORDER BY total DESC, c.region ASC LIMIT 3"""
    tables = {"orders": orders, "customers": customers, "discounts": discounts}
    expected = _oracle(query, {name: (tuple(rows[0]), rows) for name, rows in tables.items()})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(context, query, {name: context.from_items(rows) for name, rows in tables.items()})
    assert _rows(actual) == _rows(expected)


def test_cte_union_join_aggregate_matches_sqlite():
    orders = [{"id": i, "cid": None if i % 5 == 0 else i % 3, "amount": i - 10} for i in range(40)]
    customers = [{"cid": i, "region": f"r{i}"} for i in range(3)]
    query = """WITH filtered AS (SELECT cid, amount FROM orders WHERE amount >= 0),
        doubled AS (SELECT cid, amount FROM filtered UNION ALL SELECT cid, amount FROM filtered)
        SELECT c.region, COUNT(*) AS n, SUM(d.amount) AS total
        FROM doubled d JOIN customers c ON d.cid = c.cid GROUP BY c.region
        ORDER BY total DESC, c.region ASC LIMIT 3"""
    tables = {"orders": orders, "customers": customers}
    expected = _oracle(query, {name: (tuple(rows[0]), rows) for name, rows in tables.items()})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(context, query, {name: context.from_items(rows) for name, rows in tables.items()})
    assert _rows(actual) == _rows(expected)


@pytest.mark.parametrize("join", ["INNER", "LEFT", "RIGHT", "FULL"])
@pytest.mark.parametrize("projection", ["key, l.id AS lid, r.id AS rid", "l.*, r.key AS rkey"])
def test_using_join_keeps_unqualified_and_qualified_keys(join, projection):
    left = [{"id": 1, "key": 1}, {"id": 2, "key": None}]
    right = [{"id": 3, "key": 1}, {"id": 4, "key": 2}, {"id": 5, "key": None}]
    query = f"SELECT {projection} FROM left_rows l {join} JOIN right_rows r USING (key)"
    # SQLite 3.40 expands l.* with the merged USING key for RIGHT/FULL joins;
    # explicit l.key consistently refers to the original nullable left column.
    oracle_query = query.replace("l.*", "l.id, l.key")
    expected = _oracle(oracle_query, {"left_rows": (("id", "key"), left), "right_rows": (("id", "key"), right)})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(
        context,
        query,
        {"left_rows": _source(context, left, "inferred"), "right_rows": _source(context, right, "inferred")},
    )
    assert _rows(actual) == _rows(expected)


def test_using_star_and_chained_full_join_coalesce_the_key():
    tables = {
        "a": [{"key": 1, "av": "a1"}],
        "b": [{"key": 2, "bv": "b2"}],
        "c": [{"key": 2, "cv": "c2"}, {"key": 3, "cv": "c3"}],
    }
    query = "SELECT * FROM a FULL JOIN b USING (key) FULL JOIN c USING (key)"
    expected = _oracle(query, {name: (tuple(rows[0]), rows) for name, rows in tables.items()})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(context, query, {name: context.from_items(rows) for name, rows in tables.items()})
    assert _rows(actual) == _rows(expected)


@pytest.mark.parametrize("right_float", [False, True])
def test_nullable_large_integer_keys_do_not_round_or_depend_on_numeric_representation(right_float):
    left = [{"id": i, "key": key} for i, key in enumerate([None, 1, 2**53, 2**53 + 1, 2**53 + 2])]
    right = [
        {"id": i, "key": float(key) if right_float else key} for i, key in enumerate([1, 2**53, 2**53 + 1, 2**53 + 2])
    ]
    query = "SELECT l.id AS lid, r.id AS rid, l.key AS lkey FROM left_rows l JOIN right_rows r ON l.key = r.key"
    expected = _oracle(query, {"left_rows": (("id", "key"), left), "right_rows": (("id", "key"), right)})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(context, query, {"left_rows": context.from_items(left), "right_rows": context.from_items(right)})
    assert _rows(actual) == _rows(expected)


def test_using_chain_preserves_empty_intermediate_schema():
    context = KleinContext({"execution.runtime.mode": "batch"})
    tables = {name: context.from_items([{"key": index}]) for index, name in enumerate(("a", "b", "c"))}
    assert _execute(context, "SELECT key FROM a JOIN b USING (key) JOIN c USING (key)", tables) == []


@pytest.mark.parametrize(
    "query",
    [
        """WITH totals AS (SELECT key, SUM(v) AS total FROM a GROUP BY key)
            SELECT t.key, t.total, b.label FROM totals t JOIN b ON t.key = b.key ORDER BY t.key""",
        """WITH matched AS (SELECT a.key, a.v FROM a JOIN b ON a.key = b.key)
            SELECT key, SUM(v) AS total FROM matched GROUP BY key ORDER BY key""",
        """SELECT a.key, a.v FROM a JOIN b ON a.key = b.key
            UNION ALL SELECT a.key, a.v FROM a JOIN c ON a.key = c.key""",
        """WITH matched AS (SELECT a.key, a.v FROM a JOIN b ON a.key = b.key)
            SELECT a.key, COUNT(*) AS n FROM a JOIN matched m ON a.key = m.key GROUP BY a.key""",
    ],
    ids=["aggregate-cte", "join-cte", "joined-union-branches", "join-cte-and-aggregate"],
)
def test_shuffled_subqueries_complete_with_limited_memory(query):
    tables = {
        "a": [{"key": None if i % 4 == 0 else i % 3, "v": i} for i in range(12)],
        "b": [{"key": key, "label": str(key)} for key in (1, 2, 4)],
        "c": [{"key": key} for key in (0, 1, 4)],
    }
    expected = _oracle(query, {name: (tuple(rows[0]), rows) for name, rows in tables.items()})
    context = KleinContext({"execution.runtime.mode": "batch"})
    actual = _execute(context, query, {name: context.from_items(rows) for name, rows in tables.items()})
    assert _rows(actual) == _rows(expected)
