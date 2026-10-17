<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL getting started

After [installing Klein](../installation.md), use a Python
`Pipeline` to provide input tables and run SQL. SQL returns a `DataStream`, so
you can collect its rows or continue with [DataStream API](../datastream.md)
transformations. The example below includes its input data and can run as a
Python script.

## Choose a SQL entry point

| Entry point | Use it for | Catalog lifetime |
|---|---|---|
| `pipeline.sql(query, tables=...)` | A query with explicit or caller-scope bindings. | The explicit pipeline session |
| `stream.sql(query)` | A query rooted at one stream, bound as `self` by default. | The stream context's persistent session |
| `pipeline.execute_sql(statement)` | `SELECT`, Table DDL, and `INSERT INTO`. | The explicit pipeline session |

Use `pipeline.execute_sql()` for a catalog workflow. The module-level `sql()`,
`execute_sql()`, and registration functions remain available on the implicit
deferred pipeline. A top-level `sql()` call creates a fresh query session, so
it does not see tables previously created through module-level `execute_sql()`;
it does inherit functions registered on the current context.

## Query DataStreams from Python

When `tables` is omitted, `pipeline.sql()` discovers named `DataStream`
variables in the caller's Python scope:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="customer-totals")
orders = pipeline.from_items([{"customer_id": 1, "amount": 10}])
customers = pipeline.from_items([{"customer_id": 1, "name": "Ada"}])

result = pipeline.sql("""
    SELECT c.name, SUM(o.amount) AS total
    FROM orders AS o
    JOIN customers AS c USING (customer_id)
    GROUP BY c.name
""")
rows = result.collect().result()
print(rows)
```

The query returns:

```text
[{'name': 'Ada', 'total': 10}]
```

Explicit bindings are preferable in library code:

```python
result = pipeline.sql(
    "SELECT * FROM orders WHERE amount >= 10",
    tables={"orders": orders},
)
```

For reusable catalog state, `pipeline.execute_sql()` keeps temporary tables in
the explicit pipeline session. A stream can use the conventional `self`
relation:

```python
filtered = orders.sql("SELECT * FROM self WHERE amount > 10")
```

For connector-backed tables and `INSERT INTO`, continue with
[Table DDL and connectors](connectors.md). For an unbounded input or updating
results, read [Streaming SQL](streaming.md). The
[execution and support matrix](execution.md) lists supported query forms.
