<!-- SPDX-License-Identifier: Apache-2.0 -->

# Streaming SQL

Use streaming SQL for continuous queries and changing results. The example
below uses finite input to demonstrate the changelog; a continuous connector
provides a long-running source. See [Table DDL and connectors](connectors.md)
for Kafka continuous tables and [SQL execution and support](execution.md)
for the batch and streaming feature matrix.

## Run a continuous query

Streaming SQL follows Flink's [dynamic table](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/)
model. An ordinary mapping is an `INSERT` row. Updating queries emit
`ChangelogRow` values whose `row_kind` is `+I`, `-U`, `+U`, or `-D`.

```python
import ray.klein as klein

pipeline = klein.pipeline(
    {"execution.runtime.mode": "streaming"},
    name="streaming-sql",
)
orders = pipeline.from_items(
    [
        {"name": "Ada", "amount": 10},
        {"name": "Ada", "amount": 15},
    ]
)

changes = pipeline.sql(
    """
    SELECT name, SUM(amount) AS total
    FROM orders
    GROUP BY name
""",
    tables={"orders": orders},
)

for row in changes.collect().result():
    print(row.row_kind.value, dict(row))
```

The materialized result after every change is equivalent to the same SQL query
over the current input snapshot. The example emits `+I` for `Ada = 10`, then
`-U` for the old row and `+U` for `Ada = 25`. Append-only filters and
projections preserve the incoming row kind. Regular equality joins keep both
inputs in checkpointed state and emit insert/delete changes for matching rows.

Use a `STATE_TTL` hint to bound idle regular-join or aggregation state, with the
same warning as Flink: expiring state can make later results incomplete.

```sql
SELECT /*+ STATE_TTL('o'='1h', 'c'='6h') */
       c.name, SUM(o.amount) AS total
FROM orders AS o
JOIN customers AS c USING (customer_id)
GROUP BY c.name
```

Set a pipeline default with `table.exec.state.ttl=1h` or
`RAY_KLEIN_TABLE_EXEC_STATE_TTL=1h`. A hint takes precedence for its table
alias. State snapshots use the configured Klein backend and the Ray Object
Store checkpoint cache, so recovery and key-group rescaling use the same path
as native stateful operators.

Flink does not allow an arbitrary global sort over an unbounded table. Klein
therefore rejects streaming `ORDER BY` unless it is paired with `LIMIT`;
time-attribute ordering is not implemented yet. `ORDER BY ... LIMIT n` is
maintained as a Top-N table and emits insert/delete changes when rows enter or
leave the result.

Regular joins, non-windowed aggregates, and Top-N are stateful, but their state
shape depends on the input changelog. For insert-only input, `COUNT`, `SUM`, and
`AVG` keep constant-size accumulators per group, while global Top-N retains at
most `n` sorted rows. `MIN` and `MAX` retain value multiplicities, and a
retraction-capable aggregate or Top-N retains row multiplicities or candidate
rows so a later delete can expose the correct result. Those forms and regular
joins can grow for the lifetime of an unbounded input unless TTL is configured.
A global Top-N is still a single keyed partition, so increasing unrelated
operator parallelism does not remove that bottleneck.

The incremental aggregate and Top-N values are versioned. When the operator
first processes a record after restoring the earlier list-backed value, it
migrates that value in place to the compact representation. This is a narrow
state-value migration, not a general pre-1.0 checkpoint compatibility promise;
rehearse the exact graph and checkpoint before an upgrade.
