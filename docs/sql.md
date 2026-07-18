---
myst:
  html_meta:
    description: "Run batch and Flink-style continuous SQL over Klein for Ray DataStreams and define connectors with Table DDL."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray-native SQL and Table connectors

Klein SQL uses one SQLGlot AST with two Ray-native execution backends. Bounded
batch queries lower to lazy Ray Dataset operations. Continuous queries lower
to Klein operators with managed keyed state, checkpoints, key groups, and
changelog output. SQLGlot is the parser, not an execution engine, and Klein
does not embed DuckDB.

## Query DataStreams from Python

The top-level `sql` function discovers named `DataStream` variables in
the caller's Python scope:

```python
import ray

ray.klein.reset_context().enable_interactive_mode()

orders = ray.klein.from_items([{"customer_id": 1, "amount": 10}])
customers = ray.klein.from_items([{"customer_id": 1, "name": "Ada"}])

result = ray.klein.sql("""
    SELECT c.name, SUM(o.amount) AS total
    FROM orders AS o
    JOIN customers AS c USING (customer_id)
    GROUP BY c.name
""")
print(result.data.take_all())
```

Explicit bindings are preferable in library code:

```python
result = ctx.sql(
    "SELECT * FROM orders WHERE amount >= 10",
    tables={"orders": orders},
)
```

`SQLSession` keeps named views without materializing them, and a stream can use
the conventional `self` relation:

```python
ctx.sql_session.create_temp_view("orders", orders)
counted = ctx.sql_session.sql("SELECT COUNT(*) AS count FROM orders")
filtered = orders.sql("SELECT * FROM self WHERE amount > 10")
```

## Run a continuous query

Streaming SQL follows Flink's [dynamic table](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/)
model. An ordinary mapping is an `INSERT` row. Updating queries emit
`ChangelogRow` values whose `row_kind` is `+I`, `-U`, `+U`, or `-D`.

```python
from ray.klein import Configuration, KleinContext

ctx = KleinContext(Configuration("execution.runtime.mode=streaming"))
orders = ctx.from_items([
    {"name": "Ada", "amount": 10},
    {"name": "Ada", "amount": 15},
])

changes = ctx.sql("""
    SELECT name, SUM(amount) AS total
    FROM orders
    GROUP BY name
""", tables={"orders": orders})

ctx.enable_interactive_mode()
for row in changes.take_all():
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
therefore rejects streaming `ORDER BY` without an ascending time attribute.
`ORDER BY ... LIMIT n` is planned as a continuously maintained Top-N table and
emits insert/delete changes when rows enter or leave the result.

## Define connectors with Flink-style Table DDL

Catalog tables follow Flink Table DDL: the schema is logical metadata and the
`WITH` map selects and configures a connector factory. Creating a table does
not open files, create Kafka consumers, or launch Ray tasks.

```python
ctx.execute_sql("""
    CREATE TEMPORARY TABLE input_events (
        event_id BIGINT NOT NULL,
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topics' = 'events',
        'bootstrap_servers' = 'localhost:9092',
        'start_offset' = 'earliest',
        'end_offset' = 'latest',
        'override_num_blocks' = '4'
    )
""")

ctx.execute_sql("""
    CREATE TABLE output_rows (
        event_id BIGINT,
        payload STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/tmp/output',
        'format' = 'parquet'
    )
""")

sink = ctx.execute_sql("""
    INSERT INTO output_rows
    SELECT event_id, payload FROM input_events
""")
ctx.execute("table-insert").wait()
```

In streaming mode, the filesystem sink is checkpoint-transactional. Part files
remain below a hidden `.klein-staging` path until their committable is present
in durable checkpoint metadata, then the coordinator publishes them
idempotently. Flink-style options such as `sink.parallelism`,
`sink.rolling-policy.file-size`, `sink.rolling-policy.rollover-interval`, and
`sink.rolling-policy.inactivity-interval` configure the native sink. See the
[Filesystem connector](connectors/filesystem.md) for the full lifecycle and
option table.

Built-in connector identifiers are `filesystem`, `kafka`, and `print`.
The dedicated [connector catalog](connectors/index.md) lists their complete
option sets, defaults, data shapes, and delivery guarantees.
Applications can implement `TableFactory` and register it with
`SQLSession.register_table_factory()`. A factory validates table options at
`CREATE TABLE` time and binds a source or sink only when the table is read or
used by `INSERT INTO`.
Third-party packages can publish factories through the
`ray.klein.table_factories` Python entry-point group.

Kafka is deliberately not a second Klein-specific API. Its table options and
Python methods reuse Ray Data 2.50+ names: `topics`, `bootstrap_servers`,
`trigger`, `start_offset`, `end_offset`,
`consumer_config`, resource options, and `timeout_ms` for reads; `topic`,
`key_field`, serializers, `producer_config`, `ray_remote_args`, and
`concurrency` for writes. Complex option values use JSON strings.

The connector also validates the read-side `concurrency`,
`partition_discovery_interval_ms`, and `max_batch_size` options used by the
Python continuous source. A Kafka table with `'trigger' = 'continuous'`
selects the streaming SQL planner automatically.

## How does SQL execute?

The batch lowering boundary is:

```text
SQL text -> SQLGlot AST -> Klein analysis -> Ray Dataset DAG -> Ray execution
```

Projection and predicates become Ray row transforms; equi-joins use
`Dataset.join`; grouping uses `GroupedData.aggregate`; ordering and limits use
their native Dataset operators. Data stays partitioned in Ray's Object Store,
so there is no single-node SQL task or driver-side `take_all()` boundary.

The streaming lowering boundary is:

```text
SQL text -> SQLGlot AST -> Klein continuous plan -> keyed Ray actors
         -> managed state/checkpoints -> changelog stream
```

The planner supports `SELECT`, common table expressions (CTEs), scalar expressions, `WHERE`,
equi-joins and cross joins, `GROUP BY` with `COUNT`, `SUM`, `MIN`, `MAX`, and
`AVG`, `ORDER BY` output columns, `LIMIT`, and `UNION ALL`. It intentionally
rejects unsupported forms such as recursive CTEs, non-equality joins, `HAVING`,
and `SELECT DISTINCT` instead of silently changing semantics. Streaming mode
currently supports a single `SELECT` query block, projections, predicates,
regular inner equality joins, grouped `COUNT`/`SUM`/`MIN`/`MAX`/`AVG`, and
Top-N. CTEs, `UNION ALL`, outer joins, time-attribute ordering, and SQL window
syntax continue to use batch mode or fail during planning. All inputs must
belong to the same `KleinContext`.

Data definition language (DDL) and data manipulation language (DML) support `CREATE [TEMPORARY] TABLE`, `DROP TABLE`, and
`INSERT INTO ... SELECT`. Catalog-qualified names, computed columns,
watermarks, partitions, and `INSERT` target-column lists are reserved for
later planner iterations.
