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

## Choose a SQL entry point

| Entry point | Use it for | Catalog lifetime |
|---|---|---|
| `ray.klein.sql(query, tables=...)` | A one-off query with explicit or caller-scope bindings. | One query |
| `stream.sql(query)` | A query rooted at one stream, bound as `self` by default. | The stream context's persistent session |
| `ray.klein.execute_sql(statement)` | `SELECT`, Table DDL, and `INSERT INTO`. | The current pipeline's persistent session |

Use `ray.klein.execute_sql()` for a catalog workflow. The top-level
`ray.klein.sql()` creates a fresh session for its one query, so it does not see
tables previously created through `execute_sql()`.

## Query DataStreams from Python

The top-level `sql` function discovers named `DataStream` variables in
the caller's Python scope:

```python
import ray
import ray.klein

orders = ray.klein.from_items([{"customer_id": 1, "amount": 10}])
customers = ray.klein.from_items([{"customer_id": 1, "name": "Ada"}])

result = ray.klein.sql("""
    SELECT c.name, SUM(o.amount) AS total
    FROM orders AS o
    JOIN customers AS c USING (customer_id)
    GROUP BY c.name
""")
result.data.take_all()
rows = ray.klein.execute("customer-totals").get()
print(rows)
```

Explicit bindings are preferable in library code:

```python
result = ray.klein.sql(
    "SELECT * FROM orders WHERE amount >= 10",
    tables={"orders": orders},
)
```

For reusable catalog state, `ray.klein.execute_sql()` keeps temporary tables in
the current pipeline session. A stream can use the conventional `self`
relation:

```python
filtered = orders.sql("SELECT * FROM self WHERE amount > 10")
```

## Run a continuous query

Streaming SQL follows Flink's [dynamic table](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/)
model. An ordinary mapping is an `INSERT` row. Updating queries emit
`ChangelogRow` values whose `row_kind` is `+I`, `-U`, `+U`, or `-D`.

```python
import ray
import ray.klein

ray.klein.configure("execution.runtime.mode=streaming")
orders = ray.klein.from_items([
    {"name": "Ada", "amount": 10},
    {"name": "Ada", "amount": 15},
])

changes = ray.klein.sql("""
    SELECT name, SUM(amount) AS total
    FROM orders
    GROUP BY name
""", tables={"orders": orders})

changes.take_all()
for row in ray.klein.execute("streaming-sql").get():
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
planned as a continuously maintained Top-N table and emits insert/delete
changes when rows enter or leave the result.

Regular joins, non-windowed aggregates, and Top-N are stateful. Without a TTL,
their state can grow for the lifetime of an unbounded input. A global Top-N is
also a single keyed partition, so increasing unrelated operator parallelism
does not remove that bottleneck.

## Define connectors with Flink-style Table DDL

Catalog tables follow Flink Table DDL: the schema is logical metadata and the
`WITH` map selects and configures a connector factory. Creating a table does
not open files, create Kafka consumers, or launch Ray tasks.

```python
ray.klein.execute_sql("""
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

ray.klein.execute_sql("""
    CREATE TABLE output_rows (
        event_id BIGINT,
        payload STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/tmp/output',
        'format' = 'parquet'
    )
""")

ray.klein.execute_sql("""
    INSERT INTO output_rows
    SELECT event_id, payload FROM input_events
""")
ray.klein.execute("table-insert").wait()
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
Python methods reuse Ray Data 2.56 names: `topics`, `bootstrap_servers`,
`trigger`, `start_offset`, `end_offset`,
`consumer_config`, resource options, and `timeout_ms` for reads; `topic`,
`key_field`, serializers, `producer_config`, `ray_remote_args`, and
`concurrency` for writes. Complex option values use JSON strings.

Message encodings remain formats owned by the physical connector. For example,
Canal CDC uses `'connector'='kafka'`, `'format'='canal-json'`, with
`canal-json.*` format options; it is not registered as a separate connector.

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

The two planners intentionally have different feature sets:

| Query form | Batch | Streaming |
|---|---|---|
| Projection, supported scalar expressions, and `WHERE` | Yes | Yes |
| Inner equality join | Yes | Yes |
| Left, right, or full outer equality join | Yes | No |
| `CROSS JOIN` | Yes | No |
| `GROUP BY` with `COUNT`, `SUM`, `MIN`, `MAX`, or `AVG` | Yes | Yes |
| `ORDER BY` | Output columns | Only with `LIMIT` as Top-N |
| `LIMIT` without `ORDER BY` | Yes | No |
| Non-recursive CTE | Yes | No |
| `UNION ALL` | Yes | No |
| `HAVING`, `SELECT DISTINCT`, or non-equality join | No | No |
| SQL window syntax and time-attribute DDL | No | No |

Join `ON` conditions are conjunctions of equality predicates between qualified
left and right columns; `USING` is also supported. Unsupported forms raise
`SQLQueryError` instead of silently changing semantics. All inputs must belong
to the same Klein pipeline.

Bounded SQL translates compatible SQLGlot nodes to native Ray 2.56 expression
ASTs before falling back to Klein's row evaluator. This covers columns,
literals, arithmetic/comparison/boolean/null predicates, casts, lower/upper,
common numeric functions, `RANDOM([seed])`, `UUID()`, and
`MONOTONICALLY_INCREASING_ID()`. The I/O form
`DOWNLOAD(uri_column)` is supported as a standalone projection or aggregate
input, for example:

```sql
SELECT id, DOWNLOAD(uri) AS body
FROM files
WHERE status = 'ready'
```

In batch mode, `DOWNLOAD` lowers to Ray Data's dedicated URI download operator.
In streaming mode, Klein uses a bounded, order-preserving asynchronous operator
so downloads do not block the task actor and a full in-flight window propagates
backpressure. Composing `DOWNLOAD` inside another scalar expression or using it
as a predicate is rejected. `RANDOM([seed])`, `UUID()`, and
`MONOTONICALLY_INCREASING_ID()` are task-local streaming expressions and work in
projections, predicates, grouping, and aggregate inputs where their SQL types
are valid.

Data definition language (DDL) and data manipulation language (DML) support `CREATE [TEMPORARY] TABLE`, `DROP TABLE`, and
`INSERT INTO ... SELECT`. Catalog-qualified names, computed columns,
watermarks, partitions, and `INSERT` target-column lists are reserved for
later planner iterations.

## Distinguish queries from the database sink

`DataStream.write_sql()` writes rows through a Python DB-API connection; it is
not a `connector='sql'` Table factory. In batch mode it delegates to Ray Data.
In streaming mode each sink subtask owns a connection and flushes buffered
`executemany` calls when 128 rows accumulate, at checkpoints, and at close. Its
delivery guarantee is at-least-once, so use idempotent statements, a unique
key, or a database-native upsert. See
[Delivery semantics](delivery-semantics.md) before using it in a recoverable
pipeline.
