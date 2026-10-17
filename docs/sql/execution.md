<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL execution and support

Batch and streaming SQL share a parser but support different query forms.
Check the matrix below before choosing an execution mode. For examples, start
with [SQL getting started](getting-started.md) or
[Streaming SQL](streaming.md).

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
| Projection, supported scalar expressions, registered scalar UDFs, and `WHERE` | Yes | Yes |
| Top-level `AI_GENERATE` and `AI_EMBED` projections | Synchronous backend | Insert-only input; synchronous or asynchronous backend |
| Built-in image and PDF projections | Yes | Insert-only input |
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

In batch mode, `DOWNLOAD` runs in a one-row bounded download batch. Multiple
downloads in one batch row or streaming SQL projection share one byte budget.
In streaming mode, Klein uses a bounded, order-preserving asynchronous operator
so downloads do not block the task actor and a full in-flight window propagates
backpressure. HTTP(S)
destinations are checked before every request and redirect. By default,
credentials, unknown schemes, private/non-global resolved addresses, HTTPS
downgrades, excessive redirects, responses over 64 MiB, and requests past the
30-second I/O budget are rejected as SQL `NULL`.

Use the `sql.download.*` configuration group to narrow schemes and hosts,
authorize explicit IP ranges, or adjust byte and time limits. A hostname
allowlist does not override private-address blocking. Local development paths
continue to work through plain paths, `file://`, and `local://`. The synchronous
system DNS resolver cannot be interrupted by the HTTP timeout, and object-store
timeouts remain controlled by their filesystem implementation.

Composing `DOWNLOAD` inside another scalar expression is rejected except when
it is the direct data argument of a media function; using it as a predicate is
also rejected. `RANDOM([seed])`, `UUID()`, and
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
[Delivery semantics](../delivery-semantics.md) before using it in a recoverable
pipeline.
