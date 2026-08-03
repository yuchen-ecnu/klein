---
myst:
  html_meta:
    description: "Run batch and Flink-style continuous SQL over Klein DataStreams and define connectors with Table DDL."
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
tables previously created through `execute_sql()`. It does inherit scalar
functions and AI-function backends registered on the current context.

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

## Use common SQL built-ins

Klein provides a portable set of common scalar functions through the shared
batch and streaming expression evaluator. Use them directly in SQL; no Python
registration is required.

| Category | Functions and expressions |
|---|---|
| Text | `LOWER`, `UPPER`, `LENGTH`, `TRIM`, `CONCAT`, `CONCAT_WS`, `SUBSTRING`, `REPLACE`, `CONTAINS`, `STARTSWITH`, `ENDSWITH`, `LIKE`, `ILIKE` |
| Conditional and null | `COALESCE`, `NULLIF`, `IF`, `CASE`, `GREATEST`, `LEAST` |
| Numeric | `ABS`, `ACOS`, `ASIN`, `ATAN`, `CEIL`, `COS`, `EXP`, `FLOOR`, `LN`, `LOG`, `POWER`, `ROUND`, `SIGN`, `SIN`, `SQRT`, `TAN`, `TRUNC` |
| Date | `TO_DATE`, `YEAR`, `MONTH`, `DAY`, `DATE_ADD`, `DATE_SUB`, `DATEDIFF` |
| JSON and collections | `PARSE_JSON`, `GET_JSON_OBJECT`, `TO_JSON`, `ARRAY`, `MAP`, `ARRAY_SIZE` |
| Hash and encoding | `MD5`, `SHA2`, `BASE64`, `UNBASE64` |

These functions follow SQL `NULL` propagation unless their purpose defines
otherwise; for example, `COALESCE` selects the first non-`NULL` value and
`CONCAT_WS` skips `NULL` inputs. Date parsing currently accepts ISO values,
date arithmetic uses day offsets, and `DATEDIFF` returns whole days. `SHA2`
accepts bit lengths `0`, `224`, `256`, `384`, and `512`, where `0` means
SHA-256.

## Process images and PDFs

Install `ray-klein[media]` on every worker, then use the built-in media
functions directly on `BINARY` columns. They are fused into one batched
operator per projection. A worker prefers native libvips processing, including
its SIMD and threaded codec paths, and falls back to Pillow when libvips cannot
load or save a format. Repeated functions over the same value in one row share
the decoded image or PDF handle. Native threads match the SQL operator's integer
CPU reservation (at least one); a preconfigured `VIPS_CONCURRENCY` environment
variable takes precedence.

| Function | Result |
|---|---|
| `IMAGE_WIDTH(data)`, `IMAGE_HEIGHT(data)` | Oriented image dimensions as integers |
| `IMAGE_FORMAT(data)` | Canonical input format name |
| `IMAGE_RESIZE(data, width, height [, fit [, format [, quality]]])` | Encoded image bytes; defaults are `contain`, `PNG`, and quality `85` |
| `PDF_PAGE_COUNT(data)` | Number of pages |
| `PDF_SPLIT(data [, start_page [, end_page]])` | One single-page PDF per page as `ARRAY<BINARY>` |
| `PDF_RENDER_PAGE(data, page [, dpi])` | One page as PNG bytes; DPI defaults to `144` |
| `PDF_TO_IMAGES(data [, dpi [, start_page [, end_page]]])` | An inclusive page range as `ARRAY<BINARY>` PNG images |

PDF page numbers and ranges are 1-based. `contain` preserves aspect ratio
inside the requested box, `cover` fills and center-crops it, and `stretch`
returns the exact dimensions. Supported resize outputs are PNG, JPEG, WebP,
TIFF, GIF, BMP, AVIF, HEIF, JPEG 2000, JPEG XL, ICO, and PPM; less common
codecs remain dependent on the libvips or Pillow build installed on the worker.
Inputs can use any image loader exposed by either backend, so formats such as
SVG and additional camera or scientific formats can be enabled by a custom
libvips build. Multi-frame inputs currently operate on the first frame.

`DOWNLOAD(uri_column)` can be the direct data argument of a media function, so
fetch, decode, resize, and AI consumption remain in the distributed plan:

```sql
SELECT id,
       IMAGE_RESIZE(DOWNLOAD(uri), 1024, 1024, 'contain', 'WEBP', 85) AS image,
       AI_GENERATE(PDF_RENDER_PAGE(DOWNLOAD(pdf_uri), 1, 144)) AS summary
FROM assets
```

SQL `NULL` inputs produce `NULL` without invoking a decoder. Invalid media
fails the task without including payload data in the error. Per-call limits
bound input and output bytes, decoded pixels, PDF pages, and render DPI; the
defaults are 256 MiB, 64 megapixels, 100 selected pages, and 600 DPI. One fused
media batch processes one row at a time and has a 512 MiB cumulative
binary-value budget.

## Register Python scalar functions

Klein SQL scalar functions use ordinary Python values and do not expose a Ray
Data expression contract. Register a function, then call it from SQL with an
unquoted, case-insensitive name:

```python
def normalize_prompt(value, prefix):
    if value is None:
        return None
    return prefix + value.strip().lower()

ray.klein.register_scalar_function("normalize_prompt", normalize_prompt)
prepared = ray.klein.sql(
    "SELECT id, NORMALIZE_PROMPT(prompt, 'query: ') AS prompt FROM inputs",
    tables={"inputs": inputs},
)
```

For an explicitly isolated `KleinContext`, register through
`context.sql_session.register_scalar_function()` instead.

For a one-off `ray.klein.sql()` query, pass query-local bindings. They override
same-named session functions without changing the session:

```python
prepared = ray.klein.sql(
    "SELECT NORMALIZE_PROMPT(prompt, 'query: ') AS prompt FROM inputs",
    tables={"inputs": inputs},
    functions={"normalize_prompt": normalize_prompt},
)
```

Arguments are evaluated with SQL semantics and passed as Python scalars. SQL
`NULL` is passed as `None`; the function decides whether to propagate or replace
it. Functions work in projections, predicates, grouping expressions, and
built-in aggregate inputs. Registration validates the name and callable;
query planning validates call arity and rejects unknown functions before workers
run. Built-in SQL/Klein function names cannot be replaced.

The function and its closure are serialized with the query graph. Install its
dependencies on every worker and treat it as trusted application code. Scalar
functions are synchronous, share the enclosing SQL operator's resources, and
run row by row. For model initialization, GPU allocation, vectorized inference,
or external-service concurrency, use the batched SQL AI functions below (or
`map_batches()` before SQL). Aggregate UDFs are not supported. In streaming
changelog or recoverable queries, functions must also be deterministic and
side-effect-free: the same arguments must always produce the same result so
retractions remain correct, and retries may invoke a function more than once.
Materialize nondeterministic or external results before ordinary SQL operators.

## Register batched AI functions

`AI_GENERATE` and `AI_EMBED` are first-class SQL expressions with a
provider-neutral execution contract. Klein does not choose a model provider or
own its SDK and credentials. Register trusted application code as the backend;
a callable class is initialized once per worker, which is the appropriate place
to construct a local model or provider client.

```python
class GenerateBackend:
    def __init__(self, model_name):
        self.model = load_model_or_client(model_name)

    def __call__(self, calls):
        # Each item is the argument tuple from one non-NULL SQL call.
        prompts = [prompt for (prompt,) in calls]
        return self.model.generate(prompts)


ray.klein.register_ai_function(
    "AI_GENERATE",
    GenerateBackend,
    fn_constructor_kwargs={"model_name": "my-model"},
    batch_size=32,
    concurrency=4,
    num_gpus=1,
)

generated = ray.klein.sql(
    "SELECT id, AI_GENERATE(prompt) AS answer FROM inputs",
    tables={"inputs": inputs},
)
```

Register `AI_EMBED` the same way; its result for each call can be a vector. A
backend receives `calls`, a sequence of one- or two-element argument tuples,
and must return a result sequence of exactly the same length and order. The
first argument is the input. An optional second scalar argument carries
provider-specific configuration without Klein interpreting it. If any argument
for a row is SQL `NULL`, Klein produces `NULL` for that row and does not include
the call in the backend batch.

Use `batch_size` and, for streaming, `batch_timeout` to control batching. Use
`concurrency`, `num_cpus`, and `num_gpus` to control worker concurrency and
resources. Keep secrets in the worker environment or its secret-management
facility rather than SQL text. The backend and its constructor arguments are
serialized with the query graph and must be available on every worker. A batch
may be retried, so backends must tolerate duplicate calls and account for
provider idempotency and cost.

Synchronous backends work for bounded batch and streaming queries. An
`async def` backend is supported only in streaming mode; `async_buffer_size`
controls its in-flight batch buffer. AI calls currently have these planner
boundaries:

- The call must be a top-level `SELECT` expression, optionally with an alias;
  it cannot be nested in another expression, used in a predicate, or used by an
  aggregate query.
- Streaming input must be insert-only. Updating/retracting changelog streams
  are rejected before execution.
- `AI_CLASSIFY`, `AI_SIMILARITY`, `AI_AGG`, and `AI_SUMMARIZE_AGG` are reserved
  for later versions and currently raise a planning error.

For an isolated context, call
`context.sql_session.register_ai_function()`. Registrations are session-local;
the top-level `ray.klein.sql()` inherits the current context's registered AI
backends, just as it does scalar functions.

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
[Delivery semantics](delivery-semantics.md) before using it in a recoverable
pipeline.
