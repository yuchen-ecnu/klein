---
myst:
  html_meta:
    description: "Use Ray Data readers and Dataset operations from Klein for Ray without duplicating the installed Ray API."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray Data interoperability

For a connector-oriented summary of readers, transforms, writers, execution
modes, and guarantees, see the dedicated [Ray Data connector](connectors/ray-data.md).

Klein for Ray does not copy Ray Data's loading APIs. Those signatures change
between Ray releases and duplicating them creates an immediately stale second
API. Instead, source factories are exposed directly from `ray.klein`:

- `ray.klein.read_csv`, `read_parquet`, `range`, and other factories resolve
  their installed `ray.data` counterpart dynamically.
- `stream.data` resolves public `ray.data.Dataset` methods.

Resolution happens against the installed Ray version when the graph executes.
Arguments are forwarded unchanged, while `help()`, `inspect.signature()`, and
the docstring come from that same Ray installation.

```python
import ray
import ray.klein

stream = (
    ray.klein.read_csv("s3://bucket/input")
    .data.random_shuffle(seed=7)
)
stream.data.take(10)
rows = ray.klein.execute("ray-data-read").get()
```

New Ray factories and Dataset methods are available automatically. Inspect the
current installation with `dir(ray.klein)` and `stream.data.available`.

## Ray Data expressions

Klein forwards Ray 2.56 expression objects unchanged, so their exact AST,
schema inference, optimizer rules, and execution operators remain owned by Ray
Data in batch mode. The expression-bearing `with_column` and `filter(expr=...)`
forms also have native Klein streaming implementations:

```python
from ray.data.expressions import col, download, random, uuid

prepared = (
    ray.klein.read_parquet("input/")
    .data.with_column("total", col("price") * col("quantity"))
    .data.with_column("body", download("uri"))
    .data.with_column("sample", random(seed=7))
    .data.with_column("request_id", uuid())
    .data.filter(expr=col("total").is_not_null() & (col("total") > 0))
)
```

This includes Ray 2.56's column/literal AST, arithmetic, comparison, boolean,
null and membership operators, aliases, PyArrow and Python UDF expressions,
string/list/array/map/struct/datetime namespaces, synthetic IDs/random/UUIDs,
and the dedicated `download()` expression. In batch mode, `download()` is not
converted to a row UDF: `Dataset.with_column()` retains Ray's URI partitioning
and concurrent download plan. In streaming mode, Klein evaluates one URI per
record in a bounded, order-preserving asynchronous window. A null or unreadable
URI produces `None`, matching Ray 2.56's download operator.

## Choose Klein or Ray Data operations

Use native Klein methods such as `stream.map` and `stream.filter` for general
unbounded transformations. `stream.data.with_column(name, expr)` and
`stream.data.filter(expr=expr)` work in both modes; other `stream.data`
transforms and all terminal consumers remain batch-only.

`ray.klein.read_kafka(..., trigger="once")` delegates to Ray Data, while
`trigger="continuous"` selects Klein's unbounded, checkpoint-aware source. The
continuous source keeps the same raw record schema and modern Confluent
`consumer_config` style, adds source `concurrency`, partition discovery, and
poll-batch controls, and runs only on the streaming backend. Put Confluent
authentication settings in `consumer_config`.

This explicit boundary also resolves name collisions. For example,
`stream.map(fn)` is Klein's stream/batch operation, while
`stream.data.map(fn)` is the currently installed Ray Dataset method.

## Arbitrary and third-party operations

The named dynamic methods cover every public Dataset factory and method. The
explicit forms cover third-party connectors and multi-step Ray objects such as
`GroupedData` without requiring Klein to understand those types:

```python
source = ray.klein.source(my_dataset_factory, config)

aggregated = source.data.transform(
    lambda ds: ds.groupby("customer_id").mean("amount")
)

aggregated.data.consume(lambda ds: ds.summary())
summary = ray.klein.execute("customer-summary").get()
```

A transform callable must return exactly one `Dataset`. A consumer may return
any value. Consumers remain lazy: the terminal call registers the consumer and
`ray.klein.execute("job-name").get()` returns its result.

Other Klein streams passed anywhere inside positional or keyword arguments are
automatically compiled into Dataset dependencies. This supports methods such
as `union`, `zip`, and `join` without exposing compiler internals:

```python
left = ray.klein.read_parquet("left/")
right = ray.klein.read_parquet("right/")
joined = left.data.join(right, join_keys="id")
```

## Advanced: isolate graph-building contexts

`KleinContext` can isolate multiple graph builders in one process. Its
`context.data` namespace remains available for explicitly scoped graph builders,
while ordinary application code should prefer the module-level readers. Ray
Data methods are generally unavailable directly on `KleinContext` or
`DataStream`; use `context.data.read_csv(...)` and
`stream.data.random_shuffle(...)` when the explicit namespace is needed. The
documented stable sink entry points are exceptions, including
`stream.write_sql(...)`, which uses Ray Data in batch mode and Klein's
at-least-once DB-API sink in streaming mode.
