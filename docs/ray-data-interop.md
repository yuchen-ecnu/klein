---
myst:
  html_meta:
    description: "Use Ray Data readers and Dataset operations from Klein without duplicating the installed Ray API."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray Data interoperability

For a connector-oriented summary of readers, transforms, writers, execution
modes, and guarantees, see the dedicated [Ray Data connector](connectors/ray-data.md).

Klein does not copy Ray Data's loading APIs. Those signatures change
between Ray releases and duplicating them creates an immediately stale second
API. Instead, the explicit adapter resolves them from the installed Ray
version:

- `pipeline.ray_data.read_csv`, `read_parquet`, `range`, and other factories
  resolve their installed `ray.data` counterpart dynamically. Module-level
  factories remain available for deferred compatibility code.
- `stream.ray_data` resolves public `ray.data.Dataset` methods. The shorter
  `stream.data` alias remains available for compatibility.

Resolution happens against the installed Ray version when the graph executes.
Arguments are forwarded unchanged, while `help()`, `inspect.signature()`, and
the docstring come from that same Ray installation.

```python
import ray.klein as klein

pipeline = klein.pipeline(name="ray-data-read")
stream = pipeline.ray_data.read_csv("s3://bucket/input").ray_data.random_shuffle(seed=7)
rows = stream.ray_data.take(10).result()
```

New Ray factories and Dataset methods are available automatically. Inspect the
current installation with `pipeline.ray_data.available` and
`stream.ray_data.available`.

## Ray Data expressions

Klein forwards Ray 2.56 expression objects unchanged, so their exact AST,
schema inference, optimizer rules, and execution operators remain owned by Ray
Data in batch mode. The expression-bearing `with_column` and `filter(expr=...)`
forms also have native Klein streaming implementations:

```python
from ray.data.expressions import col, download, random, uuid

prepared = (
    pipeline.ray_data.read_parquet("input/")
    .ray_data.with_column("total", col("price") * col("quantity"))
    .ray_data.with_column("body", download("uri"))
    .ray_data.with_column("sample", random(seed=7))
    .ray_data.with_column("request_id", uuid())
    .ray_data.filter(expr=col("total").is_not_null() & (col("total") > 0))
)
```

This includes Ray 2.56's column/literal AST, arithmetic, comparison, boolean,
null and membership operators, aliases, PyArrow and Python UDF expressions,
string/list/array/map/struct/datetime namespaces, synthetic IDs/random/UUIDs,
and the dedicated `download()` expression. In both modes Klein routes
`DownloadExpr` through the same `sql.download.*` URI, SSRF, redirect, timeout,
and byte policy used by SQL. Batch mode uses one-row download batches; streaming
keeps one request in flight per task so a completed-response limit cannot be
multiplied by a count-only async window. A null, rejected, or unreadable URI
produces `None`, matching Ray's soft-failure contract.

## Choose Klein or Ray Data operations

Use native Klein methods such as `stream.map` and `stream.filter` for general
unbounded transformations. `stream.ray_data.with_column(name, expr)` and
`stream.ray_data.filter(expr=expr)` work in both modes; other `stream.ray_data`
transforms and all terminal consumers remain batch-only.

`pipeline.read_kafka(..., trigger="once")` delegates to Ray Data, while
`trigger="continuous"` selects Klein's unbounded, checkpoint-aware source. The
continuous source keeps the same raw record schema and modern Confluent
`consumer_config` style, adds source `concurrency`, partition discovery, and
poll-batch controls, and runs only on the streaming backend. Put Confluent
authentication settings in `consumer_config`.

This explicit boundary also resolves name collisions. For example,
`stream.map(fn)` is Klein's stream/batch operation, while
`stream.ray_data.map(fn)` is the currently installed Ray Dataset method.

## Arbitrary and third-party operations

The named dynamic methods cover every public Dataset factory and method. The
explicit forms cover third-party connectors and multi-step Ray objects such as
`GroupedData` without requiring Klein to understand those types:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="customer-summary")
source = pipeline.ray_data.source(my_dataset_factory, config)

aggregated = source.ray_data.transform(lambda ds: ds.groupby("customer_id").mean("amount"))

summary = aggregated.ray_data.consume(lambda ds: ds.summary()).result()
```

A transform callable must return exactly one `Dataset`. A consumer may return
any value. On an explicit `Pipeline`, the consumer submits its single terminal
and returns a job handle. The module-level API remains deferred: its terminal
registers the consumer, and `ray.klein.execute("job-name").result()` returns
the value.

Other Klein streams passed anywhere inside positional or keyword arguments are
automatically compiled into Dataset dependencies. This supports methods such
as `union`, `zip`, and `join` without exposing compiler internals:

```python
pipeline = klein.pipeline(name="join-datasets")
left = pipeline.ray_data.read_parquet("left/")
right = pipeline.ray_data.read_parquet("right/")
joined = left.ray_data.join(right, join_keys="id")
```

## Advanced: isolate graph-building contexts

`KleinContext` retains a deferred, permissive builder for compatibility and
advanced selective execution. Its `context.ray_data` namespace and the shorter
`context.data` alias remain available. Ray Data methods are generally
unavailable directly on `KleinContext` or `DataStream`; use
`context.ray_data.read_csv(...)` and `stream.ray_data.random_shuffle(...)`.
The documented stable sink entry points are exceptions, including
`stream.write_sql(...)`, which uses Ray Data in batch mode and Klein's
at-least-once DB-API sink in streaming mode.
