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

ray.klein.reset_context().enable_interactive_mode()

rows = (
    ray.klein.read_csv("s3://bucket/input")
    .data.random_shuffle(seed=7)
    .data.take(10)
)
```

New Ray factories and Dataset methods are available automatically. Inspect the
current installation with `ray.klein.current_context().data.available` and
`stream.data.available`.

## Choose Klein or Ray Data operations

Use native Klein methods such as `stream.map` and `stream.filter` when an
operation must work in an unbounded streaming pipeline. Calls under
`stream.data` have exact Ray Data batch semantics and are batch-only.

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

result = aggregated.data.consume(lambda ds: ds.summary())
```

A transform callable must return exactly one `Dataset`. A consumer may return
any value. In interactive mode a consumer returns that value immediately; in
regular mode use `ray.klein.execute(...).get()`.

Other Klein streams passed anywhere inside positional or keyword arguments are
automatically compiled into Dataset dependencies. This supports methods such
as `union`, `zip`, and `join` without exposing compiler internals:

```python
left = ray.klein.read_parquet("left/")
right = ray.klein.read_parquet("right/")
joined = left.data.join(right, join_keys="id")
```

## Isolate graph-building contexts

`KleinContext` can isolate multiple graph builders in one process. Its
`context.data` namespace remains available for explicitly scoped graph builders,
while application code should prefer the module-level readers. Ray Data methods
are deliberately unavailable directly on `KleinContext` or `DataStream`;
use `context.data.read_csv(...)` and `stream.data.random_shuffle(...)` when the
explicit namespace is needed.
