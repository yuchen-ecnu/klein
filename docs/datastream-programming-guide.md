---
myst:
  html_meta:
    description: "Build Klein for Ray DataStream programs with precise record, UDF, batching, async, partitioning, and failure semantics."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# DataStream programming guide

Klein programs build a lazy graph of sources, transformations, and terminal
sinks. The whole graph runs either on the Ray Data batch backend or on Klein's
native streaming backend. The selected backend matters: both accept the same
core transformation calls, but batching, scheduling, schema discovery, and
failure recovery belong to the backend that executes the graph.

Use the [operator compatibility matrix](operator-compatibility.md) before
mixing batch-only and streaming-only operations, and inspect the complete graph
with `ray.klein.explain(...)` before submitting it.

## Records and schemas

The portable DataStream record is a mapping from column name to Python value:

```python
{"event_id": "e-17", "amount": 12.5, "tags": ["new", "paid"]}
```

Native streaming sources and operators require every emitted data record to be
a `Mapping`. Wrap a scalar or object in a named field instead of emitting it
directly:

```python
# Valid
return {"item": value}

# Invalid in native streaming
return value
```

`ChangelogRow` is also a mapping, with row-kind metadata used by continuous SQL
and changelog-aware connectors. Ordinary dictionaries are append-only rows.

Klein does not require a declared static schema for an ordinary native stream.
Mapping keys are the column names, and value types are checked by the UDF or
connector that consumes them. Consequently, a graph can be constructed even
when two branches produce incompatible fields; the error appears only when an
operator or sink tries to use those fields. Validate important contracts in
application code and keep branch schemas consistent before `union()`.

`DataStream.schema()` is a terminal operation. In batch mode it reports the
schema discovered by the underlying Ray Data `Dataset`; it is not a schema
declaration for a native streaming graph. Connector formats can impose their
own schemas and changelog modes. See the [connector catalog](connectors/index.md)
and [SQL guide](sql.md) for those contracts.

### Row-shaped and columnar records

Without operator batching, a UDF receives one row mapping at a time. With
native streaming batching enabled, Klein converts an arrival-ordered group of
rows into a column mapping:

```python
{
    "event_id": numpy.array(["e-17", "e-18"]),
    "amount": numpy.array([12.5, 9.0]),
}
```

A columnar result must itself be a mapping, every column must be sequence-like,
and all columns must have the same length. A malformed result fails the UDF
invocation. Zero-length output represents no rows.

For code that must run on either backend, prefer row mappings for `map`,
`flat_map`, and `filter`, and reserve vectorized code for `map_batches`. Ray
Data owns its batch-mode schema inference and conversion rules; see
[Ray Data interoperability](ray-data-interop.md).

## Choose a transformation

| Operation | Input and output contract | Use it for |
|---|---|---|
| `map(fn)` | One input invocation produces one mapping. With native batching, one column mapping produces one column mapping. | One-to-one enrichment or replacement. |
| `map_batches(fn)` | One columnar batch produces one columnar batch; its row count may change. | Vectorized or accelerator-backed work. |
| `flat_map(fn)` | One input invocation returns or yields zero or more mappings. With native batching, each yielded mapping is a columnar batch. | Expansion, tokenization, or optional output. |
| `filter(fn)` | Returns one Boolean per row, or one Boolean sequence per native batch. Retained rows are unchanged. | Row selection. |
| `map_reduce(...)` | Expands each input, processes expanded rows in batches, regroups them, then emits one postprocessed mapping. | A streaming fan-out, batched inference, fan-in pattern. |

All five operations are lazy. They return another `DataStream`; attach a sink
and call `execute()` to run them.

## Map one record to one record

`map` is the default choice for row-wise transformations:

```python
import ray
import ray.klein

events = ray.klein.from_items(
    [
        {"event_id": "e-17", "amount": 12.5},
        {"event_id": "e-18", "amount": 9.0},
    ]
)

normalized = events.map(
    lambda row: {
        **row,
        "amount_cents": round(row["amount"] * 100),
    },
    name="NormalizeAmount",
    concurrency=4,
)
normalized.take_all()
rows = ray.klein.execute("normalize-amount").get()
```

The unbatched callable must return exactly one mapping. Returning `None`, a
scalar, or a list of rows is invalid in native streaming; use `filter` or
`flat_map` for those cardinalities.

`map(batch_size=N)` has a deliberately narrower meaning than `map_batches`.
It enables columnar invocations in native streaming, but the Ray Data lowering
of `map` remains row-wise. Avoid this form in a graph that may run in batch
mode. Prefer `map_batches` when batching is part of the logical UDF contract.

## Map a vectorized batch

`map_batches` makes the batch contract explicit:

```python
from datetime import timedelta

import numpy as np
import ray
import ray.klein


def add_tax(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    return {
        **batch,
        "amount_with_tax": batch["amount"] * 1.08,
    }


stream = (
    ray.klein.from_items(
        [
            {"event_id": "e-17", "amount": 12.5},
            {"event_id": "e-18", "amount": 9.0},
        ]
    )
    .map_batches(
        add_tax,
        batch_size=256,
        batch_timeout=timedelta(seconds=2),
        batch_format="numpy",
        name="AddTax",
    )
)
stream.take_all()
rows = ray.klein.execute("add-tax").get()
```

`"default"` and `"numpy"` expose batches as
`dict[str, numpy.ndarray]`. The public API also forwards `"pandas"` to Ray
Data in batch mode, where the callable receives a `pandas.DataFrame`. Pandas is
not a portable native-streaming batch contract; use the NumPy-style mapping
when the same graph may select streaming.

In native streaming, `batch_size` is the maximum number of rows in one
invocation, not a promise that every batch is full. A partial batch is flushed
when its timeout expires, at an end-of-input or checkpoint/control boundary,
or when a two-input side changes. In batch mode, Ray Data controls block
boundaries and may also supply a smaller batch. The output may contain fewer or
more rows than the input, provided all output columns have equal length.

## Expand with `flat_map`

A `flat_map` callable returns any finite iterable of mappings. A generator is
useful when expansion would otherwise allocate a large list:

```python
def split_order(order: dict):
    for line in order["lines"]:
        yield {
            "order_id": order["order_id"],
            "sku": line["sku"],
            "quantity": line["quantity"],
        }


lines = orders.flat_map(split_order, name="SplitOrder")
```

Returning an empty iterable drops the input. Values yielded for one invocation
are emitted in generator order. A generator is not transactional: if it yields
three rows and then raises, those three rows have already been emitted. A retry
can emit them again.

When `batch_size` is set in native streaming, the callable receives a column
mapping and every yielded value must itself be a valid column mapping. As with
`map`, that option does not turn the Ray Data `flat_map` lowering into a batch
UDF, so use it only for a deliberately streaming graph.

## Filter records

An unbatched predicate returns a truth value for one row:

```python
paid = events.filter(
    lambda row: row["status"] == "paid" and row["amount"] > 0,
    name="PaidOrders",
)
```

With native streaming batching enabled, the predicate receives a column
mapping and must return one Boolean decision for every input row:

```python
paid = events.filter(
    lambda batch: (batch["status"] == "paid").tolist(),
    batch_size=512,
)
```

The number of decisions must match the batch length. Retained values are the
original input rows; `filter` does not apply changes made to a separate result.
Ray Data batch execution continues to use its row-wise `Dataset.filter`
contract, so the batched predicate form is native-streaming-only.

## Compose expansion, batching, and regrouping

`map_reduce` is a native-streaming composite for a common inference pattern:

```text
input
  -> flat-map preprocess
  -> adaptive shuffle
  -> batched process
  -> key partition
  -> ordered regroup and postprocess
```

The preprocess callable expands one input mapping into zero or more mutable
row mappings. Klein remembers their original order and identity. The batch
callable receives column mappings and must preserve the number of rows while
returning equal-length columns. `key_selector` must select the same logical key
for every expanded row from one original input. It routes processed rows, and
the final callable receives one mapping of lists for that original input in
preprocess order.

```python
import numpy as np

from ray.klein.api import MissingDataStrategy


def expand(document):
    for sentence in document["sentences"]:
        yield {"document_id": document["id"], "sentence": sentence}


def embed(batch):
    return {
        "document_id": batch["document_id"],
        "embedding": np.asarray(
            [[len(sentence)] for sentence in batch["sentence"]]
        ),
    }


def assemble(group):
    return {
        "document_id": group["document_id"][0],
        "embeddings": group["embedding"],
    }


embedded = documents.map_reduce(
    key_selector=lambda row: row["document_id"],
    preprocess_fn=expand,
    batch_process_fn=embed,
    postprocess_fn=assemble,
    concurrency=(2, 4, 2),
    batch_process_size=64,
    batch_process_format="numpy",
    preprocess_missing_data_strategy=MissingDataStrategy.ERROR,
    name="EmbedDocuments",
)
```

The three entries in `num_cpus`, `num_gpus`, and `concurrency` configure the
preprocess, batch-process, and postprocess stages respectively. Each stage also
has its own callable-class constructor arguments. `batch_process_timeout`
controls partial streaming batches.

If preprocessing emits nothing, `MissingDataStrategy.ERROR` fails the
invocation, `WARNING` logs and drops it, and `IGNORE` silently drops it. This is
separate from the job-wide UDF exception policy. `map_reduce` has no Ray Data
lowering; explicitly select streaming for a bounded graph that uses it:

```python
ray.klein.configure({"execution.runtime.mode": "streaming"})
```

## Callable forms and worker construction

Transforms accept a plain function or a callable class. `flat_map` additionally
works naturally with a synchronous generator function.

| Form | Construction and invocation |
|---|---|
| Plain function or lambda | Serialized with the graph and invoked directly. No runtime context is injected. |
| Synchronous generator function | Supported where the operator expects an iterable, notably `flat_map` and preprocess. |
| Callable class type | Instantiated for each worker/subtask, then its `__call__` method is invoked. Pass the class, not an instance. |
| `async def` callable | Native streaming support requires a positive `async_buffer_size`. |
| Async generator | Not supported; use an async coroutine that returns an ordinary finite iterable for `flat_map`. |

Constructor arguments are valid only with a callable class:

```python
class Score:
    def __init__(self, threshold, *, runtime_context):
        self.threshold = threshold
        self.task_index = runtime_context.task_index
        self.counter = runtime_context.metric_group.counter("scored")

    def __call__(self, row):
        self.counter.inc()
        return {**row, "accepted": row["score"] >= self.threshold}


scored = events.map(
    Score,
    fn_constructor_args=(0.8,),
    name="Score",
    concurrency=4,
)
```

If a callable class constructor has a parameter literally named
`runtime_context`, Klein supplies it as a keyword argument. Do not also provide
that keyword. The read-only context exposes:

- `task_name`, `task_index`, and `parallelism`;
- the effective job `config`;
- the operator `metric_group`;
- `runtime_info`, including batching and async settings;
- `job_id`, which is the live job namespace in native streaming.

In Ray Data batch execution, the context is a compiler-side operator context;
Ray Data owns physical worker identities, so `task_index` and `parallelism` are
not streaming subtask coordinates. Do not use them for batch partitioning.

An ordinary callable class has construction but no managed `open()`/`close()`
lifecycle. Build per-worker immutable helpers in its constructor. For resources
that require deterministic cleanup or checkpoint participation, implement a
documented source/sink lifecycle instead of relying on a transform object's
destructor.

## Batching, timeouts, and ordered async I/O

The common tuning arguments describe different limits:

- `batch_size` bounds rows per native-streaming invocation. `None` disables
  batching; positive integers enable it.
- `batch_timeout` bounds how long a partial native-streaming batch waits after
  its first row. It is meaningful only when batching is enabled.
- `async_buffer_size` bounds concurrent async invocations in each streaming
  subtask. It must be a positive integer when set.
- `concurrency` controls physical operator parallelism; `num_cpus` and
  `num_gpus` are the resources requested for each operator worker.

For `map_batches`, Ray Data receives `batch_size` and `batch_format` in batch
mode, but not the streaming timeout. For `map`, `flat_map`, and `filter`, Ray
Data uses its row-wise operation and does not receive Klein's streaming batch
settings. `async_buffer_size` is a native-streaming execution window, not a Ray
Data concurrency option.

An async transform must opt in explicitly:

```python
import asyncio


async def enrich(row):
    await asyncio.sleep(0)  # replace with bounded async I/O
    return {**row, "enriched": True}


enriched = events.map(enrich, async_buffer_size=32, name="Enrich")
```

Klein starts at most `async_buffer_size` calls per subtask. Calls may complete
out of order, but results are emitted in input order. A checkpoint barrier or
other control message is queued behind all earlier invocations, so it cannot
overtake their output. When the window is full, upstream consumption waits;
this is backpressure, not record dropping.

Set `async_buffer_size` only for an awaitable callable. Without it, an async
function is invoked through the synchronous path and its coroutine is not a
valid record. With it, a synchronous return value cannot be awaited. For async
`flat_map`, the coroutine must resolve to an ordinary iterable of mappings.

Synchronous invocations are serialized within each native streaming subtask.
Parallelism comes from multiple subtasks. Ordering is therefore defined per
input channel/subtask; shuffles and parallel union branches do not establish a
global order.

## Exceptions, retries, and data loss

The default `udf.ignore-exception=false` propagates a UDF exception. The task
fails, and streaming recovery may replay data from a checkpoint. Batch-mode
failure and retry behavior belongs to Ray Data.

Setting the option to `true` logs the error, increments the UDF exception
metric, and continues with later input:

```python
ray.klein.configure({"udf.ignore-exception": True})
```

In `auto` mode this setting selects native streaming even for an otherwise
batch-lowerable bounded graph. Ray Data's block-level error policy does not
provide the same per-record continuation and Klein metric semantics.

This is an explicit data-loss policy, not a dead-letter queue. A failed sync or
async invocation normally emits no result, which can corrupt aggregates,
joins, state, and source-to-sink accounting. A synchronous `flat_map` generator
is an additional edge case: rows yielded before the exception remain emitted,
so ignoring the exception does not make that input atomically dropped. Keep the
default unless the missing-output behavior is acceptable and observable.

## Serialization and external side effects

Klein and Ray serialize UDFs, closures, callable classes, and constructor
arguments to execute them away from the driver. Do not capture open sockets,
file handles, locks, event loops, or other non-serializable driver state. Ship
small immutable configuration and construct clients on the worker instead.
Mutable driver globals are not shared state between workers.

Worker failure can re-run an invocation. Generator output can be replayed, and
an external request can succeed even if its acknowledgement is lost before a
checkpoint. Make transform-side effects idempotent using a stable event key, or
move effects to a sink with the required delivery protocol. See
[delivery and consistency guarantees](delivery-semantics.md).

Do not keep correctness-critical aggregation state in a callable-class field.
It is local to one worker and is not automatically checkpointed or repartitioned.
Use [managed keyed state](ray-native-state.md) for recoverable state.

## Partition, merge, and chain streams

The partitioning calls `broadcast()`, `rescale()`, `round_robin()`,
`adaptive_shuffle()`, and `partition_by(...)` configure the outgoing edge used
by the **next** native-streaming operator. Apply them immediately before that
consumer:

```python
balanced = events.round_robin().map(transform, concurrency=8)
```

`key_by(selector)` is different: it creates stable key-group routing for
stateful processing, windows, joins, and rescaling. Read
[managed state](ray-native-state.md) before choosing keys or max parallelism.
These edge methods do not repartition a Ray Data batch; use the matching
`stream.data` operation for batch partitioning.

`left.union(right, ...)` requires every input to belong to the same internal
pipeline. It merges the branches but defines no total order between them.
Each branch retains its own upstream order until scheduling or a downstream
shuffle interleaves records. In batch mode, union delegates to Ray Data.

Native streaming enables operator chaining by default. Compatible stateless,
synchronous operators connected by a forward edge can share one task and avoid
an actor hop. Chaining requires matching CPU/GPU, concurrency, batch-size, and
async-buffer contracts. A shuffle, multiple upstreams, managed state, async
execution, or different resources creates a boundary. Chaining changes physical
placement, not record semantics. Disable
`pipeline.operator-chaining.enabled` when diagnosing lifecycle behavior or
when an operator must remain an independent scaling unit.

## Related guides

- [Event time and watermarks](event-time.md) covers timestamps, idleness,
  windows, and late data.
- [Managed state](ray-native-state.md) covers keyed state and timers.
- [SQL](sql.md) explains bounded and continuous relational execution.
- [Connector catalog](connectors/index.md) lists source, sink, schema, and
  delivery contracts.
- [Performance tuning](performance-tuning.md) covers resource sizing,
  concurrency, buffers, and chaining.
- [Job lifecycle](job-lifecycle.md) explains sinks, execution, handles,
  cancellation, and cleanup.
