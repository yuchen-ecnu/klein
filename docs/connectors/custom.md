---
myst:
  html_meta:
    description: "Implement custom Klein for Ray SourceFunction, SinkFunction, two-phase-commit sink, and TableFactory connectors."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-custom-connectors)=
# Custom connectors

Use a custom connector when an external system has no public Ray Data adapter
and no built-in Klein integration. Native `SourceFunction` and `SinkFunction`
connectors execute only on Klein's streaming runtime unless you also provide an
internal Ray Data lowering.

## Implement a source

A source class implements `run`, `cancel`, `snapshot_state`, and
`restore_state`. It may implement `open`, `close`, and
`notify_checkpoint_complete` for lifecycle and external offset commits.

```python
import ray
from threading import Event
from ray.klein import SourceFunction


class RecordSource(SourceFunction):
    def __init__(self, records):
        self.records = tuple(records)
        self.next_index = 0
        self.cancelled = Event()

    def run(self, context):
        while self.next_index < len(self.records) and not self.cancelled.is_set():
            record = self.records[self.next_index]
            self.next_index += 1
            context.collect(record)

    def cancel(self):
        self.cancelled.set()

    def snapshot_state(self, checkpoint_id):
        return {"next_index": self.next_index}

    def restore_state(self, state):
        self.next_index = state["next_index"]


ray.klein.configure({"execution.runtime.mode": "streaming"})
stream = ray.klein.source(
    RecordSource,
    fn_constructor_args=[[{"id": 1}, {"id": 2}]],
    bounded=True,
    concurrency=1,
    name="records",
)
```

Pass the class, not a preconstructed instance, so every Ray task owns its
resources. `fn_constructor_args` and `fn_constructor_kwargs` are serialized as
top-level actor-construction arguments. `num_cpus`, `num_gpus`, and
`concurrency` control source resources; `bounded` records finiteness but does
not make a native source batch-lowerable. A bounded native source without a
lowering must force streaming as shown above. Declare `changelog_mode` when
the source can emit non-insert `RowKind` values.

`SourceContext.collect()` accepts mapping records and preserves source order.
Advance the externally meaningful position before calling `collect`, as in the
example, so a barrier caused by that call snapshots the matching next position.
The other context methods are:

| Method | When to use it |
|---|---|
| `on_idle()` | A blocking poll returned no data; lets time checkpoints and watermark idleness advance. |
| `emit_watermark(timestamp)` | All preceding records are at or before explicit event-time progress. |
| `mark_idle()` | Exclude this input from downstream minimum-watermark calculation. |
| `mark_active(resume_watermark=None)` | Reactivate before emitting after an idle period. |

`ray.klein.source` registration options are:

| Argument | Default | Meaning |
|---|---:|---|
| `fn` | Required | `SourceFunction` class; instances are rejected. |
| `fn_constructor_args`, `fn_constructor_kwargs` | `None` | Arguments used to construct one source per subtask. |
| `lowering` | `None` | Advanced declarative Ray Data source lowering for batch support. |
| `num_cpus`, `num_gpus` | `None` | Effective defaults are 1 CPU and 0 GPUs. |
| `concurrency` | `None` | Effective parallelism 1; an integer or `(min, max)` range is accepted. |
| `name` | `None` | Logical operator name. |
| `bounded` | `False` | Whether the source is finite; independent of batch support. |
| `changelog_mode` | `None` | Optional non-empty set of emitted `RowKind` values. |

Source snapshot state must be pickleable. `notify_checkpoint_complete` is
at-least-once across coordinator recovery, so use the checkpoint ID as an
idempotency key when committing offsets externally. `cancel` should only signal
the run loop; release clients, files, and threads in `close`.

## Implement a sink

Subclass `SinkFunction` for an ordinary streaming sink:

```python
from ray.klein import SinkFunction


class ClientSink(SinkFunction):
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.client = None

    def open(self, runtime_context):
        self.client = make_client(self.endpoint)

    def write(self, value):
        self.client.send(value)

    def flush(self):
        self.client.flush()

    def close(self):
        if self.client is not None:
            self.client.close()


stream.write(
    ClientSink,
    fn_constructor_args=["https://sink.internal"],
    concurrency=4,
    name="client-output",
)

ray.klein.execute("custom-source").wait()
```

`DataStream.write` accepts the same constructor, resource, concurrency, and
name options. It additionally accepts operator `batch_size` (default `None`)
and `batch_timeout` (default 3 seconds), plus an advanced `lowering` for Ray
Data batch support. Leave the internal `node_type` override unset in connector
code.

`write(value)` receives one mapping. `flush()` is called where buffered output
must be drained, including checkpoint alignment. Connector clients should be
constructed in `open`, not serialized from the driver. A normal `SinkFunction`
does not gain exactly-once semantics merely by flushing at a barrier; document
the external system's replay and idempotency behavior.

## Implement a two-phase-commit sink

Use `TwoPhaseCommitSinkFunction` only when prepared output can remain private
until a global checkpoint is durable. In addition to `write`, implement:

- `prepare_commit(checkpoint_id)`, which closes the current transaction, starts
  a fresh one, and returns a pickleable `SinkCommittable` or `None` for no data;
- `abort_current_transaction()`, which discards writer-local unprepared data.

A `SinkCommittable` supplies a stable string `transaction_id` and idempotent
`commit()` and `abort()` methods. It must store only enough serializable
information to reconnect and publish or discard the prepared transaction; do
not capture clients, locks, threads, or open handles. The coordinator persists
the committable before calling `commit` and may retry either terminal method
after recovery. The [filesystem connector](filesystem.md) is the reference
implementation.

## Add a Table DDL connector

Subclass `TableFactory`, set a non-empty `identifier`, and implement only the
directions the connector supports:

| Method | Responsibility |
|---|---|
| `validate(table)` | Reject missing, malformed, or unknown connector options eagerly. |
| `create_source(context, table)` | Convert catalog metadata to a `DataStream`. |
| `create_sink(stream, table)` | Bind a stream to connector output. |
| `supported_sink_row_kinds(table)` | Declare accepted changelog kinds; default is insert-only. |

Register an instance in one SQL session:

```python
ray.klein.register_table_factory(MyTableFactory())
```

For an installable connector package, publish a Python entry point in the
`ray.klein.table_factories` group. The entry point may resolve to a factory
instance or class. Klein discovers these alongside the built-in `filesystem`,
`kafka`, and `print` factories. Duplicate identifiers fail unless a
session-local registration explicitly uses `replace=True`.

See [SQL and Table DDL](../sql.md) for catalog behavior and
[Testing](../testing.md) for connector test expectations.
