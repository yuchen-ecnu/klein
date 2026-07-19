---
myst:
  html_meta:
    description: "Use Klein for Ray managed keyed state, RocksDB, key-group rescaling, TTL, timers, and Object Store checkpoint acceleration."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Managed state with Ray-native snapshot acceleration

## Choose a state backend

Klein uses a hybrid state design:

- The in-memory backend is the dependency-free default and is appropriate for
  moderate state that is checkpointed frequently. RocksDB is an optional hot
  keyed-state backend for larger state. Point reads, writes, TTL indexes,
  namespaces, and timers stay local to the operator task in either case.
- Ray's Object Store is an acceleration layer for large immutable snapshots.
  A barrier snapshot smaller than the configured threshold stays inline; a
  larger snapshot is stored once with `ray.put` and the coordinator pins the
  nested `ObjectRef` until it is durably checkpointed or superseded.
- External checkpoint storage is the durability boundary. Local paths,
  S3-compatible storage, and GCS use the
  [Flink-style checkpoint layout](checkpoint-storage.md). Object spilling is a
  capacity feature, not a durable recovery protocol.

This design uses zero-copy sharing, distributed reference counting, spilling, and owner-local recovery without adding a distributed object round trip to the per-record state path.

## How does managed state work?

`DataStream.key_by()` creates a keyed branch and installs a `KeyPartitioner`.
Stateful operators then use a backend-neutral API:

- `ValueState`, `ListState`, and `MapState`, each created from a descriptor;
- key plus namespace isolation, used by windows;
- per-descriptor TTL with create/write or read/write refresh;
- ordered, deduplicated event-time and processing-time timers.

`MemoryStateBackend` implements the complete state contract. The optional
`RocksDBStateBackend` stores state, expiry indexes, timers, and metadata in
separate column families through `rocksdict`.

Key ownership is stable across Python processes and parallelism changes. Klein
hashes serialized keys into a fixed key-group space defined by
`state.keyed.max-parallelism`, then assigns contiguous key-group ranges to the
current subtasks. Python's process-salted `hash()` is never used. The maximum
parallelism is part of checkpoint compatibility and must not change on restore.

:::{important}
Set `state.keyed.max-parallelism` before the first checkpoint. Don't change it when you restore or rescale that job.
:::

```python
from datetime import timedelta

from ray.klein import KeyedProcessFunction
from ray.klein.state import StateTTLConfig, ValueStateDescriptor


class RunningTotal(KeyedProcessFunction):
    total = ValueStateDescriptor(
        "total",
        ttl_config=StateTTLConfig(timedelta(hours=1)),
    )

    def process(self, row, context):
        state = context.state(self.total)
        total = (state.value or 0) + row["amount"]
        state.value = total
        return {"customer_id": row["customer_id"], "total": total}


totals = orders.key_by(lambda row: row["customer_id"]).process(RunningTotal())
```

### Choose a state handle

Descriptors are stable state schema. Give every logical value a unique,
unchanging name and declare descriptors on the function class rather than
creating a new descriptor per record.

| Descriptor | Returned handle | Operations |
|---|---|---|
| `ValueStateDescriptor` | `ValueState` | Read/write `.value`; `None` means absent; `clear()` deletes it. |
| `ListStateDescriptor` | `ListState` | Standard mutable-sequence operations such as `append`, `extend`, indexing, deletion, and `clear`. |
| `MapStateDescriptor` | `MapState` | Standard mutable-mapping operations such as item get/set/delete, `update`, iteration, and `clear`. |

Every handle is isolated by the current key and an optional namespace. Windows
use the window object as a namespace, so two windows for the same key do not
share an aggregate.

### Configure state TTL

```python
from datetime import timedelta

from ray.klein.state import (
    StateTTLConfig,
    StateTTLUpdateType,
    StateVisibility,
    ValueStateDescriptor,
)

profile = ValueStateDescriptor(
    "profile",
    ttl_config=StateTTLConfig(
        timedelta(hours=6),
        update_type=StateTTLUpdateType.ON_READ_AND_WRITE,
        visibility=StateVisibility.NEVER_RETURN_EXPIRED,
    ),
)
```

`ON_CREATE_AND_WRITE` refreshes expiry only on writes;
`ON_READ_AND_WRITE` also refreshes it on a successful read.
`NEVER_RETURN_EXPIRED` treats an expired value as absent even before background
cleanup deletes its bytes. `RETURN_EXPIRED_IF_NOT_CLEANED_UP` can expose such a
value until cleanup runs and should be used only when that weaker visibility is
intentional. Cleanup processes at most `state.ttl.cleanup.batch-size` entries
after one operator input.

TTL uses processing time. It is independent of event-time watermarks and can
make results incomplete if a key returns after expiration.

### Register timers

Processing-time timers fire from worker wall-clock progress. Event-time timers
fire only when the aggregate input watermark reaches their timestamp. Timer
timestamps are integer milliseconds and are deduplicated by key, namespace,
domain, and timestamp.

```python
from datetime import timedelta

from ray.klein import KeyedProcessFunction
from ray.klein.state import ValueStateDescriptor


class InactivityAlert(KeyedProcessFunction):
    deadline = ValueStateDescriptor("inactivity-deadline")
    timeout_ms = int(timedelta(minutes=10).total_seconds() * 1000)

    def process(self, row, context):
        state = context.state(self.deadline)
        if state.value is not None:
            context.timer_service.delete_event_time_timer(state.value)
        deadline = row["event_time_ms"] + self.timeout_ms
        state.value = deadline
        context.timer_service.register_event_time_timer(deadline)

    def on_timer(self, event, context):
        state = context.state(self.deadline)
        if state.value != event.timestamp:
            return None
        state.clear()
        return {"customer_id": context.current_key, "inactive_at": event.timestamp}


alerts = orders.key_by(lambda row: row["customer_id"]).process(
    InactivityAlert(),
    timestamp_selector=lambda row: row["event_time_ms"],
)
```

Pass a `KeyedProcessFunction` instance or a plain two-argument callable to
`process()`. Use a class when `on_timer()` behavior is required. Timer state and
the operator watermark are included in managed-state checkpoints.

## Use stateful operators

The streaming runtime provides:

- keyed process functions with managed state and timers;
- tumbling, sliding, and session event-time windows;
- two-stream keyed interval joins with independent key and timestamp selectors;
- state TTL and incremental expiry cleanup for all managed state descriptors.

Window and join APIs accept a `state_ttl` safety bound. Interval joins also
evict records once their event-time bounds can no longer match.

```python
from datetime import timedelta
from ray.klein import TumblingWindow

hourly = (
    orders.key_by(lambda row: row["customer_id"])
    .window(
        TumblingWindow(timedelta(hours=1)),
        timestamp_selector=lambda row: row["event_time_ms"],
        state_ttl=timedelta(days=1),
    )
    .reduce(lambda left, right: {
        "customer_id": left["customer_id"],
        "amount": left["amount"] + right["amount"],
        "event_time_ms": right["event_time_ms"],
    })
)
```

## How does a barrier snapshot work?

When a barrier aligns at a stateful task:

1. the task flushes pre-barrier output;
2. the backend exports state, TTL indexes, and timers as portable logical
   key-group fragments and Klein serializes the operator watermark with them;
3. the snapshot stays inline or is cached in the Ray Object Store according to
   `state.checkpoint.object-store-cache.min-bytes`;
4. the task registers the immutable reference before forwarding the barrier;
5. after the sink acknowledgement, the coordinator promotes those references
   to the latest hot recovery point;
6. periodic persistence materializes them under `chk-N/op-*/`, verifies size
   and SHA-256, and publishes `_metadata` last.

On a task restart the current hot reference is preferred. If the coordinator,
owner, or cluster was lost, the task restores the latest durable snapshot.
Checkpoint retention releases old exclusive blobs; normal Ray reference
counting releases superseded hot snapshots.

On rescale, every prior fragment for the logical operator is available through
the coordinator. A new subtask selects only the key groups in its newly assigned
range and merges those groups into an empty local backend. Scale-up and
scale-down therefore preserve keyed state, TTL indexes, and timers without
rehashing keys or depending on the previous subtask count.

## Configure managed state

```python
from ray.klein import Configuration
from ray.klein.config import StateOptions

config = Configuration()

config.set(StateOptions.BACKEND, "rocksdb")
config.set(StateOptions.LOCAL_DIRECTORY, "/mnt/nvme/klein-state")
config.set(StateOptions.OBJECT_STORE_CACHE_ENABLED, True)
config.set(StateOptions.OBJECT_STORE_CACHE_MIN_BYTES, 4 * 1024 * 1024)
config.set(StateOptions.TTL_CLEANUP_BATCH_SIZE, 1000)
config.set(StateOptions.MAX_PARALLELISM, 32768)
```

Install `ray-klein[rocksdb]` before selecting the RocksDB backend.

Put the RocksDB working directory on node-local SSD. It is disposable working
state; durable checkpoints belong on replicated filesystem or object storage.

## Understand the consistency boundary

Klein's existing progress protocol is at-least-once and allocates barriers per
source. Managed-state snapshots participate in that same protocol. Arbitrary
non-transactional sinks are therefore not exactly-once. Event-time timers
advance only from explicit aggregate watermarks; record timestamps alone never
move event time. Watermark and idle-input semantics are described in
[Event time and idle inputs](event-time.md). Klein can restore a new submission
from an existing completed checkpoint through `execution.savepoint.path`.
Creating a named, user-triggered savepoint independently of the periodic
checkpoint lifecycle remains future work; see
[Restore and rescale a job](checkpoint-recovery.md).

`ObjectStoreStateBackend` remains available as a low-level immutable MVCC
primitive for coarse state partitions. The Object Store is used where its
immutable-object model helps snapshots; it is not forced into the hot
per-record mutation path.

## References

- [Ray object fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html)
- [Ray object spilling](https://docs.ray.io/en/latest/ray-core/internals/object-spilling.html)
- [Flink state backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
