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
from ray.klein.config import StateOptions

config.set(StateOptions.BACKEND, "rocksdb")
config.set(StateOptions.LOCAL_DIRECTORY, "/mnt/nvme/klein-state")
config.set(StateOptions.OBJECT_STORE_CACHE_ENABLED, True)
config.set(StateOptions.OBJECT_STORE_CACHE_MIN_BYTES, 4 * 1024 * 1024)
config.set(StateOptions.TTL_CLEANUP_BATCH_SIZE, 1000)
config.set(StateOptions.MAX_PARALLELISM, 128)
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
[Event time and idle inputs](event-time.md). Savepoints remain future work.

`ObjectStoreStateBackend` remains available as a low-level immutable MVCC
primitive for coarse state partitions. The Object Store is used where its
immutable-object model helps snapshots; it is not forced into the hot
per-record mutation path.

## References

- [Ray object fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html)
- [Ray object spilling](https://docs.ray.io/en/latest/ray-core/internals/object-spilling.html)
- [Flink state backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
