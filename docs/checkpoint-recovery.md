---
myst:
  html_meta:
    description: "Restore, resubmit, and rescale Klein for Ray jobs from durable checkpoints."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Restore and rescale a job

This guide covers two recovery paths:

- an in-place worker or coordinator restart inside the same live job; and
- a new submission after the JobManager, Ray head node, or entire cluster was
  lost.

In-place recovery automatically uses the current job's latest completed
checkpoint. A new submission needs the explicit URI of one completed `chk-N`
directory in `execution.savepoint.path`.

## Prepare before failure

Use shared durable storage and retain more than one checkpoint while operating
an important job:

```python
import ray

ray.klein.reset_context({
    "execution.runtime.mode": "streaming",
    "execution.checkpointing.dir": "s3://data-platform/klein-checkpoints",
    "execution.checkpointing.num-retained": 3,
    "state.keyed.max-parallelism": 32768,
})
```

Keep the following with the deployment definition:

- source topics, consumer identities, and start-position policy;
- the graph-building code and stable operator names;
- `state.keyed.max-parallelism`;
- serializer, schema, and connector versions;
- checkpoint storage URI and credential mechanism;
- output idempotency or deduplication key.

Node-local RocksDB and Ray Object Store snapshots accelerate live recovery but
cannot recover a lost cluster.

## Select a completed checkpoint

A restorable directory contains `_metadata`:

```text
s3://data-platform/klein-checkpoints/
└── klein-orders-0123abcd/
    ├── chk-41/_metadata
    ├── chk-42/_metadata
    └── _latest
```

Prefer the newest `chk-N` whose `_metadata` is readable. `_latest` is only an
optimization; Klein falls back to scanning completed checkpoint directories
when the pointer is missing, stale, or corrupt. Do not select an incomplete
directory that lacks `_metadata`.

Job snapshots and checkpoint logs expose recent completion and failure history.
They do not copy durable checkpoint payloads into the state actor.

## Resubmit from the checkpoint

Recreate the same logical graph and pass the complete checkpoint-directory URI:

```python
import ray

ray.init(address="auto")
ctx = ray.klein.reset_context({
    "execution.runtime.mode": "streaming",
    "execution.checkpointing.dir": "s3://data-platform/klein-checkpoints",
    "execution.savepoint.path": (
        "s3://data-platform/klein-checkpoints/"
        "klein-orders-0123abcd/chk-42"
    ),
    "state.keyed.max-parallelism": 32768,
})

# Build the same sources, transforms, operator names, and sinks here.
handle = ctx.execute("orders-restored")
print(handle.namespace)
handle.wait()
```

The restore path is intentionally singular: it names one completed checkpoint,
not the checkpoint root. Configuration accepts unknown keys for application
metadata, so a misspelling such as `execution.checkpointing.restore-path` is a
silent no-op. Use the canonical `execution.savepoint.path` key.

When resubmitting on the same cluster, stop the old job first. A stable
`job.namespace` is useful for operational attachment, but reusing a namespace
while its old detached actors still exist can address the wrong job. After a
whole-cluster loss the old actors no longer exist.

## Change parallelism safely

Managed keyed state is partitioned into a fixed key-group space. You may change
the concurrency of keyed operators during restore when all of these remain
true:

1. `state.keyed.max-parallelism` is unchanged and is not below the new
   concurrency.
2. The same logical operators, stable names, state descriptors, and serializers
   are reconstructed.
3. Source and sink connectors support the new parallelism. For example, Kafka
   source concurrency above the useful partition count adds idle subtasks.
4. External systems tolerate any changed connection or transaction fan-out.

Klein assigns each restored subtask a contiguous key-group range and loads only
the fragments it now owns. Keys are not rehashed with Python's process-local
`hash()`.

Never change max parallelism for a job whose checkpoint must remain
restorable. Treat state descriptor names and serialized value shapes as schema.

## Validate the restored job

Before returning traffic or trusting output, check:

- the job reaches `RUNNING` rather than repeatedly entering recovery;
- the first completed checkpoint after restore succeeds;
- `state_durable_restore_fallbacks` and restore-duration metrics match the
  expected recovery path;
- Kafka lag or another source-position metric resumes near the checkpoint;
- no unexpected late-record or sink-commit failures appear;
- output deduplication absorbs the accepted replay window.

Use `ray-klein status <namespace>` for a summary and
`ray-klein attach <namespace>` for live progress.

## Common restore failures

| Symptom | Likely cause | Action |
|---|---|---|
| `_metadata` missing | Incomplete checkpoint publication | Select an earlier completed `chk-N`. |
| Unsupported checkpoint format | Code and checkpoint schema are incompatible | Run the matching Klein version or a deliberate migration tool; do not edit pickled metadata. |
| State checksum/size mismatch | Missing or corrupt state object | Verify object-store consistency and restore an earlier retained checkpoint. |
| Missing operator state | Graph identity or operator construction order changed | Rebuild the original logical graph and stable names. |
| Max-parallelism mismatch | `state.keyed.max-parallelism` changed | Restore with the original value. |
| Connector cannot seek/restore | Source recovery is broker-owned or custom state is incomplete | Follow that connector's documented recovery boundary. |
| Output duplicates | Sink commit succeeded before the restored checkpoint became durable | Apply the documented idempotency/deduplication design. |

Klein does not yet expose a user-triggered command that creates an independently
named savepoint. `execution.savepoint.path` restores from an already completed
checkpoint or compatible savepoint URI.
