---
myst:
  html_meta:
    description: "Persist Klein for Ray checkpoints to local filesystems, S3-compatible storage, or Google Cloud Storage."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Durable checkpoint storage

Klein keeps mutation-heavy keyed state in task-local RocksDB, optionally caches
large immutable barrier snapshots in Ray's Object Store, and persists completed
checkpoints to a durable filesystem or object store. Ray object spilling is a
capacity mechanism; it is not a replacement for checkpoints.

## Configure checkpoint storage

`execution.checkpointing.dir` accepts a local path, `file://` URI, or any
checkpoint filesystem supported by PyArrow. Klein explicitly supports S3 and
Google Cloud Storage, including S3-compatible stores through
`execution.checkpointing.storage-options`.

```python
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.configuration import Configuration

config = Configuration()
config.set(
    CheckpointOptions.DIRECTORY,
    "s3://data-platform/klein-checkpoints",
)
config.set(
    CheckpointOptions.STORAGE_OPTIONS,
    {
        "region": "ap-southeast-1",
        # For MinIO or another S3-compatible service:
        # "endpoint_override": "minio.example.com:9000",
        # "scheme": "https",
    },
)
config.set(CheckpointOptions.RETAINED_COUNT, 3)
```

PyArrow's normal credential chain is used when explicit storage options are
absent. Prefer workload identity, instance roles, or environment-managed
credentials instead of embedding secrets in application configuration.

## How are checkpoints organized?

Every job has an isolated directory below the configured root:

```text
{checkpoint-root}/
└── {job-id}/
    ├── shared/
    │   └── op-{operator-id}/kg-{key-group}/sha256-{digest}.bin
    ├── taskowned/
    │   └── op-{operator-id}/kg-{key-group}/state-v{version}-{digest}.bin
    ├── chk-1/
    │   ├── op-{task-id}/managed-state-{digest}.bin
    │   ├── op-{operator-id}/kg-{key-group}/state-v{version}-{digest}.bin
    │   └── _metadata
    ├── chk-2/
    │   └── _metadata
    └── _latest
```

- `chk-N/` is durable metadata revision `N`. A revision contains the latest
  completed state known when it was published; source barrier IDs remain part
  of that metadata and are not reused as directory identifiers.
- `shared/` contains content-addressed state reusable by multiple checkpoints.
- `taskowned/` contains state whose lifecycle is owned by a task rather than
  ordinary checkpoint retention.
- `chk-N/_metadata` is the only checkpoint completion marker.
- `_latest` accelerates lookup but is not authoritative. Recovery falls back
  to scanning readable `chk-N/_metadata` objects when the pointer is stale,
  missing, or corrupt.

Job and operator identifiers are percent-encoded as individual path components
so user-provided names cannot escape the checkpoint root.

## How does Klein publish and recover a checkpoint?

Klein writes immutable state objects first and publishes `_metadata` last. A
failure before that final write leaves an incomplete directory that recovery
ignores. On local filesystems metadata publication uses a temporary file and an
atomic rename. On S3/GCS it uses one final object PUT; Klein does not emulate a
rename because object-store rename is a copy-and-delete operation.

Checkpoint metadata also carries prepared two-phase sink committables. The
coordinator publishes those transactions only after `_metadata` is durable and
replays an idempotent commit after recovery when publication was interrupted.
Checkpoint-aware source offsets are notified after sink publication, preserving
the end-to-end ordering between input progress and output visibility.

State entries carry their serialized size and SHA-256 checksum. Recovery checks
both before deserializing a value. Retention deletes only old `chk-N/`
directories and leaves `shared/` and `taskowned/` intact for their separate
lifecycles. Checkpoint metadata has one current schema version; incompatible
pre-revision metadata is rejected explicitly instead of being guessed or
silently upgraded during recovery.

The layout follows Flink's filesystem checkpoint storage conventions:
[checkpoint storage](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/)
and [`_metadata` completion marker](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/runtime/state/filesystem/AbstractFsCheckpointStorageAccess.html).
