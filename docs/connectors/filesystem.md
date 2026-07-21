---
myst:
  html_meta:
    description: "Read files with Ray Data and write checkpoint-transactional JSON, CSV, Parquet, and text files with Klein for Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-filesystem-connector)=
# Filesystem

Filesystem input uses public Ray Data readers and is bounded. Klein's stable
output API writes JSON, CSV, and Parquet in either execution mode and text in
streaming mode. Native streaming output participates in Klein checkpoints so
uncommitted part files are not exposed as completed output.

## Read files

Use any public Ray Data file reader exposed by the installed compatible Ray
version:

```python
import ray
import ray.klein

events = ray.klein.read_parquet(
    "s3://warehouse/events/",
    columns=["event_id", "payload"],
)
```

Common readers include `read_csv`, `read_json`, `read_parquet`, and
`read_text`. Their options, schemas, file discovery, and retry semantics are
owned by Ray Data; see [Ray Data](ray-data.md). Filesystem input has no native
continuous directory watcher.

## Write files

```python
events.write_parquet(
    "s3://warehouse/normalized-events/",
    filename_prefix="events",
    max_rows_per_file=1_000_000,
    max_bytes_per_file=128 * 1024 * 1024,
    concurrency=8,
)
```

Call `write_json`, `write_csv`, `write_parquet`, `write_text`, or the common
`write_files(path, data_format, ...)` method.

| Argument | Default | Meaning |
|---|---:|---|
| `path` | Required | Output directory on a local, shared, or object filesystem. |
| `data_format` | Required to `write_files` | `json`, `csv`, `parquet`, or `text`. |
| `columns` | `None` | Explicit column order; text requires exactly one column. |
| `storage_options` | `None` | PyArrow filesystem construction options for native streaming. |
| `filename_prefix` | `"part"` | Prefix for final part filenames. |
| `max_rows_per_file` | `None` | Positive row-count rolling threshold. |
| `max_bytes_per_file` | `None` | Positive encoded-byte rolling threshold. |
| `rollover_interval` | `None` | Maximum duration a part remains open. |
| `inactivity_interval` | `None` | Close an inactive part at the next write or checkpoint. |
| `ray_remote_args` | `None` | Ray Data batch options; `num_cpus` and `num_gpus` also size native sink tasks. |
| `concurrency` | `None` | Batch writer concurrency or native sink parallelism. |
| `ray_data_options` | `None` | Extra options forwarded only to the bounded Ray Data writer. |

Streaming records must be mappings. JSON is newline-delimited; every CSV part
contains its own header. Text output encodes the single selected column as
UTF-8. JSON, CSV, and Parquet lower to their Ray Data writers in batch; text
does not have a Klein batch lowering and therefore requires streaming mode.
In batch, only `path`, `data_format`, `ray_data_options`, `ray_remote_args`, and
`concurrency` affect the Ray Data write. Column order, storage options, filename
prefix, and rolling policies are native-streaming settings and are not
forwarded to Ray Data.

## Understand the streaming commit lifecycle

Each streaming sink subtask owns a unique attempt directory:

```text
{output}/
├── .klein-staging/{job-id}/{subtask}-{attempt}/
│   ├── .part-...inprogress
│   └── .part-...pending
└── part-{subtask}-{attempt}-{sequence}.{extension}
```

At an aligned checkpoint barrier, the writer closes its in-progress part and
returns a serializable committable. The coordinator then:

1. persists source state, operator state, and sink committables in checkpoint
   metadata;
2. publishes pending parts to final paths with idempotent commits;
3. notifies checkpoint-aware sources that the checkpoint is durable.

Recovery reloads persisted committables and retries publication. An already
published target completes successfully instead of producing another part.
Klein aborts prepared files from an abandoned checkpoint when possible.

This provides **exactly-once file visibility relative to Klein checkpoints**.
Use durable shared storage for both `execution.checkpointing.dir` and output.
Force termination or an unreachable object store can leave hidden staging
objects, but readers that ignore `.klein-staging` do not treat them as output.

## Configure rolling

Every checkpoint rolls the active part. You may roll earlier by row count,
encoded bytes, elapsed time, or inactivity:

```python
from datetime import timedelta

events.write_json(
    "s3://warehouse/events-json/",
    max_rows_per_file=1_000_000,
    max_bytes_per_file=128 * 1024 * 1024,
    rollover_interval=timedelta(minutes=15),
    inactivity_interval=timedelta(minutes=1),
)
```

Rolling prepares a private file; publication still waits for a successful
checkpoint. Time policies are evaluated on writes and at the next checkpoint,
not by a separate per-part timer.

## Use Table DDL

The connector identifier is `filesystem`:

```sql
CREATE TABLE output_events (
    event_id BIGINT,
    payload STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://warehouse/events',
    'format' = 'parquet',
    'sink.parallelism' = '8',
    'sink.filename-prefix' = 'events',
    'sink.rolling-policy.file-size' = '128 MiB',
    'sink.rolling-policy.rollover-interval' = '15 min'
);
```

`path` and `format` are required. Source options use a `source.` prefix and are
forwarded to the matching Ray Data reader, for example
`source.override_num_blocks`. Sink options are:

| Table option | Default | Meaning |
|---|---:|---|
| `sink.filename-prefix` | `part` | Final filename prefix. |
| `sink.max-rows-per-file` | None | Positive row-count threshold. |
| `sink.parallelism` | None | Positive native sink parallelism. |
| `sink.rolling-policy.file-size` | None | Bytes or a size such as `128 MB` or `128 MiB`. |
| `sink.rolling-policy.rollover-interval` | None | Positive duration such as `15 min`. |
| `sink.rolling-policy.inactivity-interval` | None | Positive inactivity duration. |
| `sink.storage-options` | None | JSON object for the native PyArrow filesystem. |
| `sink.ray-data-options` | None | JSON object forwarded only to bounded output. |

Supported DDL formats are `csv`, `json`, `parquet`, and `text`. A text sink
table must have exactly one column. See [SQL and Table DDL](../sql.md) for full
statement behavior.

## Credentials and operations

Prefer workload identity, instance roles, or the filesystem client's standard
credential chain over secrets in `storage_options`. Checkpoint metadata stores
serialized transactions, so do not put non-serializable filesystem clients in
connector options. Monitor checkpoint duration and failures as described in
[Observability](../observability.md).
