---
myst:
  html_meta:
    description: "Complete Klein configuration reference with every option, type, default, constraint, and environment variable."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-configuration-reference)=
# Configuration reference

This page lists every `ConfigOption` declared by Klein. The key column contains
the canonical key accepted by mappings and `key=value` strings. Every key also
has an environment-variable form: add the `RAY_KLEIN_` prefix, replace dots and
hyphens with underscores, and convert the result to upper case. For example,
`execution.runtime.mode` becomes `RAY_KLEIN_EXECUTION_RUNTIME_MODE`.

See [Configure Klein](configuration.md) for source precedence, input forms,
type conversion, and context isolation.

## Configuration groups

Use the option prefix to find the subsystem that owns a setting. Most jobs
need only a small subset; start with execution mode, durable checkpoint
storage, state backend, and the job namespace, then tune buffers or placement
only after observing a specific bottleneck.

| Prefix | Controls |
| --- | --- |
| `execution.*` | Runtime selection, task deployment, restart policy, checkpoints, and restore. |
| `job.*` | Submission, startup, health checks, shutdown, and Ray namespace identity. |
| `pipeline.*` | Backpressure buffers, micro-batching, transport, placement, and replay. |
| `state.*` | Managed-state backend, local working data, snapshot caching, TTL cleanup, and key groups. |
| `event-time.*` | Event-time progress and idle-input detection. |
| `table.*` | Stateful Table and continuous SQL execution. |
| `sql.download.*` | URI, network, redirect, timeout, and byte limits for SQL `DOWNLOAD`. |
| `observability.*` | Dashboard publication and retained job history. |
| `serve.*` | Ray Serve execution-region clients, batching, connections, timeouts, and retries. |
| `udf.*` | User-function failure behavior. |
| `partitioner.*` | Compatibility-only adaptive partitioner settings. |

## How to read the tables

- `duration` values are Python `datetime.timedelta` objects in typed code. In
  mappings, strings, and environment variables, use a number followed by `ms`,
  `s`, `min`, `h`, `d`, or `w`, such as `500ms`, `30s`, or `1.5h`. An
  unquoted numeric value in a mapping or `key=value` input is interpreted as
  seconds; environment-variable durations need a unit.
- Enum values are case-insensitive. The tables show them in their canonical
  lower-case form.
- `None` means the feature has no configured value; it is different from an
  empty string, zero, or an empty mapping.
- Paths under `<temp-dir>` use the operating system's temporary directory and
  therefore vary by host.
- Unless a row says otherwise, an option is read when a job is compiled or its
  streaming runtime component starts. Configure the context before calling
  `execute()`.

:::{note}
Klein currently accepts arbitrary canonical keys in a `Configuration`, but
only the options below are read by Klein. Unknown keys are retained by
`to_dict()` and may appear in the dashboard, but they don't change runtime
behavior.
:::

## Execution mode and task deployment

Execution mode is job-wide. `auto` chooses one backend for the complete graph;
an explicit mode fails when any graph vertex cannot run on that backend.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `execution.runtime.mode` | `auto` | enum: `auto`, `batch`, `streaming` | Selects the execution engine. `auto` uses streaming when any source is unbounded, any graph vertex has no batch lowering, or `udf.ignore-exception=true`; otherwise it uses batch. Explicit modes must still support every vertex. |
| `execution.task.deployment.mode` | `default` | enum: `default`, `balanced` | Selects streaming-task placement. `default` tries a placement group, then round-robin placement, then native Ray placement. `balanced` skips the placement-group attempt. |

## Restart strategy

These settings are read by the native streaming scheduler. They do not apply
to a bounded graph that runs entirely on Ray Data.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `execution.restart-strategy.fixed-delay.attempts` | `3` | int | Maximum restarts allowed inside the count window. Must be at least `0`; `0` suppresses the first restart. |
| `execution.restart-strategy.fixed-delay.delay` | `10s` | duration | Delay before each restart. Must be non-negative. |
| `execution.restart-strategy.fixed-delay.count-interval` | `10min` | duration | Sliding window used to count restart attempts. Must be greater than zero. |

## Checkpointing

Checkpoint settings govern barrier creation, coordinator limits, durable
storage, retention, and restore. A bounded graph that runs entirely on Ray
Data does not create Klein checkpoint components.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `execution.checkpointing.trigger.interval-duration` | `60s` | duration | Maximum wall-clock time between source checkpoint barriers. `0` disables the time trigger. |
| `execution.checkpointing.trigger.interval-records` | `512` | int | Maximum records emitted by a source between checkpoint barriers. `0` disables the record trigger. Whichever trigger fires first resets both intervals. |
| `execution.checkpointing.persistence-interval` | `600` | int, seconds | Interval for persisting checkpoint-coordinator metadata. Must be at least `0`; `0` disables periodic persistence. Completion paths can still persist metadata. |
| `execution.checkpointing.max-concurrent-checkpoints` | `100` | int | Maximum checkpoint attempts that may be in flight. Must be at least `1`. |
| `execution.checkpointing.timeout` | `600` | int, seconds | Maximum time an in-flight checkpoint or aligned completion RPC/storage phase may take. Must be at least `0`; `0` disables alignment expiry, while completion phases retain a 30-second safety deadline. |
| `execution.checkpointing.max-history-size` | `100` | int | Maximum checkpoint-history entries retained in coordinator memory. Must be at least `1`. |
| `execution.checkpointing.async-notify` | `false` | bool | If `true`, committers send checkpoint-complete notifications without waiting for the coordinator acknowledgement and reap or retry them at later barriers. |
| `execution.checkpointing.dir` | `<temp-dir>/klein/checkpoint` | string | Durable checkpoint root. Supports local paths, `file://`, `s3://`, and `gs://`. Use shared durable storage for recovery across nodes. |
| `execution.checkpointing.storage-options` | `None` | mapping or `None` | Keyword arguments passed to PyArrow `S3FileSystem` or `GcsFileSystem`. Accepted only when the checkpoint URI uses `s3://` or `gs://`. |
| `execution.checkpointing.num-retained` | `1` | int | Number of completed `chk-N` directories retained per job. Must be at least `1`. |
| `execution.savepoint.path` | `None` | string or `None` | Checkpoint or savepoint path from which the job restores at submission. |

Setting both checkpoint trigger intervals to `0` prevents sources from emitting
periodic checkpoint barriers. This also delays checkpoint-driven source offset
durability and two-phase-commit sink completion; use that combination only when
you intentionally don't need periodic recovery points.

## Job lifecycle

Lifecycle timeouts are independent budgets. A deployment can consume several
per-step scheduler budgets while still remaining inside `job.deploy.timeout`.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `job.scheduler.start.timeout` | `300` | int, seconds | Per-step limit for starting workers and for startup-heavy coordinator operations such as checkpoint restoration. |
| `job.deploy.timeout` | `600` | int, seconds | Total time budget for coordinator initialization, worker scheduling, and coordinator start. |
| `job.stop.timeout` | `60` | int, seconds | Total time budget for stopping the supervisor, workers, and coordinator. It is also the default `cancel()` budget. |
| `job.coordinator.rpc.timeout` | `30` | int, seconds | Limit for lightweight coordinator RPCs such as health probes, metadata flush, and stop. |
| `job.healthcheck.interval` | `15` | int, seconds | Interval between `JobManager` health checks. |
| `job.namespace` | `""` | string | Ray namespace used for this job's named actors. An empty string generates a unique `klein-<job>-<id>` namespace. Set a stable value only when another client or operations tool must attach to the same actors. |

## Buffers and backpressure

These are hard per-task or per-edge bounds for the native streaming data
plane. Increase them only when metrics show sustained capacity pressure and
the Ray workers have enough memory for the larger worst-case footprint.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `pipeline.input-buffer.size` | `200` | int | Maximum logical rows queued in each streaming task inbox. Must be positive. A single oversized columnar block is admitted only while the inbox is otherwise empty. |
| `pipeline.input-buffer.max-bytes` | `67108864` (64 MiB) | int | Maximum estimated payload bytes shared by each task inbox, dequeue handoff, and input batcher. Must be positive. One oversized block is admitted exclusively so progress remains possible. |
| `pipeline.input-buffer.put-timeout` | `1s` | duration | Compatibility timeout for targets without immediate admission support. Native tasks use non-blocking capacity probes and backoff. |
| `pipeline.output-buffer.max-rows` | `1000` | int | Hard per-edge bound on logical rows retained before transfer to the emit queue. Exceeding it fails fast instead of growing task memory without limit. |
| `pipeline.output-buffer.max-bytes` | `67108864` (64 MiB) | int | Hard per-edge estimated-byte bound before transfer to the emit queue. One oversized block is allowed only when exclusive. |
| `pipeline.emit-queue.max-batches` | `2` | int | Maximum detached output batches waiting in the FIFO emit queue. Must be positive. |

## Batching and data transport

Batch controls trade latency for throughput. Row and byte limits are evaluated
together; the first reached limit flushes the batch.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `pipeline.internal.batch-size` | `10` | int | Records accumulated per downstream target before a micro-batch is emitted. Must be non-negative; `0` effectively emits each record immediately. |
| `pipeline.internal.batch-max-rows` | `1000` | int | Flushes a transport micro-batch once it reaches this many logical rows. Must be positive. |
| `pipeline.internal.batch-max-bytes` | `4194304` (4 MiB) | int | Flushes a transport micro-batch once its estimated payload reaches this size. Must be positive. |
| `pipeline.transport.object-store-threshold-bytes` | `131072` (128 KiB) | int | Duplicated broadcast batches at or above this size use one shared Ray Object Store reference. `0` shares every duplicated batch. |
| `pipeline.operator-chaining.enabled` | `true` | bool | Co-locates compatible non-shuffle operators in one task to avoid serialization between them. |
| `pipeline.columnar-passthrough.enabled` | `true` | bool | Coalesces Arrow-compatible inter-task data into `RecordBatch` objects while keeping chained operators native. Keyed and custom-partitioned edges are sliced by row key; unsupported Python mapping subclasses use the compatibility path. Disable only for the legacy row-oriented wire shape. |

## Scheduling and placement

Placement groups are an optimization with explicit fallbacks, not a job-wide
gang-scheduling guarantee.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `pipeline.placement-group.enabled` | `true` | bool | Tries to reserve one independently releasable single-bundle Ray placement group per streaming actor. This permits incremental scale-out reservation and scale-in release, but does not provide job-wide gang scheduling or FORWARD co-location. If reservation fails, Klein falls back to round-robin and then native placement. Ignored by `balanced` deployment mode and local debug mode. |
| `pipeline.placement-group.strategy` | `PACK` | enum: `PACK`, `SPREAD` | Ray placement-group strategy passed to each elastic actor group. Since each group contains one bundle, `PACK` and `SPREAD` currently have equivalent placement behavior. `STRICT_PACK` and `STRICT_SPREAD` are rejected because actor-scoped elastic groups cannot preserve their cross-actor guarantees. |
| `pipeline.placement-group.ready-timeout` | `120s` | duration | Maximum wait for placement-group reservation before Klein tries a fallback placement strategy. |

## Replay and task recovery

Replay buffers retain unacknowledged output for single-task recovery. Their
memory guard is intentionally hard: crossing it fails the task into the normal
recovery path instead of risking a process out-of-memory failure.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `pipeline.replay-buffer.enabled` | `true` | bool | Retains emitted records until downstream progress confirms they can be dropped, enabling single-task at-least-once replay. `false` leaves full-job restart as the recovery path. |
| `pipeline.replay-buffer.watermark-flush-batches` | `32` | int | Forces the complete input/operator/output durability boundary and advances every pending sender after this many processed input batches. Must be positive. |
| `pipeline.replay-buffer.max-bytes` | `268435456` (256 MiB) | int | Hard estimated-memory guard for retained replay data. After old acknowledgements are applied, a new batch that would cross the bound fails the task into normal recovery before process OOM. Must be positive while replay is enabled. |

## Managed state

The local state directory is disposable working data. Durable recovery always
comes from completed checkpoints under `execution.checkpointing.dir`.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `state.backend.type` | `memory` | string: `memory`, `rocksdb` | Backend for managed keyed state. Both recover from completed checkpoints; install `ray-klein[rocksdb]` before selecting RocksDB. |
| `state.backend.local-dir` | `<temp-dir>/klein/state` | string | Node-local working directory for RocksDB state. This isn't the durable checkpoint directory. |
| `state.checkpoint.object-store-cache.enabled` | `true` | bool | Caches sufficiently large immutable state snapshots in Ray's Object Store to accelerate recovery. Disabled automatically in in-process debug mode. |
| `state.checkpoint.object-store-cache.min-bytes` | `1048576` (1 MiB) | int | Minimum serialized snapshot size stored in the Object Store instead of coordinator memory. Must be non-negative. |
| `state.ttl.cleanup.batch-size` | `1000` | int | Maximum expired state entries removed after processing one operator input. Must be at least `1`. |
| `state.keyed.max-parallelism` | `32768` | int | Stable key-group count for keyed state. Must be at least `1`, must not be lower than operator parallelism, and must match the value stored in a restored checkpoint. |

`state.keyed.max-parallelism` is checkpoint metadata, not a routine tuning
knob. Changing ordinary operator concurrency can rescale keyed state, but
changing the max parallelism prevents restoration of existing keyed-state
checkpoints.

## Event time, Table and SQL, and UDF behavior

These settings change logical application behavior rather than only resource
usage. Review their correctness implications before overriding the defaults.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `event-time.idle-input.check-interval` | `1s` | duration | How often a task evaluates its input-idleness strategy while its inbox is empty. Must be greater than zero. |
| `table.exec.state.ttl` | `None` | duration or `None` | Default idle retention for streaming SQL regular joins, Top-N, and non-windowed aggregations. A configured value must be greater than zero. SQL hints or operator arguments override it for their state. |
| `udf.ignore-exception` | `false` | bool | If `true`, log a user-function exception and continue processing later records. In `auto` mode this selects native streaming so Klein can preserve record-level error and metric semantics. Leave disabled when dropping a failed record would violate correctness. |

## SQL DOWNLOAD network boundary

These options are captured with the query graph and apply to batch and
streaming SQL `DOWNLOAD`, including downloads used as media-function inputs.
Host and IP rules are conjunctive: a host allowlist never authorizes a private
resolved address by itself.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `sql.download.allowed-schemes` | `["file", "local", "memory", "s3", "gs", "http", "https"]` | sequence | URI schemes accepted by `DOWNLOAD`. A path without a scheme is treated as local. Unknown schemes are rejected. |
| `sql.download.allowed-hosts` | `[]` | sequence | Optional exact or `*.suffix` HTTP(S) host allowlist. Empty permits any host that passes all IP rules. |
| `sql.download.denied-hosts` | `[]` | sequence | Exact or `*.suffix` HTTP(S) hosts rejected before resolution. Deny rules take precedence. |
| `sql.download.allowed-ip-ranges` | `[]` | sequence | Optional IP/CIDR allowlist applied to every address returned for a host. Explicit ranges can authorize approved private destinations. |
| `sql.download.denied-ip-ranges` | `[]` | sequence | IP/CIDR denylist applied to every resolved address. Deny rules take precedence over allow ranges. |
| `sql.download.allow-private-network` | `false` | bool | Allows HTTP(S) addresses that are loopback, private, link-local, reserved, or otherwise not globally routable. Prefer narrow `allowed-ip-ranges`. |
| `sql.download.max-bytes` | `67108864` (64 MiB) | int | Maximum retained bytes for one download operator and row. Multiple SQL `DOWNLOAD` expressions in one projection or batch row share this budget; separate chained `stream.data.with_column(..., download(...))` operators each have their own budget. |
| `sql.download.timeout` | `30s` | duration | HTTP(S) connect, redirect, and response-read budget. Synchronous operating-system DNS resolution cannot be interrupted by this timer; storage-filesystem timeouts remain provider-specific. |
| `sql.download.max-redirects` | `5` | int | Maximum HTTP(S) redirects. Every destination is revalidated, redirects cannot leave HTTP(S), and HTTPS cannot downgrade to HTTP. |

## Compatibility-only adaptive partitioner options

The following options remain declared for compatibility with earlier adaptive
partitioner implementations. The current adaptive partitioner reacts directly
to downstream write timeouts and doesn't read them, so changing them has no
runtime effect.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `partitioner.adaptive.buffer-busy-threshold` | `0.5` | float | Intended fraction of input-buffer capacity above which a target is busy. |
| `partitioner.adaptive.busy-ratio` | `0.5` | float | Intended fraction of busy targets that triggers a statistics refresh. |
| `partitioner.adaptive.update-interval` | `3.0` | float, seconds | Intended interval between adaptive-partitioner statistics updates. |

## Observability

Dashboard options control publication to Klein's state actor. Logging format
and level are direct environment variables documented at the end of this page.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `observability.dashboard.enabled` | `true` | bool | Publishes redacted, read-only job snapshots to the detached Klein state actor. It doesn't control logs, metrics, or checkpointing. |
| `observability.dashboard.history-size` | `100` | int | Maximum current and terminal jobs retained in the state actor's in-memory history. Must be at least `1`; this history isn't durable. |

## Ray Serve integration

These settings apply only to regions marked for Ray Serve execution and require
the `ray-klein[serve]` optional dependencies.
See the [Ray Serve integration](connectors/ray-serve.md) for graph constraints,
deployment configuration, request behavior, and retries.

| Key | Default | Type | Description |
| --- | --- | --- | --- |
| `serve.proxy-endpoints` | `None` | string or `None` | Comma-separated HTTP base URLs for Serve proxies. At least one is required when an embedded proxy client is created. |
| `serve.deployment-name` | `None` | string or `None` | Ray Serve deployment name. Required for an embedded proxy client. |
| `serve.route-prefix` | `/` | string | Route prefix appended to the proxy endpoint and deployment path. |
| `serve.client.num-cpus` | `1.0` | float | CPU allocation for an embedded proxy client actor. A single-node Serve region inherits that node's resource setting instead. |
| `serve.client.concurrency` | `1` | int | Embedded proxy client operator concurrency. A single-node Serve region inherits that node's concurrency instead. |
| `serve.client.async-buffer-size` | `100` | int | Maximum pending asynchronous requests buffered by the embedded proxy client. |
| `serve.client.batch-timeout` | `5` | int, seconds | Maximum time spent accumulating a proxy batch. |
| `serve.client.batch-size` | `2` | int | Records per proxy request batch. |
| `serve.client.max-attempts` | `30` | int | Maximum HTTP attempts for one proxy request. |
| `serve.client.slow-request-warning` | `600` | int, seconds | Elapsed request time after which Klein emits a slow-request warning. |
| `serve.client.http-timeout` | `300` | int, seconds | Total timeout for each HTTPX request attempt. |
| `serve.client.http-connect-timeout` | `5` | int, seconds | HTTPX connection-establishment and pool-acquisition timeout. |
| `serve.client.http-limit-per-host` | `1000` | int | Maximum pooled HTTP connections to one host. |
| `serve.client.http-connection-limit` | `1000` | int | Maximum total pooled HTTP connections. |
| `serve.client.retry-backoff-max` | `3.0` | float, seconds | Maximum randomized exponential retry delay. The runtime also caps this value at `10s`. |

## Direct environment variables

The variables below are read directly and aren't typed `ConfigOption` values.
They don't participate in explicit-code-over-environment precedence and don't
appear in `Configuration.to_dict()`.

| Variable | Default | Description |
| --- | --- | --- |
| `RAY_KLEIN_DEBUG` | `0` | Enables in-process debug actors for `1`, `true`, or `yes` (case-insensitive). This mode doesn't validate Ray serialization, scheduling, isolation, or failure recovery. |
| `RAY_KLEIN_COMPILE_ONLY` | unset | If present, compiles the stream graph and returns a completed handle without executing the job. The value itself isn't parsed. |
| `RAY_KLEIN_RESOURCE_PLAN_LOAD_PATH` | unset | Loads a JSON resource plan and applies it to the compiled graph. |
| `RAY_KLEIN_RESOURCE_PLAN_PERSIST_PATH` | unset | Writes the compiled graph's resource plan to this path. |
| `RAY_SERVICE_NAME` | unset | When set inside a Klein Serve deployment, requires incoming requests to carry the same `rayservice` header. |
| `RAY_KLEIN_LOGGING_CONFIG` | bundled `logging.yaml` | Path to the YAML `dictConfig` loaded by `ray.klein.configure_logging()`. |
| `RAY_KLEIN_LOG_LEVEL` | YAML-configured level | Overrides the `ray.klein` logger level when `configure_logging()` runs. Standard Python levels and `TRACE` are accepted. |
| `RAY_KLEIN_LOG_FORMAT` | `text` | Selects `text` or `json` formatting when `configure_logging()` runs. |
| `KLEIN_NO_RICH_UI` | unset | Any non-empty value disables the interactive terminal progress table. |
| `NO_COLOR` | unset | Conventional terminal setting; any non-empty value also disables Klein's rich progress table. |
