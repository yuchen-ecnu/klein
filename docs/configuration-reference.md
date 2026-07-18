---
myst:
  html_meta:
    description: "Complete Klein for Ray configuration reference with every option, type, default, constraint, and environment variable."
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

## Execution, recovery, and checkpointing

These options primarily affect the streaming runtime. A bounded job compiled
to Ray Data doesn't create Klein checkpoint, restart, or task-deployment
components.

| Key | Type | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `execution.runtime.mode` | enum: `auto`, `batch`, `streaming` | `auto` | Selects the execution engine. `auto` uses streaming when any source is unbounded or any sink lacks a Ray Data lowering; otherwise it uses batch. A bounded custom source without a batch lowering must explicitly select streaming. |
| `execution.task.deployment.mode` | enum: `default`, `balanced` | `default` | Selects streaming-task placement. `default` tries a placement group, then round-robin placement, then native Ray placement. `balanced` skips the placement-group attempt. |
| `execution.restart-strategy.fixed-delay.attempts` | int | `3` | Maximum restarts allowed inside the count window. Must be at least `0`; `0` suppresses the first restart. |
| `execution.restart-strategy.fixed-delay.delay` | duration | `10s` | Delay before each restart. Must be non-negative. |
| `execution.restart-strategy.fixed-delay.count-interval` | duration | `10min` | Sliding window used to count restart attempts. Must be greater than zero. |
| `execution.checkpointing.trigger.interval-duration` | duration | `60s` | Maximum wall-clock time between source checkpoint barriers. `0` disables the time trigger. |
| `execution.checkpointing.trigger.interval-records` | int | `512` | Maximum records emitted by a source between checkpoint barriers. `0` disables the record trigger. Whichever trigger fires first resets both intervals. |
| `execution.checkpointing.persistence-interval` | int, seconds | `600` | Interval for persisting checkpoint-coordinator metadata. Must be at least `0`; `0` disables periodic persistence. Completion paths can still persist metadata. |
| `execution.checkpointing.max-concurrent-checkpoints` | int | `100` | Maximum checkpoint attempts that may be in flight. Must be at least `1`. |
| `execution.checkpointing.timeout` | int, seconds | `600` | Maximum time an in-flight checkpoint may take before it is discarded. Must be at least `0`; `0` disables checkpoint expiry. |
| `execution.checkpointing.max-history-size` | int | `100` | Maximum checkpoint-history entries retained in coordinator memory. Must be at least `1`. |
| `execution.checkpointing.async-notify` | bool | `false` | If `true`, committers send checkpoint-complete notifications without waiting for the coordinator acknowledgement and reap or retry them at later barriers. |
| `execution.checkpointing.dir` | string | `<temp-dir>/klein/checkpoint` | Durable checkpoint root. Supports local paths, `file://`, `s3://`, and `gs://`. Use shared durable storage for recovery across nodes. |
| `execution.checkpointing.storage-options` | mapping or `None` | `None` | Keyword arguments passed to PyArrow `S3FileSystem` or `GcsFileSystem`. Accepted only when the checkpoint URI uses `s3://` or `gs://`. |
| `execution.checkpointing.num-retained` | int | `1` | Number of completed `chk-N` directories retained per job. Must be at least `1`. |
| `execution.savepoint.path` | string or `None` | `None` | Checkpoint or savepoint path from which the job restores at submission. |

Setting both checkpoint trigger intervals to `0` prevents sources from emitting
periodic checkpoint barriers. This also delays checkpoint-driven source offset
durability and two-phase-commit sink completion; use that combination only when
you intentionally don't need periodic recovery points.

## Job lifecycle

| Key | Type | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `job.scheduler.start.timeout` | int, seconds | `300` | Per-step limit for starting workers and for startup-heavy coordinator operations such as checkpoint restoration. |
| `job.deploy.timeout` | int, seconds | `600` | Total time budget for coordinator initialization, worker scheduling, and coordinator start. |
| `job.stop.timeout` | int, seconds | `60` | Total time budget for stopping the supervisor, workers, and coordinator. It is also the default `cancel()` budget. |
| `job.coordinator.rpc.timeout` | int, seconds | `30` | Limit for lightweight coordinator RPCs such as health probes, metadata flush, and stop. |
| `job.healthcheck.interval` | int, seconds | `15` | Interval between `JobManager` health checks. |
| `job.namespace` | string | `""` | Ray namespace used for this job's named actors. An empty string generates a unique `klein-<job>-<id>` namespace. Set a stable value only when another client or operations tool must attach to the same actors. |

## Pipeline buffers, placement, and data plane

| Key | Type | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `pipeline.input-buffer.size` | int | `200` | Maximum records queued in each streaming task inbox. Values at or below `0` make the underlying asyncio queue unbounded. |
| `pipeline.input-buffer.put-timeout` | duration | `1s` | Time allowed for a downstream inbox write before the emitter treats it as backpressure and retries or reroutes. The current runtime uses whole seconds. |
| `pipeline.output-buffer.size` | int | `1000` | Reserved output-buffer capacity. The current collector carries this value in deployment descriptors but doesn't use it to bound a queue. |
| `pipeline.internal.batch-size` | int | `10` | Records accumulated per downstream target before a micro-batch is emitted. Must be non-negative; `0` effectively emits each record immediately. |
| `pipeline.operator-chaining.enabled` | bool | `true` | Co-locates compatible non-shuffle operators in one task to avoid serialization between them. |
| `pipeline.columnar-passthrough.enabled` | bool | `false` | Keeps batched output column-oriented across compatible downstream edges instead of converting it to rows and back. Keyed and custom-partitioned edges are sliced by row key. |
| `pipeline.placement-group.enabled` | bool | `true` | Tries to gang-schedule the streaming job in a Ray placement group. If reservation fails, Klein falls back to round-robin and then native placement. Ignored by `balanced` deployment mode and local debug mode. |
| `pipeline.placement-group.strategy` | string | `PACK` | Ray placement-group strategy. Klein documents and tests `PACK` and `SPREAD`; invalid values are rejected by Ray. |
| `pipeline.placement-group.ready-timeout` | duration | `120s` | Maximum wait for placement-group reservation before Klein tries a fallback placement strategy. |
| `pipeline.replay-buffer.enabled` | bool | `true` | Retains emitted records until downstream progress confirms they can be dropped, enabling single-task at-least-once replay. `false` leaves full-job restart as the recovery path. |
| `pipeline.replay-buffer.watermark-flush-batches` | int | `32` | Forces an output flush and replay-watermark advance after this many processed input batches. Lower values reduce retained replay data at the cost of smaller downstream batches. Values below `1` are clamped to `1`. |
| `pipeline.replay-buffer.max-bytes` | int | `0` | Reserved soft byte limit for replay data; `0` is documented as unlimited. The current runtime records replay size but doesn't yet apply this limit or pacing behavior. |

## Managed state, SQL state, event time, and UDFs

| Key | Type | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `state.backend.type` | string: `memory`, `rocksdb` | `memory` | Backend for managed keyed state. Both recover from completed checkpoints; install `ray-klein[rocksdb]` before selecting RocksDB. |
| `state.backend.local-dir` | string | `<temp-dir>/klein/state` | Node-local working directory for RocksDB state. This isn't the durable checkpoint directory. |
| `state.checkpoint.object-store-cache.enabled` | bool | `true` | Caches sufficiently large immutable state snapshots in Ray's Object Store to accelerate recovery. Disabled automatically in in-process debug mode. |
| `state.checkpoint.object-store-cache.min-bytes` | int | `1048576` (1 MiB) | Minimum serialized snapshot size stored in the Object Store instead of coordinator memory. Must be non-negative. |
| `state.ttl.cleanup.batch-size` | int | `1000` | Maximum expired state entries removed after processing one operator input. Must be at least `1`. |
| `state.keyed.max-parallelism` | int | `128` | Stable key-group count for keyed state. Must be at least `1`, must not be lower than operator parallelism, and must match the value stored in a restored checkpoint. |
| `table.exec.state.ttl` | duration or `None` | `None` | Default idle retention for streaming SQL regular joins, Top-N, and non-windowed aggregations. A configured value must be greater than zero. SQL hints or operator arguments override it for their state. |
| `event-time.idle-input.check-interval` | duration | `1s` | How often a task evaluates its input-idleness strategy while its inbox is empty. Must be greater than zero. |
| `udf.ignore-exception` | bool | `false` | If `true`, log a user-function exception and continue processing later records. Leave disabled when dropping a failed record would violate correctness. |

`state.keyed.max-parallelism` is checkpoint metadata, not a routine tuning
knob. Changing ordinary operator concurrency can rescale keyed state, but
changing the max parallelism prevents restoration of existing keyed-state
checkpoints.

## Adaptive partitioner compatibility options

The following options remain declared for compatibility with earlier adaptive
partitioner implementations. The current adaptive partitioner reacts directly
to downstream write timeouts and doesn't read them, so changing them has no
runtime effect.

| Key | Type | Default | Intended meaning |
| --- | --- | --- | --- |
| `partitioner.adaptive.buffer-busy-threshold` | float | `0.5` | Intended fraction of input-buffer capacity above which a target is busy. |
| `partitioner.adaptive.busy-ratio` | float | `0.5` | Intended fraction of busy targets that triggers a statistics refresh. |
| `partitioner.adaptive.update-interval` | float, seconds | `3.0` | Intended interval between adaptive-partitioner statistics updates. |

## Observability

| Key | Type | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `observability.dashboard.enabled` | bool | `true` | Publishes redacted, read-only job snapshots to the detached Klein state actor. It doesn't control logs, metrics, or checkpointing. |
| `observability.dashboard.history-size` | int | `100` | Maximum current and terminal jobs retained in the state actor's in-memory history. Must be at least `1`; this history isn't durable. |

## Ray Serve integration

These settings apply only to regions marked for Ray Serve execution and require
the `ray-klein[serve]` optional dependencies.
See the [Ray Serve integration](connectors/ray-serve.md) for graph constraints,
deployment configuration, request behavior, and retries.

| Key | Type | Default | Meaning and constraints |
| --- | --- | --- | --- |
| `serve.proxy-endpoints` | string or `None` | `None` | Comma-separated HTTP base URLs for Serve proxies. At least one is required when an embedded proxy client is created. |
| `serve.deployment-name` | string or `None` | `None` | Ray Serve deployment name. Required for an embedded proxy client. |
| `serve.route-prefix` | string | `/` | Route prefix appended to the proxy endpoint and deployment path. |
| `serve.client.num-cpus` | float | `1.0` | CPU allocation for an embedded proxy client actor. A single-node Serve region inherits that node's resource setting instead. |
| `serve.client.concurrency` | int | `1` | Embedded proxy client operator concurrency. A single-node Serve region inherits that node's concurrency instead. |
| `serve.client.async-buffer-size` | int | `100` | Maximum pending asynchronous requests buffered by the embedded proxy client. |
| `serve.client.batch-timeout` | int, seconds | `5` | Maximum time spent accumulating a proxy batch. |
| `serve.client.batch-size` | int | `2` | Records per proxy request batch. |
| `serve.client.max-attempts` | int | `30` | Maximum HTTP attempts for one proxy request. |
| `serve.client.slow-request-warning` | int, seconds | `600` | Elapsed request time after which Klein emits a slow-request warning. |
| `serve.client.http-timeout` | int, seconds | `300` | Total HTTP request timeout passed to `aiohttp`. |
| `serve.client.http-connect-timeout` | int, seconds | `5` | HTTP connection-establishment timeout passed to `aiohttp`. |
| `serve.client.http-limit-per-host` | int | `1000` | Maximum pooled HTTP connections to one host. |
| `serve.client.http-connection-limit` | int | `1000` | Maximum total pooled HTTP connections. |
| `serve.client.retry-backoff-max` | float, seconds | `3.0` | Maximum randomized exponential retry delay. The runtime also caps this value at `10s`. |

## Direct environment variables

The variables below are read directly and aren't typed `ConfigOption` values.
They don't participate in explicit-code-over-environment precedence and don't
appear in `Configuration.to_dict()`.

| Variable | Default | Meaning |
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
