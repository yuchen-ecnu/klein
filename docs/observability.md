---
myst:
  html_meta:
    description: "Observe Klein for Ray jobs with structured logs, metrics, checkpoints, CLI attach, and a cluster state API."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-observability)=
# Observe Klein jobs

Klein uses Ray's existing observability stack. Worker and actor logs remain Ray
logs, runtime metrics use `ray.util.metrics`, and a lightweight detached actor
on the head node provides JSON-safe job discovery. Each `JobManager` remains
the source of truth for current state.

## Query and control jobs

Use the CLI to discover or attach to jobs from another process:

```bash
ray-klein list
ray-klein status klein-orders-0123abcd
ray-klein attach klein-orders-0123abcd
ray-klein stop klein-orders-0123abcd
```

Omit the namespace when exactly one job is running, or when you want the CLI
to present an interactive picker. Use `ray-klein stop --force <namespace>` in
non-interactive automation; without `--force`, `stop` asks for confirmation.
`ray-klein cancel` is an equivalent spelling that matches the Python API.
Use `list --all` to include retained terminal jobs, and add `--json` to `list`
or `status` when another program will consume the result.

Operations integrations can consume the stable Python state API:

```python
import ray

ray.init(address="auto")
jobs = ray.klein.list_job_snapshots()
job = ray.klein.get_job_snapshot(jobs[0]["job_id"])
```

Snapshots include job and operator status, task metrics, checkpoint history,
configuration with credential-like values redacted, and a
`dashboard_stale` marker when the last good cached snapshot is returned.
Terminal history is in memory and is distinct from durable checkpoints.

Configure publication and retention with:

```python
import ray

ray.klein.configure({
    "observability.dashboard.enabled": True,
    "observability.dashboard.history-size": 100,
})
```

Disabling state publication doesn't disable Ray logs, Ray metrics, or
checkpointing.

## Use the Klein Dashboard

Start the bundled web Dashboard from a machine that can connect to the Ray
cluster:

```bash
ray-klein dashboard --open
```

It listens on `127.0.0.1:8266` by default. The page polls the published job
snapshots and renders the operator DAG. Like Flink's JobGraph, node color mixes
idle (blue), busy (red), and backpressured (black) time. The graph uses the
maximum busy and backpressure percentage across an operator's subtasks so one
hot or skewed subtask is not hidden by the operator average; the exact values
remain printed in each node and in the operator table for accessibility.

The page also shows each operator's live parallelism and rates, and lets you
apply a new positive parallelism to one running operator. A stale or terminal
job is read-only. For a supported operator, every direct upstream task inserts
an ordered local barrier on the incident edge and pauses at that cut. The old
target aligns those barriers, snapshots managed state, and fences its direct
downstream tasks before Klein swaps routing and resumes the region. Unrelated
actors stay alive; there is no whole-job restart or global source stop.

If a source is itself a direct upstream of the target, that source task pauses
cooperatively at a record boundary while the local cut is installed. Sources
elsewhere in the graph keep running. Source operators cannot yet be the rescale
target, and transactional or collecting sinks are also unsupported in the
first version. Local rescaling also currently requires the job to have exactly
one physical source task (one source operator at concurrency 1); this keeps the
post-commit recovery checkpoint on one consistent source cut until Klein gains
a shared checkpoint epoch across parallel sources. Unsupported controls are
disabled with the runtime-provided reason.

The replacement operator is created and restores its state before the old
operator is removed, but its input pump stays fenced until the topology commit.
A scale operation therefore needs temporary capacity for both parallelisms.
Existing job-wide PlacementGroup bundles are not resized: replacement actors
use Ray's native placement, and bundles made surplus by a scale-in remain
reserved until the job ends.

The normal scale path does not restart the job. After commit, the checkpoint
coordinator asks the source to emit an ordinary checkpoint at its next record
or idle boundary; this stabilizes the new topology without stopping the source.
The recovery fence is removed only after that checkpoint is durable. If any
task fails after the local commit but before then, Klein deliberately falls
back to a consistent global checkpoint recovery instead of restoring one task
from stale state. If the coordinator is rebuilt during this window, Klein
re-requests the stabilization checkpoint automatically.

Use `--host` and `--port` to change the listener. Binding to a non-loopback
address exposes an unauthenticated control endpoint and is refused unless you
explicitly pass `--allow-unauthenticated`. Put such a listener behind an
authenticated reverse proxy, or access the default listener through an SSH
tunnel instead. A reverse proxy should rewrite `Host` to the configured
listener host; the server rejects untrusted host names to prevent DNS-rebinding
control requests.

:::{note}
The standalone wheel does not patch Ray Dashboard backend or React files. The
bundled Klein Dashboard is a small, independent HTTP page over the stable state
API. Embedding the same controls inside Ray's native Dashboard still requires
an upstream Ray integration; Klein deliberately avoids Ray's private dashboard
implementation.
:::

## Configure operational logs

Klein emits operational events through Python's standard `logging` package.
When the application doesn't install a Klein-specific handler, messages follow
the normal Ray driver, worker, and actor logging pipeline. Call
`configure_logging()` when the process needs a dedicated text or JSON stream:

```python
import ray

ray.klein.configure_logging(level="INFO", log_format="json")
```

The equivalent environment settings are:

```bash
export RAY_KLEIN_LOG_LEVEL=INFO
export RAY_KLEIN_LOG_FORMAT=json
```

Set `RAY_KLEIN_LOGGING_CONFIG` to a YAML `dictConfig` file to replace the
bundled handler configuration. Klein doesn't replace the root logger or write
directly into Ray's private session directory.

JSON events contain a timestamp, level, component, event name, message, process
and thread identity, plus available context such as `job_id`, `operator_id`,
`task_id`, `subtask_index`, and `checkpoint_id`. Stable event names use dotted
verbs, for example `job.status.changed`, `checkpoint.completed`, and
`failover.global.started`. Structured fields whose names look like passwords,
tokens, credentials, secrets, or API keys are redacted.

## Keep logs and data separate

Operational logs go to stderr. Stdout is reserved for an explicit user-facing
boundary: terminal progress rendering or a console data sink. This makes data
safe to pipe without mixing it with task lifecycle messages.

`ConsoleSinkFunction` writes one JSON Lines object per record:

```json
{"sink":"console","subtask_index":0,"sequence":1,"value":{"id":42}}
```

Don't use `print()` in an operator for diagnostics. Create a module logger with
`get_logger(__name__)` inside Klein itself, or use the application's standard
Python logger in user code. Avoid logging whole records, state snapshots,
connector command buffers, and configuration mappings.

## Query metrics

Klein publishes native `ray.util.metrics` metrics in three stable scopes:

| Scope | Prefix | Labels |
|---|---|---|
| Job | `ray_klein_job_` | `job_id`, `job_name` |
| Task | `ray_klein_task_` | job labels plus `task_id`, `task_name`, `subtask_index` |
| Operator | `ray_klein_operator_` | task labels plus `operator_id`, `operator_name` |

Labels identify bounded runtime topology; record values, keys, exception text,
URLs, checkpoint paths, and Object IDs are never labels. Counter definitions do
not include `_total` because Ray's Prometheus exporter adds that suffix.

The main built-in metrics are:

| Area | Metrics |
|---|---|
| Throughput | `records_in`, `records_out`, `filter_records_in`, `filter_records_dropped` |
| Execution | `processing_duration_ms`, `input_buffer_records`, `input_buffer_bytes`, `input_buffer_utilization`, `input_buffer_byte_utilization`, `emit_queue_batches`, `transport_requests`, `transport_batch_rows`, `transport_batch_bytes`, `transport_send_duration_ms`, `transport_inflight_requests`, `backpressure_events`, `backpressure_duration_ms` |
| Event time | `current_input_watermark_ms`, `current_output_watermark_ms`, `watermark_lag_ms`, `idle_inputs`, `late_records_dropped`, `timers_fired` |
| Checkpoints | `barriers_in`, `barriers_out`, `checkpoint_alignment_duration_ms`, `checkpoint_barrier_latency_ms`, `checkpoints_triggered`, `checkpoints_completed`, `checkpoints_failed`, `checkpoints_in_progress`, `checkpoint_duration_ms`, `checkpoint_persist_duration_ms`, `sink_transactions_pending`, `sink_transactions_committed`, `sink_transaction_commit_failures`, `sink_transaction_commit_duration_ms` |
| State | `managed_state_size_bytes`, `state_snapshot_duration_ms`, `state_restore_duration_ms`, `ttl_entries_cleaned`, `replay_buffer_records`, `replay_buffer_bytes`, `state_object_store_writes`, `state_object_store_restores`, `state_durable_restore_fallbacks` |
| Connectors | `redis_flush_duration_ms`, `redis_flush_batch_records`, `redis_lookup_duration_ms`, `redis_failures`, `kafka_poll_duration_ms`, `kafka_poll_batch_records`, `kafka_assigned_partitions`, `kafka_consumer_lag_records`, `kafka_commits`, `kafka_commit_duration_ms`, `kafka_errors`, `rocketmq_received_records`, `rocketmq_acknowledged_records`, `rocketmq_pending_records`, `rocketmq_errors`, `serve_request_duration_ms`, `serve_request_failures` |

Latencies and batch sizes are histograms rather than last-value gauges. For
example, a Prometheus query for operator p95 processing latency is:

```promql
histogram_quantile(
  0.95,
  sum(rate(ray_klein_operator_processing_duration_ms_bucket[5m]))
    by (le, job_id, operator_name)
)
```

Rows per second can be queried with:

```promql
sum(rate(ray_klein_operator_records_out_total[1m]))
  by (job_id, operator_name)
```

Custom application metrics remain available from `runtime_context.metric_group`.
Give custom metrics a unit-bearing name and description, reuse the same type and
label set for a name, and use histograms for distributions:

```python
latency = runtime_context.metric_group.histogram(
    "external_call_duration_ms",
    boundaries=[1, 5, 10, 50, 100, 500, 1000],
    description="External service call duration in milliseconds.",
)
with latency.time():
    call_external_service()
```

Use Ray's Prometheus and Grafana integration for retention, alerting, and
cross-job dashboards. Klein snapshots derive short-interval row rates, busy
percentage, and backpressure percentage from monotonic task counters without a
Prometheus dependency.

For symptom-oriented interpretation, see
[Performance tuning](performance-tuning.md) and
[Troubleshooting](troubleshooting.md).

## State publication architecture

The standalone integration uses only a small Ray actor surface:

1. A streaming `JobClient` registers its `JobManager` with one detached,
   zero-CPU state actor on the Ray head node.
2. The state actor refreshes immutable snapshots concurrently and keeps the
   last good snapshot for temporary actor outages and terminal history.
3. `list_job_snapshots`, `get_job_snapshot`, `rescale_operator`, and
   `cancel_job` expose a stable client API for CLIs, dashboards, and automation.

The bundled HTTP page is one adapter over this API. An eventual native Ray
Dashboard contribution can reuse the same boundary without moving scheduling
state into the dashboard process.
