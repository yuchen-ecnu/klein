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
ray-klein cancel klein-orders-0123abcd
```

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

:::{note}
The standalone wheel does not patch or bundle Ray Dashboard backend or React
files. A native Ray Dashboard page requires an upstream Ray integration that
adapts this state API. This repository deliberately avoids depending on Ray's
private dashboard implementation.
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
| Execution | `processing_duration_ms`, `input_buffer_records`, `input_buffer_utilization`, `backpressure_events`, `backpressure_duration_ms` |
| Event time | `current_input_watermark_ms`, `current_output_watermark_ms`, `watermark_lag_ms`, `idle_inputs`, `late_records_dropped`, `timers_fired` |
| Checkpoints | `barriers_in`, `barriers_out`, `checkpoint_alignment_duration_ms`, `checkpoint_barrier_latency_ms`, `checkpoints_triggered`, `checkpoints_completed`, `checkpoints_failed`, `checkpoints_in_progress`, `checkpoint_duration_ms`, `checkpoint_persist_duration_ms`, `sink_transactions_pending`, `sink_transactions_committed`, `sink_transaction_commit_failures`, `sink_transaction_commit_duration_ms` |
| State | `managed_state_size_bytes`, `state_snapshot_duration_ms`, `state_restore_duration_ms`, `ttl_entries_cleaned`, `replay_buffer_records`, `state_object_store_writes`, `state_object_store_restores`, `state_durable_restore_fallbacks` |
| Connectors | `redis_flush_duration_ms`, `redis_flush_batch_records`, `redis_lookup_duration_ms`, `redis_failures`, `kafka_poll_duration_ms`, `kafka_poll_batch_records`, `kafka_assigned_partitions`, `kafka_consumer_lag_records`, `kafka_commits`, `kafka_commit_duration_ms`, `kafka_errors`, `serve_request_duration_ms`, `serve_request_failures` |

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
```

Use Ray's Prometheus and Grafana integration for retention, alerting, and
cross-job dashboards. Klein snapshots derive short-interval row rates, busy
percentage, and backpressure percentage from monotonic task counters without a
Prometheus dependency.

## State publication architecture

The standalone integration uses only a small Ray actor surface:

1. A streaming `JobClient` registers its `JobManager` with one detached,
   zero-CPU state actor on the Ray head node.
2. The state actor refreshes immutable snapshots concurrently and keeps the
   last good snapshot for temporary actor outages and terminal history.
3. `list_job_snapshots`, `get_job_snapshot`, and `cancel_job` expose a stable
   client API for CLIs, dashboards, and automation.

An eventual Ray Dashboard contribution can add HTTP and React adapters without
moving scheduling state into the dashboard process.
