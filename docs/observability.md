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
ray-klein dashboard --open
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

Start the standalone Klein Dashboard from a process connected to the Ray
cluster:

```bash
ray-klein dashboard --open \
  --ray-dashboard-url http://127.0.0.1:8265
```

Klein listens on `127.0.0.1:8266` by default. The exact MUI/React Flow Klein UI
is bundled in the `ray-klein` wheel, so serving it doesn't require a Ray source
checkout, Node.js, port 3001, or changes to Ray Dashboard. Klein data and control
requests stay on 8266. Overview, Jobs, Serve, Cluster, Actors, Metrics, Logs,
and actor-detail links leave the gateway for the configured native Ray
Dashboard. Set `RAY_KLEIN_RAY_DASHBOARD_URL`, or pass
`--ray-dashboard-url`, when it uses a browser-visible proxy URL.

Frontend contributors can run the source in `frontend/` and temporarily point
8266 at it with `--frontend-url`; production installations use the packaged UI.

The page polls published job snapshots and renders the operator DAG. Like
Flink's JobGraph, the whole node and its border continuously blend from idle
blue to busy red using the hottest subtask, then toward gray with a black border
for backpressure. Node titles and values use Flink's primary text opacity while
metric labels use its quieter secondary opacity. Selecting a node or operator
row opens a right-side drawer with subtask metrics. Separate views expose
checkpoint history, per-operator state size, barrier alignment and latency, and
redacted configuration.

The page also shows each operator's live parallelism and rates and exposes
guarded local rescaling. Select an operator, enter the target parallelism in
its details drawer, and confirm the change. When rescaling is unavailable, the
disabled control shows the runtime-provided reason. The same operation remains
available through the stable Python state API:

```python
result = ray.klein.rescale_operator(job_id, operator_id, parallelism=4)
```

A stale or terminal job is read-only. Rescaling changes only the physical
delta: subtasks whose indexes exist at both parallelisms retain their actor
identities. On a scale-out, Klein creates the added actors and waits for them
to answer a ping before it interrupts the data path. It then asks every direct
upstream task to insert an ordered local barrier on the incident edge and pause
at that cut. The target aligns those barriers, snapshots managed state, and
fences its direct downstream tasks before Klein swaps routing and resumes the
region. Unrelated actors stay alive; there is no whole-job restart or global
source stop.

If a source is itself a direct upstream of the target, that source task pauses
cooperatively at a record boundary while the local cut is installed. Sources
elsewhere in the graph keep running. Source operators cannot yet be the rescale
target, and transactional or collecting sinks are also unsupported. Multiple
source operators and parallel source subtasks are supported because the local
cut pauses the target's direct upstream tasks rather than globally stopping
every source. Unsupported API requests are rejected with the runtime-provided
reason.

Added actors remain fenced while the local cut is formed. At the cut, retained
actors prepare their new runtime descriptors and managed state transactionally,
and added actors prepare their initial runtime and assigned state. The old
runtime remains available until topology commit, so a pre-commit failure can
roll back without changing the retained actor identities. On a scale-in, Klein
does not create replacements: the removed actors stay fenced and available for
rollback until commit, and only those removed actors are stopped afterward.
With placement groups enabled, each physical actor owns an independently
releasable single-bundle group. Scale-out reserves the added groups before the
barrier, and scale-in releases the retired groups only after their actors stop.
Retained actors and their reservations never move. This elastic layout trades
job-wide gang scheduling and FORWARD-affinity bundle grouping for true
incremental resource allocation and release. It accepts only `PACK` and
`SPREAD`; `STRICT_PACK` and `STRICT_SPREAD` are rejected because separate
actor groups cannot preserve a cross-actor strict-placement guarantee.

"Retained" refers to the Ray actor identity, not to the Python operator object.
A retained actor calls the operator's `build`/`open` lifecycle for a pending
runtime while its old runtime is still open, then closes the old object in a
supervised background cleanup after commit. Plain functions and Klein's
framework operators support this handoff. Callable or lifecycle classes are
rejected by default because constructors or `open()` may acquire exclusive
external resources. Such a class may set
`supports_concurrent_rescale = True` only when two task-local instances can
safely overlap during rollback-preserving handoff.

The normal scale path does not restart the job. Before the committed topology
is released, the checkpoint coordinator arms every physical source in the
target operator's physical `CheckpointDomain` set for one shared stabilization
epoch. A checkpoint domain is a weakly connected component of physical
execution edges, so independent FORWARD lanes and disconnected branches do not
needlessly coordinate. Each selected source emits the same checkpoint ID at
its next record or idle boundary. Barrier ordering forms the consistent cut,
so sources keep producing while metadata becomes durable. Every task
temporarily backpressures only a physical input channel whose barrier arrived
early; other inputs keep draining until their matching barriers arrive, then
the task snapshots and forwards the barrier once. Blocked records remain
charged to the bounded inbox rather than moving to an unbounded side buffer;
the finite set of in-flight control barriers has reserved admission so data
traffic cannot starve alignment. The component's source offsets and operator
shards are persisted atomically. Domains outside the target set do not join or
wait for this epoch. The recovery fence is removed only after durability and
source-state release. If any task fails after the local commit but before then,
Klein deliberately falls back to a consistent global checkpoint recovery
instead of restoring one task from stale state. If the coordinator is rebuilt
during this window, Klein reclaims the old epoch and re-requests stabilization
automatically.

The standalone Dashboard has no built-in authentication. Its default listener
is loopback-only, and a non-loopback listener requires
`--allow-unauthenticated`. Expose both Klein and Ray through the cluster's
authenticated operations proxy.

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
