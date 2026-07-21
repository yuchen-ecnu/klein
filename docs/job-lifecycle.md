---
myst:
  html_meta:
    description: "Understand Klein for Ray graph contexts, sinks, execution handles, job states, identifiers, cancellation, failures, resource plans, and cleanup."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Job lifecycle

A Klein job has two distinct lifetimes:

1. On the driver, module-level APIs build a lazy graph ending in one or more
   sinks; an internal pipeline owner keeps their shared state.
2. `execute()` compiles that graph and either runs a Ray Data batch job to
   completion or submits a native streaming job to a detached `JobManager`.

Keeping the builder, submitted job, and returned handle separate makes
deployment and cleanup predictable. This page describes those boundaries.

## Build one graph, then execute it

Streams created together share graph configuration, identifiers, and SQL
session state. Streams from different isolated pipelines cannot be combined.

The normal API keeps that owner out of application code:

```python
import ray
import ray.klein

ray.klein.configure({"execution.runtime.mode": "batch"})
events = ray.klein.from_items([{"id": 1}, {"id": 2}])
result = events.map(lambda row: {"id": row["id"] * 10})
result.take_all()
handle = ray.klein.execute("multiply-ids")
```

### Advanced isolation

Create an explicit context only when one process must build independent
pipelines or needs isolated configuration:

```python
from ray.klein import KleinContext

ctx = KleinContext({"execution.runtime.mode": "streaming"})
events = ctx.from_values({"id": 1}, {"id": 2})
events.show()
handle = ctx.execute("show-events")
```

Creating a terminal operation such as `show()`, `take_all()`, or `write_*()`
constructs a `StreamSink` and immediately registers it with that context. The
graph remains lazy: registration does not read records or create workers.

`execute("job-name")` uses every terminal currently registered in the
pipeline. A successful submission consumes those registrations; an exception
keeps them available for inspection or retry. A retry can repeat side effects
that completed before a batch failure, so connector delivery guarantees still
apply. Register all branches first, then call `execute()` once to submit them
as one job. Use a fresh `KleinContext` only for an independently configured
logical job.

`reset_context()` remains a deprecated compatibility helper for ambient
module-level pipelines. New module-level code should finish graph construction
with terminal operations and call `execute("job-name")`; use an explicit
context only for the advanced isolation case.

## Explain before execution

`explain()` compiles the registered graph and returns its resource plan as
JSON. It does not initialize Ray, create a `JobManager`, run a source, or invoke
a sink:

```python
print(ray.klein.explain("multiply-ids"))
```

Attach every intended sink before calling `explain`; the plan includes every
terminal currently registered in that pipeline. Explaining does not consume
the registrations, so the following `execute("multiply-ids")` submits the same
graph as long as no other terminals are added in between.

Plan construction includes the Ray Serve graph rewrite and any resource plan
loaded through the environment. It can also persist the resulting resource
plan when requested. Operator chaining and physical streaming expansion happen
later during submission, so use the runtime observability view for the final
physical task set.

## Execute a job

At least one sink is required. Without one, `execute()` raises a `ValueError`
instead of running an unconsumed stream. Pass a stable name to production jobs;
calling `execute()` without one creates a random display name.

Execution has three possible paths:

| Path | What `execute()` does | Returned handle |
|---|---|---|
| Batch | Compiles the graph to Ray Data, executes every sink synchronously, and returns after completion. | `CompletedJobHandle` |
| Native streaming | Initializes Ray if necessary, creates and schedules a detached job, and returns after submission succeeds. | `LiveJobHandle` |
| Compile-only | Builds the logical graph but runs no source, UDF, or sink. | `CompletedJobHandle` |

In `execution.runtime.mode=auto`, an unbounded source, any graph vertex without
a batch lowering, or `udf.ignore-exception=true` selects streaming. Only a
bounded graph whose every vertex can lower to Ray Data and whose record-level
ignore-exception policy is disabled selects batch. This check includes sources,
intermediate transformations, and sinks, so a bounded custom source or a graph
containing streaming-only `key_by`, managed state, `map_reduce`, windows, or
joins selects streaming automatically. A production deployment can still pin
`streaming` deliberately when mode changes should require a configuration
review.

Conversely, selecting streaming cannot make a Ray Data-only source or operation
streamable. Check [operator compatibility](operator-compatibility.md) whenever
the graph mixes execution families.

Batch execution and initial streaming submission can fail before a handle is
returned. Those exceptions are raised directly from `execute()`. A live handle
reports failures that occur after successful submission.

## Use the returned handle

The two handle types intentionally have different meanings:

| Operation | `CompletedJobHandle` | `LiveJobHandle` |
|---|---|---|
| `status` | Always `JobStatus.FINISHED`. | RPC to the job's `JobManager`. |
| `wait()` | Returns immediately. | Blocks without polling until a terminal state; raises `KleinError` for `FAILED`. |
| `get()` | Returns the in-memory batch result or compile-only logical graph. | Waits for terminal state and drains one collecting sink's output queue. |
| `cancel(timeout=60)` | Returns `True`; there is no live work to stop. | Requests coordinated teardown and returns whether cancellation completed. |
| `namespace` | `None`. | The Ray namespace/job identifier. |

Use `get()` only when the job contains one result-producing terminal. Klein
enforces that `take()` or `take_all()` is the job's only terminal in both batch
and streaming modes, so collection has one consistent contract. Use `wait()`
for one or more side-effect terminals. Batch errors have already been raised by
`execute()`, before the completed handle exists.

Use `wait()` for ordinary streaming sink jobs. `LiveJobHandle.get()` is a
special collection boundary: the graph must contain exactly one collecting
operator such as `take()` or `take_all()`. It is not a general way to retrieve
file, Kafka, SQL, or custom sink results. It also drains after terminal status
without performing `wait()`'s explicit failed-status check, so use `wait()` or
check `status` when failure propagation matters.

If `LiveJobHandle.wait()` receives `KeyboardInterrupt` or `SystemExit`, it
attempts a five-second cancellation and then re-raises the signal exception.
Simply allowing the submitting process to exit without waiting is different:
the detached streaming job continues on the Ray cluster.

## Job states

`JobStatus` defines five non-terminal and three terminal states:

| State | Terminal | Meaning |
|---|---:|---|
| `CREATED` | No | The `JobManager` exists but has not accepted a graph. |
| `SUBMITTING` | No | The graph is being optimized, expanded, restored, and scheduled. |
| `DEPLOYING` | No | A valid public lifecycle state reserved for deployment progress. |
| `INITIALIZING` | No | A valid public lifecycle state reserved for task initialization progress. |
| `RUNNING` | No | Submission succeeded and supervision is active. |
| `FINISHED` | Yes | All tasks reached normal completion and teardown ran. |
| `CANCELLED` | Yes | User cancellation completed teardown. |
| `FAILED` | Yes | Submission, recovery, or runtime supervision failed permanently. |

The current streaming manager normally follows one of these paths:

```text
CREATED -> SUBMITTING -> RUNNING -> FINISHED
              |             |  \-> CANCELLED
              |             \----> FAILED
              \------------------> FAILED
```

`DEPLOYING` and `INITIALIZING` are part of the enum so clients must tolerate
them, but the current manager does not explicitly publish them during normal
submission. A successfully returned live handle is usually already `RUNNING`
because `execute()` waits for scheduling to succeed. Status snapshots retain a
timestamped transition history for diagnosis.

A finite source naturally propagates end-of-data through every sink; once all
tasks finish, the job becomes `FINISHED`. A bounded collecting operation such
as `take(n)` can request graceful source drain. Neither case is cancellation.

## Job names, namespaces, and job IDs

`job_name` is the human-readable logical label used in logs, plans, lineage,
and metrics. A native streaming submission also gets a Ray namespace. Without
an explicit setting, its shape is:

```text
klein-{sanitized-job-name}-{8-hex-characters}
```

The embedded name is lowercased, runs of non-alphanumeric characters become a
dash, and it is capped at 40 characters. The random suffix lets two submissions
with the same `job_name` coexist safely.

Set `job.namespace` only when operations tooling needs a stable identifier:

```python
ray.klein.configure({
    "job.namespace": "orders-production",
    "execution.runtime.mode": "streaming",
})
```

An explicit non-empty namespace is used verbatim. Do not reuse it while an old
detached job's named actors still exist.

For native streaming, `handle.namespace`, the observability `job_id`, and the
`RuntimeContext.job_id` are the same namespace string. The display `job_name`
is not a unique runtime ID. Batch and compile-only handles have no Ray
namespace.

Record `job_name`, namespace, code/configuration version, source identity, and
checkpoint root in deployment metadata. The namespace is what `ray-klein
status`, `attach`, and `cancel` use after the original driver exits. See
[observability](observability.md).

## Execute multiple sinks as one graph

Register every terminal before the final call when branches must share upstream
work and one job lifecycle:

```python
prepared = events.map(normalize, name="Normalize")
prepared.show()
prepared.write_json("s3://bucket/orders/")

plan = ray.klein.explain("orders")
handle = ray.klein.execute("orders")
```

In batch mode, a shared fan-out may materialize the common `Dataset` so its
branches can reuse it. Multi-sink jobs are intended for side effects and should
be observed with `wait()`, not treated as an ordered aggregate result.

In native streaming, the job reaches `FINISHED` only after all task and sink
vertices finish. One job lifecycle does not imply that unrelated sinks have the
same external guarantee: each connector still defines its own acknowledgement,
idempotency, and transaction boundary. Disconnected graph components can also
have separate checkpoint domains. Review
[delivery and consistency guarantees](delivery-semantics.md) for every branch.

### Advanced: selective execution

If one pipeline deliberately stages terminals for different submissions,
select explicit roots for each job. Unselected terminals remain pending:

```python
preview_sink = prepared.show()
archive_sink = prepared.write_json("s3://bucket/orders/")

preview = ray.klein.explain("orders-preview", sinks=(preview_sink,))
preview_handle = ray.klein.execute("orders-preview", sinks=(preview_sink,))
archive_handle = ray.klein.execute("orders-archive", sinks=(archive_sink,))
```

An isolated context exposes the equivalent advanced form as
`ctx.execute("orders-preview", sinks=(preview_sink,))`. Keep explicit root
selection local to code that genuinely needs multiple submission boundaries;
the normal multi-sink path is to register all terminals and execute once.

## Cancel and drain safely

`handle.cancel(timeout=seconds)` serializes with recovery and operator-rescale
operations. A successful live cancellation:

1. stops supervision and requests worker teardown;
2. attempts graceful worker stop, then force-kills survivors;
3. releases the job placement group;
4. makes a best-effort terminal checkpoint-progress flush and stops the
   checkpoint coordinator; and
5. records `CANCELLED` and wakes waiters.

The method returns `False` if the job is already terminal or if it cannot
acquire and finish the lifecycle operation within the timeout. A `False` result
does not prove that every actor has stopped; inspect the job snapshot and retry
through normal operational controls if necessary.

Cancellation is an abort, not a savepoint operation. In-flight transactional
sink work can be aborted, and the terminal metadata flush is best effort. When
you need a reproducible restore boundary, use the documented checkpoint/savepoint
workflow before replacing the deployment.

## Understand failure boundaries

### Worker or coordinator failure

The `JobManager` supervises tasks and the checkpoint coordinator. Recovery can
replace an individual task when retained upstream data permits it, or restart
the wider job from the latest checkpoint. The fixed-delay restart policy limits
restarts within a time window; exhausting that policy tears down the job and
sets `FAILED`.

Recovery can replay records and external effects. Checkpointed managed state is
restored consistently with source progress, but end-to-end delivery is still
limited by the selected sink. See [checkpoint recovery](checkpoint-recovery.md)
and [delivery semantics](delivery-semantics.md).

### Driver failure

The native streaming `JobManager` is a named, detached Ray actor. The process
that called `execute()` may exit or crash without cancelling the dataflow.
Connect another driver to the same cluster and use the recorded namespace to
inspect or cancel it. This does not apply when the first driver is inside
`handle.wait()` and receives an interrupt, because `wait()` deliberately tries
to cancel.

### JobManager failure

Ray is configured to restart the `JobManager` actor and retry its actor tasks.
However, a newly constructed manager cannot currently reconstruct an already
submitted logical graph from durable storage. Actual loss of the JobManager
process therefore requires resubmitting the same graph from a durable
checkpoint, despite the actor restart policy. Read
[driver fault tolerance](driver-fault-tolerance.md) before relying on detached
execution.

### Cluster or head/GCS failure

A namespace, detached actor, and Ray Object Store data live only with the Ray
cluster. Losing the head/GCS service or the entire cluster loses that live
control plane. Start a new cluster and resubmit the compatible graph with the
exact completed checkpoint configured through `execution.savepoint.path`.
Checkpoint storage must be reachable from the new cluster; local object-store
state is not a durable recovery boundary.

## Persist and apply resource plans

`explain()` returns a JSON resource plan with:

- `nodes`: `id`, `name`, `num_cpus`, `num_gpus`, `concurrency`, `batch_size`,
  and `async_buffer_size` for each logical node;
- `edges`: source ID, target ID, and partition strategy.

Persist a plan while compiling an application:

```bash
RAY_KLEIN_COMPILE_ONLY=1 \
RAY_KLEIN_RESOURCE_PLAN_PERSIST_PATH=resource-plan.json \
python pipeline.py
```

Edit only the tunable node fields, then load it for a real submission:

```bash
RAY_KLEIN_RESOURCE_PLAN_LOAD_PATH=resource-plan.json \
python pipeline.py
```

A loaded plan can override only `num_cpus`, `num_gpus`, `concurrency`,
`batch_size`, and `async_buffer_size`. Compatibility requires the same node
keys, which combine operator name and numeric ID. Keep explicit operator names
and graph construction order stable. Serialized edges are descriptive; loading
a plan does not rewrite application topology or partitioning.

Both `explain()` and `execute()` honor the load and persist variables because
they act during logical graph construction. A plan can therefore change the
JSON returned by `explain()`.

### Compile-only is not a successful data run

`RAY_KLEIN_COMPILE_ONLY` is enabled by the variable's **presence**, regardless
of its value. In that mode Klein builds the graph, skips Ray initialization and
all actors, sources, records, UDFs, and sink effects, and returns a completed
handle whose `get()` value is the logical graph.

Its `status` is `FINISHED` because compilation finished, not because the data
job ran. This distinction matters in automation: never treat a compile-only
handle as delivery evidence.

Unset all three workflow variables after use:

```bash
unset RAY_KLEIN_COMPILE_ONLY
unset RAY_KLEIN_RESOURCE_PLAN_LOAD_PATH
unset RAY_KLEIN_RESOURCE_PLAN_PERSIST_PATH
```

Otherwise a later invocation can silently compile without executing, load an
old tuning plan, or overwrite a plan artifact.

## Cleanup checklist

Normal finish, successful cancellation, and permanent failure tear down stream
workers, stop the coordinator, and release the placement group. Forced cleanup
kills workers that did not stop gracefully. Cleanup does **not** undo external
sink writes or delete durable checkpoint files.

After a job ends:

1. Confirm the terminal status and capture failure details or the last completed
   checkpoint before disposing of deployment metadata.
2. Confirm no active job remains before reusing an explicit namespace. A
   terminal detached `JobManager` can remain addressable, and the cluster-wide
   state actor retains a bounded in-memory terminal history. `ray-klein list`
   hides terminal entries by default; use `ray-klein list --all` to inspect them.
3. Apply the application's checkpoint-retention policy separately. Cancellation
   does not erase checkpoints, staging artifacts governed by a connector, or
   committed external data.
4. Select sinks explicitly for each submission. Successful submission consumes
   those pending sink registrations. If using the advanced isolated API,
   discard its `KleinContext` when finished; it has no runtime `close()` method.
5. Unset compile-only and resource-plan environment variables used by build or
   deployment tooling.

There is no public terminal-job purge operation. Use unique automatic
namespaces for routine submissions, and manage cluster lifetime through the Ray
deployment rather than calling internal actor-kill helpers.

For production procedures, continue with [deployment](deployment.md),
[observability](observability.md), [checkpoint recovery](checkpoint-recovery.md),
and [troubleshooting](troubleshooting.md).
