---
myst:
  html_meta:
    description: "Understand Ray Data autoscaling and safely change a running Klein streaming operator's parallelism."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Autoscaling and live operator rescaling

Scaling has three independent layers in Klein. Choose the layer that matches
the execution mode and the resource you need to change:

| Layer | Execution mode | Decision owner | Klein entry point |
|---|---|---|---|
| Ray Data worker pool | Batch | Ray Data | `concurrency=(min, max)` on a callable-class transform |
| Klein operator parallelism | Streaming | User, Dashboard, or external controller | `rescale_operator()` |
| Ray cluster nodes | Batch and streaming | Ray cluster autoscaler | Ray cluster configuration |

Klein does not currently include a metric-driven controller that automatically
changes streaming parallelism. The Dashboard and `rescale_operator()` perform
an explicit, live change to one operator. The Dashboard submits the change
asynchronously; the Python function waits for its topology result. Ray's
cluster autoscaler can provide nodes for that request, but it does not choose
the operator's parallelism.

:::{important}
`DataStream.rescale()` is a partitioning method. It limits which downstream
subtasks each upstream subtask can address; it does not add or remove actors.
:::

## Autoscale a bounded transform

In batch mode, Klein lowers compatible transforms to Ray Data and forwards
their concurrency setting. A callable class uses Ray actors, so a `(min, max)`
range gives Ray Data an autoscaling actor pool:

```python
import ray
import ray.klein


class Enrich:
    def __call__(self, row):
        return {**row, "normalized": row["value"] / 100}


ray.klein.configure({"execution.runtime.mode": "batch"})

stream = (
    ray.klein.from_items([{"value": 10}, {"value": 30}])
    .map(Enrich, concurrency=(2, 8), num_cpus=1)
)
stream.take_all()
rows = ray.klein.execute("batch-enrichment").get()
```

Ray Data owns the utilization policy and the actor lifecycle. The range limits
workers, not cluster nodes, and each live worker requests the transform's
`num_cpus` and `num_gpus`. See Ray Data's
[transform concurrency guide](https://docs.ray.io/en/latest/data/transforming-data.html#specifying-concurrency)
for the policy in the installed Ray version.

The same tuple has different behavior in Klein's native streaming runtime:
`(2, 8)` starts the operator at the lower bound, `2`. It neither triggers a
streaming policy nor caps a later `rescale_operator(..., parallelism=...)`
request. Set explicit policy bounds in an external controller if one is
required.

## Prepare a streaming job for live rescaling

Live rescaling changes one running operator without restarting the entire job.
The request is accepted only when all of these conditions hold:

| Requirement | Current behavior |
|---|---|
| Job health | The job and every task instance must be `RUNNING`. |
| Source topology | Multiple source operators and source subtasks are coordinated within checkpoint domains. |
| Target | Source, transactional-sink, and collecting-sink operators are not supported. |
| Managed state | Target parallelism cannot exceed `state.keyed.max-parallelism`. |
| Coordination | Only one rescale runs at a time; wait for its stabilization checkpoint before the next one. |
| Capacity | Scale-out needs schedulable resources for the added subtasks. |

Set `state.keyed.max-parallelism` before the first checkpoint and keep it stable:

```python
ray.klein.configure({
    "execution.runtime.mode": "streaming",
    "execution.checkpointing.dir": "s3://data-platform/klein-checkpoints",
    "state.keyed.max-parallelism": 128,
})
```

The Dashboard and stable state API require job snapshot publication through
`observability.dashboard.enabled`, which is enabled by default.

Use stable, unique operator names when constructing the graph. The Dashboard
shows numeric operator IDs, current parallelism, and a runtime-provided reason
when an operator cannot be resized.

Operator chaining is enabled by default. Compatible stateless operators on a
forward edge can become one physical operator, including a chain rooted at a
source that cannot be resized. Treat the published operator snapshot as the
authoritative set of scaling units. Set
`pipeline.operator-chaining.enabled=false` before submission when a transform
must remain independently rescalable.

## Resize an operator

Start the bundled Dashboard from a machine that can connect to the Ray cluster:

```bash
ray-klein dashboard --open
```

Select a running job, enter the target parallelism on an enabled operator, and
apply the change. The server returns as soon as the JobManager accepts the
operation. You can close the operator panel, navigate away, or close the page;
the JobManager continues the rescale. The execution graph and operator table
are driven by the polled job snapshot, so reopening the page reconstructs the
current target and phase instead of relying on browser-local state.

The Dashboard shows these operation states:

| Status | Dashboard meaning |
|---|---|
| `ACCEPTED` | The operation is queued by the JobManager. |
| `RUNNING` | Task instances and routes are being coordinated. |
| `STABILIZING` | The new parallelism is committed and its stabilization checkpoint is pending. |
| `COMPLETED` | The topology and stabilization checkpoint are complete. |
| `NOOP` | The requested parallelism already matched the operator. |
| `REJECTED` | A precondition or request was invalid. |
| `FAILED` | The runtime operation failed; the snapshot reports the retained topology and error. |

The latest per-operator record is available as
`operator["rescale_operation"]`; recent job-level records are available as
`job["rescale_operations"]`. Both remain observable across page refreshes.
Only one operation can run at a time, and the runtime keeps later rescale
controls disabled through `STABILIZING`.

See [Observe Klein jobs](observability.md) before exposing the Dashboard beyond
its loopback listener.

Automation that needs to wait for the topology result can use the stable,
synchronous state API:

```python
import ray
import ray.klein

ray.init(address="auto")

jobs = ray.klein.list_job_snapshots()
job = next((item for item in jobs if item["status"] == "RUNNING"), None)
if job is None:
    raise RuntimeError("No published RUNNING Klein job")

for operator in job["operators"]:
    print(
        operator["op_id"],
        operator["name"],
        operator["parallelism"],
        operator["can_rescale"],
        operator["rescale_disabled_reason"],
    )

target_name = "Enrich"
target = next(
    (
        operator
        for operator in job["operators"]
        if operator["name"] == target_name and operator["can_rescale"]
    ),
    None,
)
if target is None:
    raise RuntimeError(f"No rescalable operator named {target_name!r}")

result = ray.klein.rescale_operator(
    job["job_id"],
    operator_id=target["op_id"],
    parallelism=4,
    timeout=60,
)
if result is None:
    raise RuntimeError("The job is no longer published")
print(result)
```

The result is JSON-safe and has one of these statuses:

| Status | Meaning | Next action |
|---|---|---|
| `COMPLETED` | The topology change committed. | Wait for the stabilization checkpoint, then validate rates and lag. |
| `NOOP` | The target already has that parallelism. | No action is required. |
| `REJECTED` | A precondition or request was invalid. | Read `error` and correct the request. |
| `FAILED` | The runtime operation raised an error. | Refresh the job snapshot before deciding whether to retry. |

A result of `None` means that the job is no longer published in this cluster.
A client timeout only stops that caller from waiting. The remote operation can
still commit. Likewise, a `FAILED` response can follow the topology commit if a
later checkpoint-gate response fails. Never retry blindly; first refresh
`get_job_snapshot()` and compare the operator's current parallelism.

## What happens at the local cut?

1. On scale-out, Klein creates and pings only the added actors. Subtasks whose
   indexes exist at both parallelisms retain their Ray actor identities.
2. The coordinator pauses new checkpoints and waits for in-flight checkpoint
   completion before changing the data plane.
3. Direct upstream tasks insert an ordered local barrier and pause at that cut.
   A directly connected source pauses cooperatively at a record boundary;
   unrelated regions keep running.
4. The target aligns the barriers, snapshots managed state, and fences its
   direct downstream tasks. Retained and added actors prepare the new runtime
   and their assigned state while the old runtime remains available.
5. Klein commits the new topology and routing atomically. A pre-commit failure
   rolls back to the old runtime. On scale-in, only surplus actors are stopped
   after commit.
6. The source emits an ordinary stabilization checkpoint. Until it becomes
   durable, a task failure deliberately escalates to consistent global
   checkpoint recovery instead of restoring one task from stale state.

Keyed state moves by stable key group; keys are not rehashed with Python's
process-local `hash()`. The operation does not change connector delivery
guarantees, so sinks still need the idempotency or transaction design described
in [Delivery semantics](delivery-semantics.md).

An incident `FORWARD` edge is valid only while its endpoints have equal
parallelism. Klein changes that edge to its normal shuffle choice when a
rescale makes the counts differ. Explicit non-forward partitioners are
preserved.

## Coordinate with Ray cluster autoscaling

Ray's cluster autoscaler is outside Klein. A streaming scale-out creates actor
CPU/GPU demands that an already configured cluster autoscaler can satisfy, but
node provisioning must finish within `job.scheduler.start.timeout`. The
operator request can otherwise time out while waiting for its added actors.

Existing job-wide PlacementGroup bundles are not resized. Added scale-out
actors use Ray's native placement, and bundles made surplus by scale-in remain
reserved until the job ends. A streaming scale-in therefore does not guarantee
that the cluster autoscaler can remove a node.

## Validate and automate safely

After a completed request, verify that:

- the operator snapshot reports the target `parallelism` and all instances are
  running;
- `job["checkpoints"]["rescale_recovery_fenced"]` becomes `False` after a new
  completed checkpoint;
- source lag, throughput, busy time, and backpressure move in the expected
  direction; and
- checkpoint duration, external connections, and sink fan-out remain within
  their operating limits.

An external autoscaling controller should use both interval averages and the
hottest-subtask values, require several consecutive samples, apply hysteresis
and a cooldown, and enforce explicit minimum and maximum parallelism. It should
submit a request only while `can_rescale` is true, serialize changes, and wait
for the recovery fence to clear. Backpressure alone is not sufficient: a slow
sink, broker limit, or hot key can get worse when parallelism increases.

For symptom-oriented tuning, see [Tune Klein performance](performance-tuning.md).
For changing parallelism while resubmitting from a durable checkpoint, see
[Restore and rescale a job](checkpoint-recovery.md).
