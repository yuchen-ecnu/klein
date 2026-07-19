---
myst:
  html_meta:
    description: "Deploy Klein for Ray applications to an existing Ray cluster, Ray Jobs, containers, and KubeRay."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Deploy Klein jobs

A production deployment has three independent pieces:

1. a Ray cluster with enough worker resources and observability enabled;
2. an identical Python environment on every node that may run a Klein task;
3. a submission process that builds the graph, records the job namespace, and
   exits or attaches without owning the job's lifetime.

Klein's streaming JobManager is a named detached actor. Losing the submitting
process does not stop the job, but losing the whole Ray cluster requires a new
submission from durable checkpoint storage.

## Package the application

Pin Klein and the tested Ray minor together. Install only the extras required by
the graph, on the driver and every eligible worker:

```text
ray-klein[kafka,rocksdb]==<release>
ray[data]>=2.56.1,<2.57
```

Connector-specific native dependencies also belong in the worker image. For
example, RocketMQ requires a compatible `librocketmq`; RocksDB uses
`rocksdict`; Serve needs `ray[serve]`, `aiohttp`, and `orjson`.

Build the image or runtime environment before submission. Do not install
dependencies dynamically inside a UDF: that makes worker startup
nondeterministic and can exceed deployment timeouts.

## Connect to a cluster

Initialize Ray explicitly when the application needs a remote address,
dashboard, metrics exporter, runtime environment, or custom resources:

```python
import ray

ray.init(address="auto")

ctx = ray.klein.reset_context({
    "job.namespace": "orders-production",
    "execution.checkpointing.dir": "s3://platform-checkpoints/klein",
})

# Build sources, transforms, and sinks.
handle = ctx.execute("orders")
print(handle.namespace)
```

If Ray is not initialized, native streaming execution starts an embedded local
cluster. That is convenient for development but is not a production deployment
strategy.

## Submit with Ray Jobs

Keep graph construction in a normal Python entry point, for example
`pipeline.py`, and submit it with the Ray Jobs mechanism used by the cluster:

```bash
ray job submit \
  --address http://ray-head:8265 \
  --working-dir . \
  -- python pipeline.py
```

The submission process may remain attached, or it may exit after printing the
Klein namespace. The detached streaming actors continue running. Capture the
Ray Jobs submission ID, Klein namespace, source consumer identity, code version,
and checkpoint root in deployment metadata.

For container or KubeRay deployments, put the same wheel/application artifact
in the head and worker images or provide an immutable Ray runtime environment.
Mount node-local SSD for RocksDB working state when used, and provide object
storage credentials through workload identity or the platform's secret
mechanism rather than source code.

## Size resources

Each source, transform, and sink declares `num_cpus`, `num_gpus`, and
`concurrency`. The streaming scheduler needs resources for all live subtasks,
the JobManager, checkpoint coordinator, and optional Serve client actors.

Before submitting:

```python
print(ctx.explain("orders"))
```

Review the plan for unexpected shuffles, fan-out, concurrency, and resource
requests. Placement groups are attempted by default; if the reservation cannot
be satisfied before its timeout, Klein falls back according to the configured
deployment mode. A fallback can start the job but change locality and
performance, so alert on deployment warnings.

The Ray Object Store must hold in-flight data and any hot checkpoint snapshots.
It is not durable storage. Size it together with
`pipeline.input-buffer.size`, output batching, replay-buffer limits, and the
largest expected state snapshot.

## Configure durable services

Production streaming jobs should set:

- a shared `execution.checkpointing.dir` reachable by every worker;
- enough retained checkpoints to survive one corrupt or operationally bad
  revision;
- stable external source identities where resubmission must recover progress;
- explicit output idempotency or transactional behavior;
- Ray metrics export and log retention independent of worker lifetime.

For S3-compatible storage, pass filesystem construction settings through
`execution.checkpointing.storage-options`. Prefer identity-based credentials.
Snapshot publication redacts credential-like configuration fields, but
checkpoint metadata and application logs should never receive raw secrets.

## Operate the deployed job

```bash
ray-klein list
ray-klein status orders-production
ray-klein attach orders-production
ray-klein stop --force orders-production
ray-klein dashboard --host 127.0.0.1 --port 8266
```

`attach` requires a TTY and detaches on Ctrl+C without stopping the job.
`stop` is cancellation, not savepoint creation. Confirm that an acceptable
completed checkpoint exists before an upgrade or cluster shutdown when the job
must be restored later.

The standalone dashboard has no authentication. Keep its default loopback bind,
or publish it only behind the cluster's authenticated operations proxy.

## Upgrade procedure

1. Verify the target Klein/Ray versions against [Compatibility](compatibility.md).
2. Wait for a completed durable checkpoint and record its exact `chk-N` URI.
3. Stop the old job and confirm its detached actors are gone.
4. Deploy the new immutable application artifact.
5. Resubmit with `execution.savepoint.path` and the original max parallelism.
6. Validate source position, state restore, sink behavior, and a new completed
   checkpoint before declaring success.

Alpha releases do not promise automatic checkpoint-schema migration. Test an
upgrade against a copy of production-shaped state before using it as the only
recovery path.
