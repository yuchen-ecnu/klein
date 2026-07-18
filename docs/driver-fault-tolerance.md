---
myst:
  html_meta:
    description: "Keep Klein streaming jobs alive across Ray driver failure and understand the Compiled Graph trade-off."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-driver-fault-tolerance)=
# Survive driver failure

Klein streaming jobs do not fate-share with the submitting driver. Every job
uses a named, detached `JobManager` Ray actor with unlimited actor restarts and
task retries. The JobManager owns the coordinator and stream tasks, so exiting
or crashing the client process does not cancel the server-side dataflow.

Record the namespace returned by `handle.namespace`, or configure a stable one:

```python
import ray

ray.klein.configure({"job.namespace": "orders-production"})
handle = pipeline.execute("orders")
print(handle.namespace)
```

Another driver can connect to the same Ray cluster and use `ray-klein attach`,
`ray-klein status`, or the public state snapshot API. Explicit cancellation is
required for unbounded jobs; detached actors intentionally outlive clients.

## Failure boundaries

Driver survival is not cluster durability. Loss of the Ray head/GCS process or
the entire cluster requires a new submission restored from a durable checkpoint
URI. Store checkpoints in S3-compatible or Google Cloud Storage for that case.
The Ray Object Store accelerates live recovery, but it is never the durable
boundary.

The JobManager actor can restart after its process fails, but Python actor
constructor state alone cannot reconstruct a submitted graph. Worker and
coordinator failures are recovered in place from checkpoints; recovery after
loss of the JobManager process currently requires resubmission with the same
job definition and `execution.checkpointing.restore-path`. This limitation is
part of the alpha compatibility contract.

## Why Klein does not require Ray Compiled Graph

[Ray Compiled Graph](https://docs.ray.io/en/latest/ray-core/compiled-graph/ray-compiled-graph.html)
optimizes repeated execution of a static actor DAG. Klein's unbounded runtime
has dynamic barriers, watermarks, backpressure, rescaling, and actor replacement.
Compiled Graph does not replace detached actor ownership or durable checkpoints,
and making it mandatory would constrain those recovery paths. Klein therefore
keeps it as a future opt-in data-plane optimization, not a control-plane
availability mechanism.
