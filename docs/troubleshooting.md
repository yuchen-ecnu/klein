---
myst:
  html_meta:
    description: "Troubleshoot Klein for Ray installation, execution-mode, connector, checkpoint, watermark, backpressure, and CLI problems."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Troubleshooting

Start by saving the job plan, namespace, effective explicit configuration,
Klein/Ray versions, first exception, and recent checkpoint history. Later actor
failures are often consequences of the first error.

```python
import ray

print(ray.__version__)
print(ray.klein.__version__)
print(ray.klein.current_context().config.to_dict())
print(ray.klein.explain("diagnostic-plan"))
```

Never paste credentials, complete connector configuration, arbitrary records,
or checkpoint payloads into an issue or log bundle.

## Installation and imports

### `ModuleNotFoundError` for a connector dependency

Install the matching extra on every worker image, not only on the submitting
machine:

```bash
python -m pip install "ray-klein[kafka]"
python -m pip install "ray-klein[redis]"
python -m pip install "ray-klein[rocksdb]"
python -m pip install "ray-klein[rocketmq]"
python -m pip install "ray-klein[serve]"
```

RocketMQ additionally needs a compatible native `librocketmq`. A Python wheel
being importable on the driver does not prove the native library exists on a
remote worker.

### Dynamic `read_*` or `stream.data` method is unavailable

Check the installed Ray API before building the graph:

```python
if not ray.klein.current_context().data.available("read_parquet"):
    raise RuntimeError("read_parquet is unavailable in this Ray version")
```

Klein discovers public Ray Data factories and Dataset methods at runtime. Stay
inside the Ray range in [Compatibility](compatibility.md).

## Planning and execution mode

### `Batch function is not defined`

The graph selected batch execution but contains a native-streaming-only source,
operator, or sink. Check [Operator compatibility](operator-compatibility.md).
Set `execution.runtime.mode=streaming` only when every node supports native
streaming; changing the mode cannot give a Ray Data-only operation a streaming
implementation.

### A bounded custom source selects the wrong mode

Source boundedness does not prove that a custom `SourceFunction` has a Ray Data
lowering. Explicitly select streaming for a bounded native source. Use a Ray
Data factory or `from_ray_dataset()` when batch execution is required.

### Streams from different contexts cannot be combined

Every graph belongs to one `KleinContext`. Rebuild all branches from the same
context; do not join or union process-global streams with streams built by an
isolated context.

### SQL fails during planning

Confirm whether the selected mode supports the statement. Continuous SQL
supports a smaller subset than batch SQL and deliberately rejects unsupported
CTEs, outer joins, arbitrary ordering, window syntax, and other forms rather
than changing semantics. Check table changelog compatibility before attaching a
sink.

## Jobs and CLI

### `ray-klein list` cannot query the Ray state API

Confirm that the client can reach the Ray cluster, the dashboard/state service
is enabled, and `ray.init(address="auto")` would attach to the intended
cluster. Network policy must allow the client to reach the cluster control
plane.

### `No JobManager found in namespace ...`

Use `ray-klein list` to copy the exact namespace. An automatically generated
namespace includes a sanitized job name and an eight-character suffix. A job
ID from the state snapshot API is not interchangeable with every CLI argument.

### `attach` says stdout is not a TTY

Live progress rendering requires an interactive terminal. Use `status` or the
JSON-safe state API from automation.

### Ctrl+C did not stop the job

Ctrl+C while attached detaches intentionally. Run
`ray-klein stop <namespace>` or call `JobHandle.cancel()` to cancel the detached
job.

## Checkpoints and recovery

### No completed checkpoint appears

Check that at least one trigger is enabled, sources are still emitting idle or
checkpoint control, all sinks acknowledge barriers, and storage is writable.
Setting both trigger intervals to zero disables periodic barriers. A blocked
source that never calls `on_idle()` can also delay time-based progress.

### Restore silently starts from the beginning

Use the exact key `execution.savepoint.path` and pass the full completed
`chk-N` URI. Unknown configuration keys are retained, so a misspelled restore
key does not necessarily raise an error.

### Restore reports checksum or metadata errors

Do not edit `_metadata` or serialized state. Verify storage consistency and
permissions, then try an earlier retained checkpoint. An unsupported format
version requires the matching Klein release or an explicit migration.

### Restore fails after changing concurrency

Keep `state.keyed.max-parallelism`, operator identity, state descriptor names,
and serializers stable. New concurrency must not exceed max parallelism. See
[Restore and rescale](checkpoint-recovery.md).

## Event time and state

### Windows never emit

Verify all of the following:

- timestamps are non-negative integer milliseconds;
- a watermark strategy or source emits watermarks;
- every active physical input has emitted its first watermark;
- empty inputs are marked idle;
- the watermark has passed the window end plus allowed lateness.

Records carrying timestamps do not advance event time by themselves.

### `late_records_dropped` rises

Compare record timestamps with the current input/output watermark and
`allowed_lateness`. Look for clock/unit mistakes—seconds interpreted as
milliseconds are common—and for partitions reactivating with old data. Raising
allowed lateness retains more state and delays window cleanup.

### Managed state grows without bound

Confirm that the key cardinality is expected, add descriptor TTL or an operator
`state_ttl` where semantics allow, and verify watermark progress for
event-time-cleaned state. TTL is a correctness choice: expiration can make late
future results incomplete.

## Backpressure and memory

### Throughput falls while input buffers stay full

Find the first downstream operator with rising processing latency or external
call duration. Increase capacity there or reduce its work before enlarging
upstream buffers. Check hot keys, sink throttling, and checkpoint alignment.

### Replay buffer exceeds its byte limit

This is a safety failure, not a request to raise the limit automatically.
Inspect downstream acknowledgements, checkpoint/sink completion, network
errors, and task restarts. Raise the limit only after measuring worst-case
memory on the same node shape.

### Placement group times out

Compare the plan's total CPU/GPU bundles with currently free cluster resources.
Add capacity, reduce requests, change the placement strategy, or use balanced
deployment. Klein may fall back to another placement path, but locality and
startup time can change.

## Collect a useful report

Include:

- minimal graph code and whether the source is bounded;
- `ray.klein.explain()` output;
- Klein, Ray, Python, operating system, and connector versions;
- sanitized explicit configuration;
- namespace and job status transitions;
- first traceback and relevant component logs;
- checkpoint revision/path shape without credentials;
- a small metric window covering the failure.

Use the repository's
[support policy](https://github.com/yuchen-ecnu/klein/blob/main/SUPPORT.md) for
help channels and
[security policy](https://github.com/yuchen-ecnu/klein/blob/main/SECURITY.md)
for vulnerabilities or reports containing sensitive infrastructure details.
