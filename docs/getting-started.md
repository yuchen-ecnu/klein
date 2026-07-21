---
myst:
  html_meta:
    description: "Install Klein for Ray and build your first bounded and streaming data pipelines."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-getting-started)=
# Get started with Klein for Ray

This guide creates a bounded `DataStream`, executes it explicitly, and shows how to submit a long-running pipeline.

## Install Klein

Create an isolated Python environment and install the Alpha release:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "ray-klein==0.1.0a1"
```

Kafka, Iceberg, RocketMQ, Redis, RocksDB, and Serve are optional integrations. Install
only the extra required by the application, for example `ray-klein[kafka]`,
`ray-klein[iceberg]`, `ray-klein[rocketmq]`, `ray-klein[redis]`,
`ray-klein[rocksdb]`, or `ray-klein[serve]`. Use `ray-klein[all]` for an
integration development environment. RocketMQ also requires the native
`librocketmq` runtime on every worker.

## Run a bounded pipeline

A terminal operation such as `take_all()` registers a sink. Call
`execute("job-name")` to run every registered sink in the bounded graph:

```python
import ray
import ray.klein

stream = (
    ray.klein.from_items(
        [
            {"name": "Ada", "amount": 4},
            {"name": "Grace", "amount": 7},
        ]
    )
    .map(lambda row: {**row, "amount": row["amount"] * 2})
)
stream.take_all()
rows = ray.klein.execute("quick-start").get()

print(rows)
```

The completed job handle returns these rows:

```text
[{'name': 'Ada', 'amount': 8}, {'name': 'Grace', 'amount': 14}]
```

Klein creates a lazy graph through the registered terminal sinks. `execute()`
selects batch execution for the bounded source and lowers the graph to Ray Data.

## Read data with Ray Data

Source construction follows `ray.data`. Call a reader directly from `ray.klein`, then choose Klein or Ray Data transformations:

```python
import ray
import ray.klein

events = ray.klein.read_parquet("s3://<bucket>/events/")

# Use Klein's DataStream semantics.
filtered = events.filter(lambda row: row["status"] == "ready")

# Use the installed Ray Data Dataset implementation.
shuffled = filtered.data.random_shuffle(seed=7)
shuffled.data.take(10)
rows = ray.klein.execute("inspect-events").get()
```

Klein forwards reader and Dataset arguments to the installed Ray version. See [Ray Data interoperability](ray-data-interop.md) for the execution boundary and advanced adapters, or the [connector catalog](connectors/index.md) to choose an input or output and review all of its options.

## Submit a dataflow

Register one or more terminal sinks, then submit them together:

```python
import ray
import ray.klein

events = ray.klein.from_items([{"id": 1}, {"id": 2}, {"id": 3}])
doubled = events.map(lambda row: {"id": row["id"] * 2})
doubled.show()
doubled.filter(lambda row: row["id"] >= 4).show()

print(ray.klein.explain("doubled-events"))
job = ray.klein.execute("doubled-events")
job.wait()
```

`explain()` returns the dataflow plan without submitting it. `execute()` returns a job handle that you can use to wait for completion or inspect status.

Bounded sources complete after producing all records. Streaming sources, such as Kafka or a custom `SourceFunction`, keep the job active until you stop it or the source terminates.

```python
events = ray.klein.read_kafka(
    "events",
    bootstrap_servers="localhost:9092",
    trigger="continuous",
    start_offset="latest",
    concurrency=4,
)

events.write_kafka(
    "processed-events",
    "localhost:9092",
    key_field="event_id",
    value_serializer="json",
    concurrency=4,
)
job = ray.klein.execute("processed-events")
```

The Kafka source emits the same raw byte schema as `ray.data.read_kafka`. It
discovers new partitions while running, marks empty inputs idle for watermark
progress, and resumes from the next offsets stored in the latest checkpoint.
For bounded jobs, `write_kafka` uses Ray Data. For streaming jobs, Klein owns a
producer per sink subtask and waits for Kafka delivery acknowledgements before
advancing checkpoint and replay watermarks. This provides at-least-once
delivery: failures can replay a message, so downstream consumers must tolerate
duplicates when exactly-once processing is required.

## Configure a pipeline

Use a mapping, a `key=value` string, typed options, or `RAY_KLEIN_*` environment variables. Explicit code takes precedence over environment values:

```python
import ray
import ray.klein

ray.klein.configure(
    {
        "execution.checkpointing.dir": "s3://<bucket>/klein-checkpoints",
        "state.backend.type": "rocksdb",
        "state.keyed.max-parallelism": 32768,
    }
)
```

This example selects the optional RocksDB backend; install `.[rocksdb]` first.
The dependency-free default is `memory`.

See [Configure Klein](configuration.md) for precedence and value conversion,
then use the [configuration reference](configuration-reference.md) to find every
supported key, default, constraint, and environment variable.

## Next steps

- Read [Key concepts](key-concepts.md) to understand execution, state, event time, and recovery.
- Build the [production streaming walkthrough](production-streaming.md) when
  you are ready to connect Kafka, watermarks, state, checkpoints, and
  transactional file output.
- Check the [operator compatibility matrix](operator-compatibility.md) before
  mixing Ray Data and native streaming operations.
- Follow the [user guides](user-guides.md) to build stateful pipelines and configure production storage.
- Browse the {doc}`API reference <api/api>` for public methods and configuration options.
