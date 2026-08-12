---
myst:
  html_meta:
    description: "Install Klein and build your first bounded and streaming data pipelines."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-getting-started)=
# Get started with Klein

This guide creates an explicit `Pipeline`, runs a bounded `DataStream`, and shows how to submit a long-running dataflow.

## Install Klein

Create an isolated Python environment and install the current source checkout:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
git clone https://github.com/yuchen-ecnu/klein.git
cd klein
python -m pip install .
```

Kafka, Iceberg, RocketMQ, Redis, RocksDB, and Serve are optional integrations. Install
only the extra required by the application, for example `ray-klein[kafka]`,
`ray-klein[iceberg]`, `ray-klein[rocketmq]`, `ray-klein[redis]`,
`ray-klein[rocksdb]`, or `ray-klein[serve]`. Use `ray-klein[all]` for an
integration development environment. RocketMQ also requires the native
`librocketmq` runtime on every worker.

## Run a bounded pipeline

`klein.pipeline()` isolates one logical job and rejects misspelled Klein
configuration keys by default. A single terminal submits itself; `result()`
waits for and returns collected rows:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="quick-start")
rows = (
    pipeline.from_items(
        [
            {"name": "Ada", "amount": 4},
            {"name": "Grace", "amount": 7},
        ]
    )
    .map(lambda row: {**row, "amount": row["amount"] * 2})
    .collect()
    .result()
)

print(rows)
```

The completed job handle returns these rows:

```text
[{'name': 'Ada', 'amount': 8}, {'name': 'Grace', 'amount': 14}]
```

Klein keeps transformations lazy until `collect()` creates and submits the
single terminal. Batch execution lowers the graph to Ray Data. The module-level
`klein.read_*` and `klein.from_*` builders plus `klein.execute()` remain
available for applications that prefer one implicit, deferred pipeline.

## Read data with Ray Data

Source construction follows `ray.data`. Use the explicit `ray_data` namespace
when calling an installed Ray Data reader or Dataset method:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="inspect-events")
events = pipeline.ray_data.read_parquet("s3://<bucket>/events/")

# Use Klein's DataStream semantics.
filtered = events.filter(lambda row: row["status"] == "ready")

# Use the installed Ray Data Dataset implementation.
shuffled = filtered.ray_data.random_shuffle(seed=7)
rows = shuffled.ray_data.take(10).result()
```

Klein forwards reader and Dataset arguments to the installed Ray version. See [Ray Data interoperability](ray-data-interop.md) for the execution boundary and advanced adapters, or the [connector catalog](connectors/index.md) to choose an input or output and review all of its options.

## Submit a dataflow

Use a `StatementSet` when multiple side-effect sinks must share one job:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="doubled-events")
events = pipeline.from_items([{"id": 1}, {"id": 2}, {"id": 3}])
doubled = events.map(lambda row: {"id": row["id"] * 2})
statements = pipeline.create_statement_set()
statements.add(doubled.show)
statements.add(doubled.filter(lambda row: row["id"] >= 4).show)

print(statements.explain())
job = statements.execute()
job.wait()
```

`StatementSet.explain()` returns the combined dataflow plan without submitting
it. `StatementSet.execute()` submits every added sink as one job. Calling a
single `write_*()` outside a statement set submits that sink directly.

Bounded sources complete after producing all records. Streaming sources, such
as Kafka or a custom `SourceFunction`, keep the job active until you stop it or
the source terminates.

```python
import ray.klein as klein

pipeline = klein.pipeline(
    {"execution.runtime.mode": "streaming"},
    name="processed-events",
)
events = pipeline.read_kafka(
    "events",
    bootstrap_servers="localhost:9092",
    trigger="continuous",
    start_offset="latest",
    concurrency=4,
    value_format="json",
)

job = events.write_kafka(
    "processed-events",
    "localhost:9092",
    key_field="event_id",
    value_serializer="json",
    concurrency=4,
)
```

With `value_format="json"`, the source decodes each UTF-8 JSON object value.
The default `raw` format emits the same byte schema as `ray.data.read_kafka`.
The source discovers new partitions while running, marks empty inputs idle for
watermark progress, and resumes from the next offsets stored in the latest
checkpoint.
For bounded jobs, `write_kafka` uses Ray Data. For streaming jobs, Klein owns a
producer per sink subtask and waits for Kafka delivery acknowledgements before
advancing checkpoint and replay watermarks. This provides at-least-once
delivery: failures can replay a message, so downstream consumers must tolerate
duplicates when exactly-once processing is required.

## Configure a pipeline

Use a mapping, a `key=value` string, typed options, or `RAY_KLEIN_*` environment variables. Explicit code takes precedence over environment values:

```python
import ray.klein as klein

pipeline = klein.pipeline(
    {
        "execution.runtime.mode": "streaming",
        "execution.checkpointing.dir": "s3://<bucket>/klein-checkpoints",
        "state.backend.type": "rocksdb",
        "state.keyed.max-parallelism": 32768,
    },
    name="orders",
)
```

This example selects the optional RocksDB backend; install `.[rocksdb]` first.
The dependency-free default is `memory`. Explicit pipelines report unknown
keys immediately and suggest close canonical names. The compatibility
module-level API can receive the same mapping through `klein.configure()`
before source construction.

Use `strict_config=False` only when the mapping intentionally includes
application-owned metadata alongside Klein options.

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
