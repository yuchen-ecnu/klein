---
myst:
  html_meta:
    description: "Use public Ray Data readers, transforms, consumers, and writers through Klein."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray Data

The Ray Data adapter exposes public Ray Data `Dataset` factories and methods as
bounded Klein sources and operations. It is the broadest batch connector: when
Ray Data adds a public reader or writer, a compatible Klein installation can
expose it without a Klein wrapper release.

## Availability and execution mode

Ray Data sources, general `stream.ray_data` transforms, and
`stream.ray_data` consumers are batch-only. The expression forms
`stream.ray_data.with_column(name, expr)` and
`stream.ray_data.filter(expr=expr)` are dual-mode: streaming execution uses
Klein's native expression operator. Non-download batch expressions delegate to
Ray Data; `download()` uses Klein's shared `sql.download.*` network and byte
policy in both modes, with one in-flight streaming request per task. The
shorter `.data` namespace is a compatibility alias.

Check availability before relying on an API that differs across Ray versions:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="ray-data-check")
if "read_parquet" not in pipeline.ray_data.available:
    raise RuntimeError("This Ray version does not expose read_parquet")

stream = pipeline.ray_data.read_parquet("s3://warehouse/events/")
if "map_batches" not in stream.ray_data.available:
    raise RuntimeError("This Ray version does not expose Dataset.map_batches")
```

Klein supports the Ray version range stated in
[Compatibility](../compatibility.md). Public Ray Data APIs outside that range
are not compatibility guarantees.

## Read data

Call a public Ray Data factory through an explicit pipeline's `ray_data`
namespace:

```python
import ray.klein as klein

pipeline = klein.pipeline(name="read-events")
parquet = pipeline.ray_data.read_parquet("s3://warehouse/events/")

json_rows = pipeline.ray_data.read_json("s3://warehouse/events-json/")

csv_rows = pipeline.ray_data.source(
    "read_csv",
    "s3://warehouse/events.csv",
    override_num_blocks=32,
)
```

You can also pass a public Ray Data factory callable instead of its name. Klein
preserves the installed function's signature and documentation, and forwards
all arguments to Ray Data. Consult the matching
[Ray Data input API](https://docs.ray.io/en/latest/data/api/input_output.html)
for connector-specific options, credentials, schemas, and return values.

## Transform data

`stream.ray_data` dynamically exposes public `Dataset` methods:

```python
result = (
    pipeline.ray_data.read_parquet("s3://warehouse/events/")
    .ray_data.map_batches(normalize, batch_format="pyarrow")
    .ray_data.filter(lambda row: row["amount"] > 0)
)
```

Use `transform` when an operation needs more than one stream or is easier to
express as a Dataset-to-Dataset function:

```python
joined = left.ray_data.transform(
    lambda left_ds, right_ds: left_ds.join(right_ds, num_partitions=64),
    right,
)
```

The callable passed to `transform` must return exactly one Ray `Dataset`.
Additional `DataStream` arguments become graph dependencies and are replaced
with their lowered Datasets at execution time.

## Consume or write data

Public terminal Dataset methods are available through `stream.ray_data`, including
writers supported by the installed Ray version:

```python
stream.ray_data.write_parquet("s3://warehouse/output/").wait()

count = stream.ray_data.consume(lambda dataset: dataset.count()).result()
```

Unlike `transform`, `consume` may return any value accepted by the underlying
terminal operation. The result is represented as a Klein sink and becomes part
of the submitted job.

For JSON, CSV, and Parquet, `stream.write_json`, `write_csv`, and
`write_parquet` provide a stable Klein entry point that lowers to Ray Data in
batch and uses checkpoint-transactional native output in streaming. See
[Filesystem](filesystem.md) before choosing between the APIs.

`stream.write_sql(sql, connection_factory, ray_remote_args=None,
concurrency=None)` is also available with the same arguments as
`ray.data.Dataset.write_sql`. Batch execution uses Ray Data's writer. Streaming
execution owns one DB-API 2.0 connection per sink subtask, preserves the first
record's column order, and commits `executemany` batches of up to 128 rows at
full batches and checkpoint flushes.

Streaming SQL output is at-least-once: a database commit can succeed before
the matching Klein checkpoint becomes durable. Use an idempotent statement or
database-native upsert when replayed rows must not create duplicates.

## Adapt an existing Dataset

Use `pipeline.ray_data.from_dataset()` when another library has already
constructed a Dataset:

```python
import ray.data

dataset = ray.data.from_items([{"id": 1}, {"id": 2}])
stream = pipeline.ray_data.from_dataset(dataset)
```

The Dataset remains bounded and batch-only. Klein does not collect it into the
driver or convert it to a native streaming source. The module-level
`from_ray_data()` function provides the same adapter for the deferred API;
`from_ray_dataset()` remains its compatibility alias.

## Configuration and guarantees

There are no Ray Data connector-wide Klein options. Pass connector and resource
arguments to the selected Ray Data method. Klein's job-wide
`execution.runtime.mode`, retry, and Ray initialization options still apply;
see the [configuration reference](../configuration-reference.md).

Except for the native `stream.write_sql` path described above, data
partitioning, retries, commit behavior, and filesystem semantics are those of
the invoked public Ray Data operation. They are not Klein streaming
checkpoints. Use a transactional native connector when the external system
must commit only after a Klein checkpoint becomes durable.
