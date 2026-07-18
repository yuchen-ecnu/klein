---
myst:
  html_meta:
    description: "Use public Ray Data readers, transforms, consumers, and writers through Klein for Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray Data

The Ray Data adapter exposes public Ray Data `Dataset` factories and methods as
bounded Klein sources and operations. It is the broadest batch connector: when
Ray Data adds a public reader or writer, a compatible Klein installation can
expose it without a Klein wrapper release.

## Availability and execution mode

Ray Data sources, `stream.data` transforms, and `stream.data` consumers are
batch-only. A pipeline containing only batch-lowerable nodes can execute on Ray
Data; forcing `execution.runtime.mode=streaming` rejects these nodes.

Check availability before relying on an API that differs across Ray versions:

```python
import ray

if not ray.klein.current_context().data.available("read_parquet"):
    raise RuntimeError("This Ray version does not expose read_parquet")

if not stream.data.available("map_batches"):
    raise RuntimeError("This Ray version does not expose Dataset.map_batches")
```

Klein supports the Ray version range stated in
[Compatibility](../compatibility.md). Public Ray Data APIs outside that range
are not compatibility guarantees.

## Read data

Call a public Ray Data factory directly from `ray.klein`, through the current
context, or through `source`:

```python
import ray

parquet = ray.klein.read_parquet("s3://warehouse/events/")

context = ray.klein.current_context()
json_rows = context.data.read_json("s3://warehouse/events-json/")

csv_rows = ray.klein.source(
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

`stream.data` dynamically exposes public `Dataset` methods:

```python
result = (
    ray.klein.read_parquet("s3://warehouse/events/")
    .data.map_batches(normalize, batch_format="pyarrow")
    .data.filter(lambda row: row["amount"] > 0)
)
```

Use `transform` when an operation needs more than one stream or is easier to
express as a Dataset-to-Dataset function:

```python
joined = left.data.transform(
    lambda left_ds, right_ds: left_ds.join(right_ds, num_partitions=64),
    right,
)
```

The callable passed to `transform` must return exactly one Ray `Dataset`.
Additional `DataStream` arguments become graph dependencies and are replaced
with their lowered Datasets at execution time.

## Consume or write data

Public terminal Dataset methods are available through `stream.data`, including
writers supported by the installed Ray version:

```python
stream.data.write_parquet("s3://warehouse/output/")

count_sink = stream.data.consume(lambda dataset: dataset.count())
```

Unlike `transform`, `consume` may return any value accepted by the underlying
terminal operation. The result is represented as a Klein sink and becomes part
of the submitted job.

For JSON, CSV, and Parquet, `stream.write_json`, `write_csv`, and
`write_parquet` provide a stable Klein entry point that lowers to Ray Data in
batch and uses checkpoint-transactional native output in streaming. See
[Filesystem](filesystem.md) before choosing between the APIs.

## Adapt an existing Dataset

Use `from_ray_dataset` when another library has already constructed a Dataset:

```python
dataset = ray.data.from_items([{"id": 1}, {"id": 2}])
stream = ray.klein.from_ray_dataset(dataset)
```

The Dataset remains bounded and batch-only. Klein does not collect it into the
driver or convert it to a native streaming source.

## Configuration and guarantees

There are no Ray Data connector-wide Klein options. Pass connector and resource
arguments to the selected Ray Data method. Klein's job-wide
`execution.runtime.mode`, retry, and Ray initialization options still apply;
see the [configuration reference](../configuration-reference.md).

Data partitioning, retries, commit behavior, and filesystem semantics are those
of the invoked public Ray Data operation. They are not Klein streaming
checkpoints. Use a native connector when the external system must participate
in Klein checkpoint recovery.
