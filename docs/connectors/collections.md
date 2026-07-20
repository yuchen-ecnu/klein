---
myst:
  html_meta:
    description: "Create Klein for Ray inputs from Python collections, values, and existing Ray Datasets."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Collections and existing Datasets

Collection sources are useful for examples, tests, small control inputs, and
adapting an existing Ray Dataset. They are finite; they are not a distributed
queue or a replacement for a production data connector.

## Choose an API

| API | Input | Automatic mode | Record requirement | Recovery |
|---|---|---|---|---|
| `ray.klein.from_items(items)` | Iterable accepted by Ray Data | Batch | Ray Data-compatible items | Ray Data retry semantics |
| `ray.klein.from_values(*values)` | Positional values | Streaming | Every value must be a mapping | Restores the next unread index |
| `ray.klein.from_ray_dataset(dataset)` | Existing Ray `Dataset` | Batch | Dataset schema | Ray Data retry semantics |

`from_items` has both a native collection source and a Ray Data lowering, so
automatic mode selects batch under the default UDF exception policy. Setting
`udf.ignore-exception=true` or explicitly forcing streaming selects the native
runtime, where values must be mappings because the native
`SourceContext.collect()` contract accepts mapping records.

## Create a bounded collection

```python
import ray
import ray.klein

stream = ray.klein.from_items([
    {"id": 1, "status": "new"},
    {"id": 2, "status": "ready"},
])
stream.show()
ray.klein.execute("collection-example").wait()
```

Use an existing Dataset without materializing it on the driver:

```python
dataset = ray.data.range(1_000)
stream = ray.klein.from_ray_dataset(dataset)
```

Both forms are batch-only in normal use and can be followed by
[Ray Data operations](ray-data.md).

## Create a finite streaming source

```python
stream = ray.klein.from_values(
    {"id": 1, "status": "new"},
    {"id": 2, "status": "ready"},
)
```

`from_values` intentionally has no Ray Data lowering, so it selects native
streaming execution. Its checkpoint state stores the next value index. After
recovery, values before that index are not emitted again by the source, though
downstream external sinks can still have their own replay semantics.

## Configuration and limits

These APIs have no connector-specific configuration. `from_values` accepts
only an optional operator `name`; `from_items` and `from_ray_dataset` follow
their public API signatures. Job-wide runtime and checkpoint settings are in
the [configuration reference](../configuration-reference.md).

The collection is serialized with the submitted job, so large local datasets
increase driver memory and submission cost. Store production-scale data in a
distributed system and use a dedicated connector.
