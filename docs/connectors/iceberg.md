---
myst:
  html_meta:
    description: "Write Ray Data batches or checkpoint-transactional streaming appends to Apache Iceberg with Klein for Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-iceberg-connector)=
# Apache Iceberg

Install the Iceberg extra on the driver and every streaming worker:

```bash
python -m pip install "ray-klein[iceberg]"
```

`DataStream.write_iceberg()` has the same arguments as the compatible Ray
Data `Dataset.write_iceberg()` API. The table and namespace must already
exist. Catalog configuration follows PyIceberg:

```python
from ray.data import SaveMode

events.write_iceberg(
    "analytics.events",
    catalog_kwargs={
        "name": "production",
        "type": "rest",
        "uri": "https://catalog.example.com",
    },
    snapshot_properties={"application": "event-normalizer"},
    mode=SaveMode.APPEND,
    concurrency=4,
)
```

## Execution modes

| Mode | Behavior |
|---|---|
| Batch | Delegates to Ray Data and supports its append, overwrite, upsert, schema evolution, and concurrency behavior. |
| Streaming | Supports `SaveMode.APPEND`; non-empty sink subtasks prepare Arrow batches, which the coordinator combines per logical sink and domain epoch before publishing one durable Iceberg snapshot. |

Streaming overwrite and upsert are rejected. An overwrite committed by one
parallel sink subtask would erase output from other subtasks, while a correct
streaming upsert needs a global key/materialization contract. Use a bounded Ray
Data job for those modes.

## Checkpoint and schema behavior

Prepared Arrow batches are compressed into Klein checkpoint metadata. Keep the
checkpoint interval and source rate sized so one interval fits comfortably in
checkpoint storage and coordinator memory. Increasing sink concurrency reduces
the batch held by each subtask; writers participating in the same checkpoint
domain epoch are committed in one Iceberg append. Physically disconnected
domains still checkpoint and commit independently.

The commit adds a reserved `ray-klein.transaction-id` snapshot property. On a
coordinator retry, Klein reloads the table and treats a snapshot with the same
transaction ID as already committed. Do not set that property yourself, and do
not expire a just-committed snapshot while its Klein checkpoint is still being
finalized.

New top-level columns are added before the data snapshot while existing
Iceberg field IDs and required identifier columns are preserved. Rows handled
by one live sink task must keep the same set of columns. Nested schema changes
and incompatible type changes should be applied to the table separately before
the stream emits the new shape.

Catalog credentials are serialized as part of the job and prepared checkpoint
transaction. Prefer workload identity or the catalog/FileIO credential chain
over embedding long-lived secrets in `catalog_kwargs`.
