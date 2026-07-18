---
myst:
  html_meta:
    description: "Inspect Klein for Ray batch and streaming records with the console and print connector."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Console and print

The console sink is a diagnostic connector for examples and local debugging.
It prints records to standard output and provides no external durability or
deduplication.

## Use the DataStream API

```python
stream.show(limit=20, concurrency=1)
```

| Argument | Default | Meaning |
|---|---:|---|
| `limit` | `20` | Maximum rows shown by Ray Data in batch; maximum per sink subtask in streaming. Use `-1` for no streaming limit. |
| `num_cpus` | `None` | Sink task CPU requirement. |
| `concurrency` | `None` | Number of streaming sink subtasks or Ray Data operation concurrency where supported. |
| `batch_size` | `None` | Maximum records delivered in one native operator batch. |
| `batch_timeout` | 3 seconds | Maximum wait for a native operator batch. |
| `name` | `"Show"` | Operator name. |

In batch, `show` delegates to `Dataset.show`. In streaming, each output line is
compact JSON Lines and has this shape:

```json
{"sink":"console","subtask_index":0,"sequence":1,"value":{"id":42}}
```

For a `ChangelogRow`, the line also includes `row_kind`. Operational logs use
standard error so tools can safely parse or pipe standard output. Sequence and
limit counters are local to each streaming subtask.

Checkpoint recovery can print a record again. Do not treat console output as an
exactly-once audit log, and avoid it for high-volume production data.

## Use Table DDL

The sink-only Table connector identifier is `print`:

```sql
CREATE TABLE debug_output (
    id BIGINT,
    payload STRING
) WITH (
    'connector' = 'print',
    'limit' = '20'
);
```

`limit` defaults to `20` and is the only connector option. The print sink
accepts all Klein row kinds and includes `row_kind` in native streaming output.
See [SQL and Table DDL](../sql.md) for inserting a query into the table.
