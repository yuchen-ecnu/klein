---
myst:
  html_meta:
    description: "Klein for Ray connector catalog for Ray Data, collections, Kafka, RocketMQ, filesystems, Redis, console, custom connectors, and Ray Serve."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-connectors)=
# Connectors

This section is the complete catalog of inputs, outputs, lookup integrations,
and execution integrations shipped with Klein. Start here when choosing how a
dataflow enters or leaves Klein; each child page documents installation,
supported execution modes, configuration, data shape, delivery guarantees, and
operational constraints.

```text
connectors/
├── index.md          # Catalog and capability matrix
├── ray-data.md       # Dynamic Ray Data readers, transforms, and writers
├── collections.md    # In-memory values and existing Ray Datasets
├── kafka.md          # Bounded/continuous input and Kafka output
├── rocketmq.md       # Continuous Apache RocketMQ input
├── canal.md          # Canal JSON value format for Kafka input
├── filesystem.md     # JSON, CSV, Parquet, and text files
├── iceberg.md        # Apache Iceberg batch and checkpointed append output
├── redis.md          # Lookups, missing-key filters, and output
├── console.md        # Diagnostic stdout output
├── custom.md         # SourceFunction, SinkFunction, and TableFactory
└── ray-serve.md      # Optional Ray Serve execution integration
```

```{toctree}
:maxdepth: 1

ray-data
collections
kafka
rocketmq
canal
filesystem
iceberg
redis
console
custom
ray-serve
```

## Capability matrix

| Connector | Input | Output | Batch | Streaming | Table DDL | Extra |
|---|---:|---:|---:|---:|---:|---|
| [Ray Data](ray-data.md) | Yes | Yes | Yes | Expressions only | No | None beyond the selected Ray Data connector |
| [Collections](collections.md) | Yes | No | Yes | Yes | No | None |
| [Kafka](kafka.md) | Yes | Yes | Yes | Yes | Yes | `kafka` |
| [RocketMQ](rocketmq.md) | Yes | No | No | Yes | No | `rocketmq` plus native `librocketmq` |
| [Filesystem](filesystem.md) | Yes | Yes | Yes | Output only | Yes | Filesystem-specific dependencies |
| [Iceberg](iceberg.md) | Via Ray Data | Yes | Yes | Append output | No | `iceberg` plus catalog-specific dependencies |
| [Redis](redis.md) | Lookup/filter | Yes | Yes | Yes | No | `redis` |
| [Console](console.md) | No | Yes | Yes | Yes | Sink only | None |
| [Custom](custom.md) | Yes | Yes | Depends on implementation | Yes | Optional | Connector-defined |
| [Ray Serve](ray-serve.md) | Execution region | Execution region | Yes | Yes | No | `serve` |

### Matrix notes

**Ray Data streaming**

`stream.data.with_column(name, expr)` and `stream.data.filter(expr=expr)`
support streaming. Other Ray Data operations are batch-only; Klein's native
filesystem writers and `stream.write_sql` support streaming separately.

**Redis transformations**

Redis lookup and missing-key transforms work in both modes; Redis output is a
native streaming sink.

## Choose a connector

- For bounded data already supported by Ray Data, use the dynamic
  [Ray Data adapter](ray-data.md). Klein preserves Ray Data's public arguments
  instead of duplicating them.
- For a long-running event log, use [Kafka](kafka.md). Continuous input is
  checkpoint-aware; output is at-least-once.
- For an existing remoting-protocol Apache RocketMQ deployment, use
  [RocketMQ](rocketmq.md) and review its broker-managed recovery boundary.
- For MySQL CDC already published by Canal, use Kafka with the
  [Canal JSON format](canal.md). FlatMessage JSON is decoded into native Klein
  changelog rows without introducing another connector.
- For checkpoint-transactional output, use [filesystem](filesystem.md). Final
  part files become visible only after their Klein checkpoint is durable.
- For an existing lakehouse table, use [Iceberg](iceberg.md). Batch mode keeps
  Ray Data's append/overwrite/upsert behavior; streaming mode appends snapshots
  only after a Klein checkpoint becomes durable.
- For enrichment or a materialized key/value output, use [Redis](redis.md), and
  account for its external at-least-once semantics.
- For development-only inspection, use [console](console.md).
- To integrate another system, implement a [custom connector](custom.md).

## Execution-mode rule

In `auto` mode, an unbounded source, any graph vertex without a batch lowering,
or `udf.ignore-exception=true` selects streaming; only a fully batch-lowerable,
bounded graph with that policy disabled selects batch. A native
`SourceFunction` or `SinkFunction` without a lowering therefore selects
streaming automatically, even when the source is bounded. See
[Configuration](../configuration.md) for explicit mode selection and
[Ray Data interoperation](../ray-data-interop.md) for the lowering model.
