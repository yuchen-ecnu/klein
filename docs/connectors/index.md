---
myst:
  html_meta:
    description: "Klein connector catalog for Ray Data, collections, Kafka, RocketMQ, filesystems, Redis, console, custom connectors, and Ray Serve."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-connectors)=
# Overview

This section catalogs the inputs, outputs, formats, and integrations shipped
with Klein. Connector names stay short in the navigation; use the groups below
to choose the right family before comparing detailed capabilities.

## Catalog

| Group | Entries | Purpose |
|---|---|---|
| Native | [Ray Data](ray-data.md), [Collections](collections.md) | Ray-native and in-memory data |
| Messaging | [Kafka](kafka.md), [RocketMQ](rocketmq.md), [Canal](canal.md) | Event streams and CDC records |
| Storage | [Filesystem](filesystem.md), [Iceberg](iceberg.md) | Files and lakehouse tables |
| Services | [Redis](redis.md), [Ray Serve](ray-serve.md) | External state and serving regions |
| Development | [Console](console.md), [Custom](custom.md) | Debugging and extension points |

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
| [Ray Serve](ray-serve.md) | Execution region | Execution region | Yes | Yes | No | `serve` |
| [Console](console.md) | No | Yes | Yes | Yes | Sink only | None |
| [Custom](custom.md) | Yes | Yes | Depends on implementation | Yes | Optional | Connector-defined |

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
