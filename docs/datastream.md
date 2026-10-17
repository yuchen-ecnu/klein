---
myst:
  html_meta:
    description: "Learn Klein's Python DataStream API through a dedicated path for pipelines, transformations, state, and event time."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-datastream)=
# DataStream API overview

Use the Python DataStream API when you want to build a pipeline in code, apply
Python functions, or work directly with keyed state and event-time operations.
A `Pipeline` connects sources, transformations, and terminal sinks into a lazy
graph. The supported operations determine whether it can run on Ray Data batch
execution or Klein's native streaming backend.

For queries, table definitions, and SQL functions, follow the separate
[SQL guide](sql.md). Both interfaces use Klein's execution and connector
infrastructure; choose the guide that matches how you author your job.

## Learning path

| Goal | Guide |
|---|---|
| Install Klein and run your first Python pipeline | [DataStream API quick start](getting-started.md) |
| Understand records, transformations, batching, and partitioning | [DataStream programming guide](datastream-programming-guide.md) |
| Develop Python functions and submit pipelines | [Pipeline development](development.rst) |
| Run complete local programs | [DataStream API examples](datastream-examples.md) |
| Use Ray Data readers and Dataset operations | [Ray Data interoperability](ray-data-interop.md) |
| Manage keyed state and timers | [Managed state](ray-native-state.md) |
| Work with timestamps, watermarks, and windows | [Event time](event-time.md) |
| Connect Kafka, checkpoints, and file output | [Production streaming walkthrough](production-streaming.md) |

## References and shared guides

Use the [operator compatibility matrix](operator-compatibility.md) to check
backend support and the [connector catalog](connectors/index.md) to select
sources and sinks. Configuration, deployment, and operations apply to both
DataStream API and SQL jobs; find these in the [user guides](user-guides.md).

The {doc}`API reference <api/api>` documents public classes, method signatures,
and parameters. Use it to look up individual calls while following the guides
above.
