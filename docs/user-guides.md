---
myst:
  html_meta:
    description: "Klein for Ray user guides for pipelines, state, event time, SQL, configuration, checkpointing, and operations."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-user-guides)=
# User guides

If you're new to Klein, complete [Getting started](getting-started.md) and read [Key concepts](key-concepts.md) first. These guides cover pipeline development and production behavior.

```{toctree}
:hidden:
:maxdepth: 2

development
ray-data-interop
sql
connectors/index
ray-native-state
event-time
checkpoint-storage
driver-fault-tolerance
observability
configuration
configuration-reference
local_debug
compatibility
package-structure
testing
private-api-inventory
releasing
```

## Build dataflows

- {doc}`Develop a pipeline <development>` describes transformations, resources, partitioning, and integrations.
- [Choose and configure connectors](connectors/index.md) provides a dedicated
  directory tree for every built-in input, output, lookup, and Serve integration.
- [Use Ray Data operations](ray-data-interop.md) explains bounded readers and the `stream.data` adapter.
- [Query streams with SQL](sql.md) covers SQLGlot planning, temporary views, and Flink-style Table data definition language (DDL).
- [Write transactional files](connectors/filesystem.md) covers streaming JSON, CSV, Parquet, text, rolling policies, and two-phase commit.

## Work with time and state

- [Manage keyed state](ray-native-state.md) covers RocksDB, key groups, rescaling, state TTL, and timers.
- [Track event time](event-time.md) covers watermark generation and the idle-input protocol.
- [Store durable checkpoints](checkpoint-storage.md) covers local filesystems, Amazon S3-compatible storage, and Google Cloud Storage.
- [Survive driver failure](driver-fault-tolerance.md) explains detached control
  actors, reattachment, checkpoint recovery, and the Compiled Graph decision.

## Configure and operate jobs

- [Observe Klein jobs](observability.md) covers structured logs, metrics,
  checkpoints, CLI attach, and the cluster state API.
- [Configure Klein](configuration.md) explains configuration sources and precedence.
- [Configuration reference](configuration-reference.md) lists every supported key, type, default, constraint, and environment variable.
- {doc}`Debug a local pipeline <local_debug>` describes embedded Ray, in-process debug mode, and logging.
- [Check compatibility](compatibility.md) defines supported Python and Ray versions.

## Contribute to Klein

- [Understand the package structure](package-structure.md) before moving or adding modules.
- [Write and run tests](testing.md) explains the test tiers and fixture rules.
- [Review the Ray private-API inventory](private-api-inventory.md) before adding a Ray dependency.
- [Release Klein](releasing.md) describes package verification and publication.
