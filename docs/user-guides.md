---
myst:
  html_meta:
    description: "Klein user guides for pipelines, state, event time, SQL, autoscaling, checkpointing, and operations."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-user-guides)=
# User guides

Start with [installation](installation.md) and [key concepts](key-concepts.md),
then choose a programming interface. The sidebar keeps the DataStream API and
SQL guides in separate sections; both share the connector, deployment, and
operations guides.

## Choose a programming interface

| Interface | Use it when | Start here |
|---|---|---|
| Python DataStream API | You want to compose Python transformations, UDFs, event-time operators, or managed state. | [API quick start](getting-started.md) · [API examples](datastream-examples.md) |
| Klein SQL | You want to express queries, aggregations, and connector tables in SQL. | [SQL quick start](sql/getting-started.md) · [SQL examples](sql/examples.md) |

The interface does not select the execution mode: both support bounded and
streaming workloads within their documented limits. SQL queries return a
`DataStream`, so you can continue processing their results with Python. See
[SQL execution and compatibility](sql/execution.md) before combining operators.

## DataStream API guide

- [DataStream API overview](datastream.md) maps the Python learning path from
  the first pipeline to stateful streaming.
- [Program with DataStream and UDFs](datastream-programming-guide.md) is the
  complete guide to records, batches, callable lifecycles, asynchronous work,
  ordering, errors, resources, and partitioning.
- {doc}`Develop a pipeline <development>` provides a compact first reference
  for transformations, resources, partitioning, and integrations.
- [Build a production streaming pipeline](production-streaming.md) follows a
  Kafka job from event-time assignment through checkpointed file output,
  operations, and restore.
- [Use Ray Data operations](ray-data-interop.md) explains bounded readers and
  the explicit `stream.ray_data` adapter.

## Klein SQL guide

- [SQL overview](sql.md) introduces query entry points and the SQL learning path.
- [SQL quick start](sql/getting-started.md) runs a query and explains table
  bindings and session scope.
- [SQL examples](sql/examples.md) provides a runnable batch query.
- [SQL connectors and Table DDL](sql/connectors.md) covers catalog tables,
  connector options, and inserts.
- [SQL functions](sql/functions.md) covers built-ins, Python scalar functions,
  batched AI functions, and image/PDF processing.
- [Continuous SQL](sql/streaming.md) explains streaming queries and changelog output.
- [SQL execution and compatibility](sql/execution.md) lists supported query
  features and execution boundaries.

## Shared application guides

- [Manage the job lifecycle](job-lifecycle.md) covers contexts, sinks,
  `Pipeline`, `StatementSet`, direct and deferred submission, handles,
  namespaces, cancellation, and cleanup.
- [Check operator compatibility](operator-compatibility.md) lists batch and
  streaming support, partitioning, changelog behavior, and sink boundaries.
- [Choose and configure connectors](connectors/index.md) provides a dedicated
  directory tree for every built-in input, output, lookup, and Serve integration.
- [Write transactional files](connectors/filesystem.md) covers streaming JSON, CSV, Parquet, text, rolling policies, and two-phase commit.

## Work with time and state

- [Understand delivery guarantees](delivery-semantics.md) composes source
  progress, replay, managed state, checkpoints, and external sink effects.
- [Manage keyed state](ray-native-state.md) covers RocksDB, key groups, rescaling, state TTL, and timers.
- [Track event time](event-time.md) covers watermark generation and the idle-input protocol.
- [Store durable checkpoints](checkpoint-storage.md) covers local filesystems, Amazon S3-compatible storage, and Google Cloud Storage.
- [Restore and rescale a job](checkpoint-recovery.md) covers checkpoint
  selection, resubmission, compatibility, validation, and common failures.
- [Survive driver failure](driver-fault-tolerance.md) explains detached control
  actors, reattachment, checkpoint recovery, and the Compiled Graph decision.

## Configure and operate jobs

- [Deploy Klein jobs](deployment.md) covers packaging, Ray Jobs, cluster
  resources, credentials, operations, and upgrades.
- [Observe Klein jobs](observability.md) covers structured logs, metrics,
  checkpoints, CLI attach, and the cluster state API.
- [Autoscaling and live operator rescaling](operator-rescaling.md) separates bounded
  Ray Data worker autoscaling, live streaming rescaling, and Ray cluster
  autoscaling, then covers the runtime safety protocol.
- [Tune performance](performance-tuning.md) maps symptoms and metrics to
  concurrency, batching, state, checkpoint, placement, and replay controls.
- [Troubleshoot jobs](troubleshooting.md) provides failure-oriented checks for
  installation, planning, connectors, state, event time, and recovery.
- [Configure Klein](configuration.md) explains configuration sources and precedence.
- [Configuration reference](configuration-reference.md) lists every supported key, type, default, constraint, and environment variable.
- {doc}`Debug a local pipeline <local_debug>` describes embedded Ray, in-process debug mode, and logging.
- [Check compatibility](compatibility.md) defines supported Python and Ray versions.
- [Check production readiness](production-readiness.md) provides an executable
  launch checklist; [Security](security.md), [Known limitations](limitations.md),
  and [Upgrading](upgrading.md) make the operational boundaries explicit.
- [Use the CLI](cli-reference.md) lists every command, option, exit behavior,
  JSON contract, and automation rule.

Contributor and internal-development documentation has its own top-level
navigation section so application users do not need to filter it out of the
runtime guides.
