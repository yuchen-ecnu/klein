---
myst:
  html_meta:
    description: "Known limitations, unsupported combinations, and non-goals for the current Klein for Ray alpha release."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-limitations)=
# Known limitations and non-goals

Klein for Ray is alpha software. This page collects the boundaries that are
otherwise easy to miss in feature-specific guides. Treat an item as unsupported
unless the documentation for the installed release explicitly says otherwise.
An unsupported combination can fail during planning, fail when workers start,
or run with weaker recovery semantics than the application expects.

The [operator compatibility matrix](operator-compatibility.md) is the concise
batch/streaming reference. This page explains the broader product and
operational boundaries.

## Platform and compatibility

| Area | Current boundary | Consequence |
| --- | --- | --- |
| Project status | Independent alpha project, not maintained or endorsed by Ray | Public APIs, checkpoint formats, and behavior can change before 1.0. Pin an exact Klein release. |
| Python | CPython 3.10 through 3.12 | Other Python implementations and versions are not in the tested matrix. |
| Ray | `ray[data]>=2.56.1,<2.57` | Klein deliberately pins one Ray minor because some Ray Data extension points can change between minors. |
| Operating systems | Linux and macOS are declared by the package | Native Windows is not part of the release contract. |
| Language API | Python only | There is no Java, Scala, SQL Gateway, or REST job-submission API. |
| Distribution | Source checkout and Python wheel | Klein does not provide a standalone cluster distribution, container image, Helm chart, or Kubernetes operator. It runs in an existing Ray environment. |

Review [Compatibility](compatibility.md) before changing Python or Ray, and
follow [Upgrading](upgrading.md) before changing any component that must read an
existing checkpoint.

## Execution model

One submitted graph uses one execution mode. Klein does not split a graph into
independent batch and streaming regions.

- `auto` selects native streaming when any source is unbounded, any graph
  vertex has no batch lowering, or `udf.ignore-exception=true`; only a fully
  batch-lowerable, bounded graph with that policy disabled is lowered to Ray
  Data.
- A bounded custom `SourceFunction` has no public batch lowering by default, so
  `auto` selects streaming. Use a Ray Data source when batch execution is
  required.
- Most arbitrary `stream.data` Dataset operations are batch-only. Streaming
  currently supports the explicitly documented Ray expression forms and native
  `DataStream` operators.
- Native streaming is record-oriented with ordered micro-batches. It is not a
  replacement for Ray Data's batch optimizer, block formats, or autoscaling
  actor-pool policy.
- A graph must have at least one sink before `execute()`. The deprecated
  interactive mode is retained only for compatibility and is not a production
  materialization protocol.

See [Key concepts](key-concepts.md), [Ray Data interoperability](ray-data-interop.md),
and [Operator compatibility](operator-compatibility.md) for the selection and
lowering rules.

## SQL and dynamic tables

Klein SQL is deliberately smaller than Flink SQL or a general-purpose database.
The batch and streaming planners also support different subsets.

- Streaming supports one `SELECT` query block; common table expressions and
  `UNION ALL` are batch-only.
- Streaming joins are regular inner equality joins. Outer, cross, temporal,
  interval-SQL, and non-equality joins are not implemented.
- `HAVING`, `SELECT DISTINCT`, recursive common table expressions, SQL window
  syntax, computed columns, watermarks in DDL, partitions, and catalog-qualified
  names are not implemented.
- Streaming `ORDER BY` requires `LIMIT` and becomes a continuously maintained
  Top-N. A global Top-N is a single keyed partition.
- Regular joins, non-windowed aggregations, and Top-N can grow without bound
  unless state TTL is configured. TTL can make later results incomplete.
- Updating queries emit changelog rows. Ordinary append sinks do not
  automatically materialize update-before, update-after, or delete records.
- SQLGlot parses the query, but Klein owns validation and execution. Installing
  another SQLGlot version or a database engine does not add SQL features.

The authoritative query matrix and expression rules are in
[SQL and Table APIs](sql.md#how-does-sql-execute).

## State, checkpoints, and recovery

Klein checkpoints provide a consistent recovery boundary for supported native
streaming sources, managed state, timers, and prepared transactional sink
output. They do not make every external effect exactly once.

| Boundary | Current behavior |
| --- | --- |
| General delivery | At least once. Non-transactional sinks can observe duplicates after replay. |
| State backends | In-memory and optional local RocksDB working state. Durable recovery still requires checkpoint storage. |
| Durable checkpoint stores | Local/file, Amazon S3-compatible, and Google Cloud Storage URIs through the documented filesystem adapter. |
| Savepoints | `execution.savepoint.path` can restore a recorded completed checkpoint, but there is no command that creates a separately managed Flink-style savepoint. |
| JobManager loss | Losing the JobManager currently requires resubmitting the same job definition from a durable completed checkpoint. |
| Whole-cluster loss | Recovery is a new submission; Klein does not recreate a Ray cluster or resubmit the application. |
| State schema migration | No automatic migration framework is promised for alpha checkpoints. |
| Serialization | Python objects, functions, keys, and state must be serializable and should be treated as trusted data. |
| Maximum parallelism | `state.keyed.max-parallelism` fixes the key-group space and cannot change when an existing checkpoint must remain restorable. |

Checkpoint metadata only covers components in its checkpoint domain. Broker
acknowledgements, database commits, remote API calls, and arbitrary UDF side
effects remain external consistency boundaries. Read
[Delivery semantics](delivery-semantics.md), [Checkpoint storage](checkpoint-storage.md),
and [Restore and rescale](checkpoint-recovery.md) together before defining an
RPO or RTO.

## Scaling and scheduling

Klein separates Ray Data worker autoscaling, native streaming operator
parallelism, and Ray cluster node autoscaling.

- There is no built-in metric-driven controller for streaming operator
  parallelism. The Dashboard and state API perform explicit changes.
- Live local rescaling does not support source operators, collecting sinks, or
  transactional sinks.
- Only one live operator rescale can run at a time, and another change must wait
  for the stabilization checkpoint.
- Scale-out actors added after initial placement use native Ray placement.
  Existing placement-group bundles are not resized, and scale-in does not
  guarantee that Ray can release a node.
- Operator chaining changes the physical scaling unit. A transform chained to
  an unsupported source cannot be resized independently.
- Hot keys, single global aggregations, external rate limits, and serial sinks
  do not become parallel merely because another operator is enlarged.
- Ray's cluster autoscaler can satisfy resource demand but does not decide
  Klein operator parallelism.

See [Autoscaling and live operator rescaling](operator-rescaling.md) for the
admission rules, rollback boundary, and stabilization fence.

## Connector guarantees

Connector behavior is not uniform:

- Streaming Kafka output is at least once and does not use Kafka transactions.
- RocketMQ progress is owned by its consumer group and can advance ahead of a
  durable Klein checkpoint. A full rollback can therefore lose acknowledged
  messages relative to that checkpoint.
- Streaming Iceberg supports append only. Overwrite and upsert remain
  batch-only.
- Filesystem and Iceberg append output is checkpoint-transactional, but readers
  and catalog retention still need to follow their documented commit protocol.
- SQL, Redis, console, and ordinary custom sinks are not globally transactional.
- A custom `SourceFunction` or `SinkFunction` defines part of the recovery
  contract. Klein cannot infer whether its external side effects are idempotent.
- Optional connector libraries and native dependencies must be present on every
  worker that may execute the connector.

Use the [connector capability matrix](connectors/index.md#capability-matrix)
and the connector's own delivery section before combining a source and sink.

## Operations and security

- The bundled Dashboard has no authentication or TLS. It binds to loopback by
  default and refuses a non-loopback listener unless the operator explicitly
  accepts the risk.
- The Dashboard control endpoint can rescale supported operators. The CLI and
  Python state API can also cancel jobs. Protect every control path with the
  Ray and network boundary documented in the security guide.
- The detached state actor keeps a bounded in-memory job history. It is not an
  audit log, durable metric store, or source of checkpoint truth.
- Klein relies on Ray and the deployment platform for cluster authentication,
  network isolation, process isolation, secret delivery, and multi-tenant
  policy.
- User-defined functions execute arbitrary Python with the worker process's
  identity and credentials. Klein does not sandbox them.
- Checkpoint and application artifacts must come from a trusted source. Loading
  Python-serialized state from an untrusted location is unsafe.
- Configuration redaction is defense in depth, not a secret store. Do not put
  credentials in logs, record values, metric labels, job names, or operator
  names.

Read [Security and trust boundaries](security.md) before exposing any Ray or
Klein endpoint.

## Explicit non-goals

The current standalone project does not aim to:

- replace Ray Core, Ray Data, Ray Jobs, Ray Serve, KubeRay, or the Ray
  autoscaler;
- reproduce every Flink API, connector, SQL feature, deployment target, or
  exactly-once protocol;
- provide a public multi-tenant control plane or hosted service;
- infer external-system correctness from a Python connector implementation;
- guarantee checkpoint compatibility across arbitrary alpha revisions; or
- make internal modules under `ray.klein._internal` or `ray.klein.runtime`
  stable application APIs.

If an application depends on a missing capability, open an issue before
building around an internal module. Include the execution mode, recovery
requirement, connector pair, state size, and expected compatibility window.
