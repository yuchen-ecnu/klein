---
myst:
  html_meta:
    description: "Frequently asked questions about choosing, installing, developing, deploying, and operating Klein for Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-faq)=
# Frequently asked questions

This page gives short answers and sends you to the authoritative guide for
details.

## Choosing Klein

### When should I use Klein?

Use Klein when a Ray application needs long-running record processing, event
time, managed keyed state, or checkpoint recovery. For bounded preparation,
inference, or training ingest without those semantics, use Ray Data directly.
See [Key concepts](key-concepts.md) and [Ray Data interoperability](ray-data-interop.md).

### Is Klein part of the Ray project?

No. Klein is independent alpha software. The `ray.klein` namespace is a
technical integration point; Klein does not replace Ray's package or claim Ray
project endorsement. See [Architecture](architecture.md).

### Is Klein a drop-in replacement for Apache Flink?

No. It offers familiar DataStream, event-time, checkpoint, keyed-state, and
dynamic-table concepts on Ray, but its API and supported operators are not a
complete Flink compatibility layer. Check [Known limitations](limitations.md),
[operator compatibility](operator-compatibility.md), and [SQL](sql.md) before
porting a job.

### Is the API stable enough for production?

Klein is at `0.1.0.dev0` and classified as alpha. Production evaluation should
pin Klein and Ray, rehearse recovery, and accept that APIs and checkpoint
formats can change before 1.0. See [API stability](api-stability.md) and the
[production-readiness checklist](production-readiness.md).

### Can one application mix Klein and native Ray libraries?

Yes. Bounded Klein graphs lower to Ray Data, while native streaming operators
run on Ray Core. Use the explicit `.data` adapter for Ray Data operations and
review where execution semantics change in [Ray Data interoperability](ray-data-interop.md).

## Installation

### Which Python and Ray versions are supported?

Python 3.10–3.12 and `ray[data]>=2.56.1,<2.57`. The Ray upper bound is
intentional. See [Installation](installation.md#supported-environment) and
[Compatibility](compatibility.md).

### Can I install `ray-klein` from PyPI?

The current documentation does not assume a published PyPI distribution.
Install a source checkout non-editably, use an editable checkout for
development, or install a wheel built from a reviewed checkout. See
[Installation](installation.md#regular-installation-from-a-checkout).

### Which optional extra do I need?

Use `kafka`, `iceberg`, `rocketmq`, `redis`, `rocksdb`, or `serve` for the
matching integration. `all` installs all runtime integrations; `dev` also adds
tests, docs, and contributor tools. The exact dependencies are listed under
[Optional extras](installation.md#optional-extras).

### Must Klein be installed on every Ray worker?

Yes. Every eligible worker needs the same Klein/Ray versions, graph extras,
application modules, and native dependencies. A driver's virtual environment
is not propagated automatically. See [cluster environment consistency](installation.md#keep-the-cluster-environment-consistent).

### Why does the driver import a connector that a worker cannot import?

The environments differ, or the optional extra/native library exists only on
the driver. Deploy one immutable image or artifact set to every node, then
restart the affected processes. See [installation troubleshooting](installation.md#troubleshoot-installation).

### Why does `import ray` work but `import ray.klein` fail?

Ray and Klein are separate distributions. Install Klein into the same Python
environment and verify it with `python -m pip show ray-klein`; merely keeping
the source checkout nearby does not extend the installed `ray` package.

## Batch and streaming execution

### How does `auto` choose an execution mode?

The whole job selects streaming when a source is unbounded, any graph vertex
lacks a batch lowering, or `udf.ignore-exception=true`. Only a fully
batch-lowerable, bounded graph with that policy disabled selects batch.
Selection is job-wide, not branch-by-branch. See
[operator compatibility](operator-compatibility.md).

### Can I force batch or streaming mode?

Yes, set `execution.runtime.mode` to `batch` or `streaming` before execution.
An incompatible graph fails instead of silently changing an operator's
semantics. See [Configuration](configuration.md) and its
[reference](configuration-reference.md#execution-recovery-and-checkpointing).

### How does `auto` handle a bounded custom source?

`SourceFunction` is a native streaming contract unless its integration
provides a batch lowering. A bounded custom source without that lowering
selects native streaming automatically; forcing batch fails. Use a Ray Data
source or provide a batch lowering when batch execution is required. See
[Custom connectors](connectors/custom.md).

### Do all `DataStream` methods work in both modes?

No. Basic row transforms work in both, but keyed state, watermarks, windows,
and most custom source/sink contracts are streaming features; many `.data`
operations are batch-only. Use the [operator compatibility matrix](operator-compatibility.md).

### What is interactive mode?

It is a deprecated, opt-in compatibility mode in which terminal operations
such as `take_all()` execute a bounded graph immediately. New code should keep
terminal operations lazy, register every intended sink, call
`execute("job-name")`, and retrieve results from the returned job handle. See
[Getting started](getting-started.md).

### Does local debug mode validate distributed behavior?

No. It is useful for deterministic development but does not validate Ray
serialization, scheduling, isolation, actor failure, or recovery. Run the
integration suite or a real cluster before deployment; see [Local debug](local_debug.rst).

### What is Klein's general delivery guarantee?

At least once. End-to-end behavior is the weakest boundary among the source,
state/checkpoint path, replay, and sink; selected sinks offer
checkpoint-transactional visibility. See [Delivery and consistency](delivery-semantics.md).

## State and event time

### Is managed state durable by itself?

No. Memory and RocksDB are working-state backends. Recovery across node or
cluster loss depends on completed checkpoints in shared durable storage. Ray
Object Store caching accelerates recovery but is not the durability boundary.
See [Ray-native state](ray-native-state.md) and [checkpoint storage](checkpoint-storage.md).

### Do checkpoints make every sink exactly once?

No. An external sink may accept an effect that is replayed after failure.
Filesystem, Iceberg append, and a correctly implemented two-phase-commit sink
have stronger checkpoint-transactional visibility; other sinks can remain at
least once. See the [sink guarantee table](delivery-semantics.md#sink-behavior).

### Why does an event-time window never emit?

Usually one active physical input has not advanced its watermark. Assign
timestamps and watermarks, configure idleness for quiet inputs, and inspect
watermark metrics. See [Event time](event-time.md) and
[Troubleshooting](troubleshooting.md#windows-never-emit).

### Are event timestamps seconds or milliseconds?

Non-negative integer milliseconds. Floating-point, negative, or second-based
values are invalid or produce the wrong window scale. Watermarks must advance
separately from timestamp assignment.

### How do I prevent state from growing forever?

Choose finite windows where appropriate, configure state TTL for long-lived
keyed or SQL state, and monitor managed-state size. TTL is a correctness choice:
expired state can make later results incomplete. See [Ray-native state](ray-native-state.md)
and [Performance tuning](performance-tuning.md).

### Can I change operator parallelism without losing keyed state?

Supported running operators can move managed state by key group through the
barrier-aligned rescale procedure. Do not change
`state.keyed.max-parallelism` for checkpoints that must remain restorable.
Sources and some sink types have additional restrictions. See
[operator rescaling](operator-rescaling.md).

### What survives total Ray cluster loss?

Detached actors do not. Resubmit the graph and restore from shared durable
checkpoint storage. Record the code version, namespace, source identity,
checkpoint path, and max parallelism needed for recovery; see
[checkpoint recovery](checkpoint-recovery.md).

## SQL and connectors

### Which engine executes Klein SQL?

SQLGlot parses one AST. Bounded queries lower to Ray Dataset operations;
continuous queries lower to native Klein operators with managed state and
changelog rows. Klein does not embed DuckDB. See [SQL and Table connectors](sql.md).

### Is every SQL query supported in streaming mode?

No. Streaming SQL implements a documented subset and rejects unsupported forms
with `SQLQueryError` rather than silently changing semantics. Stateful joins,
aggregates, and Top-N also require bounded state planning. Review the SQL guide
and [known limitations](limitations.md#sql-and-dynamic-tables).

### Why does a top-level SQL query not see a table I created earlier?

`ray.klein.sql()` creates a fresh one-query session. Use
`ray.klein.execute_sql()` for a persistent module-level catalog, or pass
explicit `tables={...}` bindings. See
[Choose a SQL entry point](sql.md#choose-a-sql-entry-point).

### Which connectors are built in?

Ray Data, collections, Kafka, RocketMQ, Canal JSON, filesystems, Iceberg,
Redis, console, custom connectors, and Ray Serve integration are documented in
the [connector catalog](connectors/index.md). Their batch/streaming modes and
extras differ.

### Does a Table DDL connector create external infrastructure?

No. `CREATE TABLE` validates logical schema and connector options; it does not
create topics, brokers, buckets, or databases. External resources and
credentials must already exist.

### Can I add a connector outside the core package?

Yes. Implement the source/sink contracts or a `TableFactory`. Third-party
packages can publish table factories through the `ray.klein.table_factories`
entry-point group. See [Custom connectors](connectors/custom.md).

### Where do I find connector delivery guarantees and schemas?

Each connector page documents its input/output shape and recovery boundary;
the cross-connector summary is [Delivery and consistency guarantees](delivery-semantics.md).
Do not infer exactly-once behavior from checkpointing alone.

## Deployment and operations

### How should I submit a production job?

Build one immutable driver/worker environment, initialize the intended Ray
cluster explicitly, and submit a normal Python entry point through the
cluster's Ray Jobs or platform workflow. Use shared checkpoint storage and
record the returned Klein namespace. See [Deployment](deployment.md) and the
[production streaming walkthrough](production-streaming.md).

### Does the job stop when the submitting process exits?

Not for native streaming execution: its `JobManager` and workers are detached
actors. A new client can discover and manage them. Loss of the whole Ray
cluster still requires resubmission from durable state; see
[driver fault tolerance](driver-fault-tolerance.md).

### Why can the CLI not find my job?

Confirm `RAY_ADDRESS` selects the correct cluster and copy the exact namespace
from `ray-klein list --all --json`. A Ray Jobs submission ID, a job display
name, a state API job ID, and the Klein Ray namespace are not universally
interchangeable. See [CLI identifiers](cli-reference.md#job-names-namespaces-and-job-ids).

### Does Ctrl+C in `attach` stop the job?

No. It detaches the terminal view and leaves the job running. Use `ray-klein
cancel NAMESPACE --force` or the equivalent `stop` alias for deliberate
cancellation. See [CLI reference](cli-reference.md#attach).

### Is the Klein Dashboard safe to expose publicly?

No. Its control endpoint is unauthenticated. It binds to loopback by default
and refuses a non-loopback listener unless `--allow-unauthenticated` is
explicit; that flag is not a security mechanism. Use a tunnel or a protected
proxy and follow [Security](security.md).

### How do I monitor a running job?

Use `ray-klein status`, the bundled Dashboard, structured logs, Ray metrics,
and durable checkpoint history. The full signals and Prometheus examples are
in [Observability](observability.md); use `status --json` or the Python state API
for automation.

### How do I diagnose low throughput or backpressure?

Start with per-operator input/output, processing latency, buffer utilization,
backpressure duration, source lag, checkpoint time, and Ray node resources.
Tune one boundary at a time using [Performance tuning](performance-tuning.md)
and the symptom guide in [Troubleshooting](troubleshooting.md).

### How do I upgrade a stateful job?

Create and record a completed durable checkpoint, stop the old job, deploy one
reviewed environment, restore explicitly, and validate source position, state,
sink behavior, and a new checkpoint. Do not assume automatic checkpoint-schema
migration; follow [Upgrade Klein jobs](upgrading.md).

### What should I include in a support report?

Include the Klein, Ray, and Python versions; plan; namespace; explicit
configuration with secrets removed; first traceback; relevant component logs;
checkpoint history; and a small metric window around the failure. See
[Collect a useful report](troubleshooting.md#collect-a-useful-report) and use
the private process in `SECURITY.md` for suspected vulnerabilities.
