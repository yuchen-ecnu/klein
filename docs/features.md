---
myst:
  html_meta:
    description: "Explore Klein for Ray feature guides for hybrid execution, state, event time, SQL, recovery, live rescaling, detached jobs, and Ray Serve."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-features)=
# Feature highlights

This page is the product-oriented map of Klein's distinctive capabilities.
Each feature links to its programming guide, operational contract, and known
boundary. Read [Key concepts](key-concepts.md) first for the common execution
model, or choose a feature directly below.

## Feature guide map

| Feature | What it adds to a Ray application | Start here | Check before production |
| --- | --- | --- | --- |
| One graph for bounded and continuous data | A lazy `DataStream` graph that lowers to Ray Data for compatible bounded work or runs as long-lived Ray actors for streaming work. | [Ray Data interoperation](ray-data-interop.md) | [Operator compatibility](operator-compatibility.md) |
| Dynamic Ray Data access | Module-level `read_*` factories and `stream.data` adapters derived from the installed compatible Ray version instead of a duplicated wrapper API. | [Ray Data interoperation](ray-data-interop.md) | [Compatibility](compatibility.md) |
| Ray-native managed state | Keyed value, list, and map state; TTL; timers; key groups; RocksDB; checkpoint restore; and rescaling. | [Managed state](ray-native-state.md) | [Checkpoint recovery](checkpoint-recovery.md) |
| Event time and idle inputs | Watermarks, idleness, event-time timers, windows, interval joins, and late-record handling. | [Event time](event-time.md) | [Delivery semantics](delivery-semantics.md) |
| Bounded and continuous SQL | SQL and Table APIs backed by SQLGlot planning, with explicit changelog rows for dynamic tables. | [SQL and Table APIs](sql.md) | [SQL execution modes](sql.md#how-does-sql-execute) |
| Checkpoint-aware recovery and output | Coordinated source progress, managed state, replay, durable checkpoint storage, and transactional publication where a sink supports it. | [Delivery semantics](delivery-semantics.md) | [Production readiness](production-readiness.md) |
| Live operator rescaling | A checkpoint-coordinated topology change through the Dashboard or Python state API without restarting the whole job. | [Live operator rescaling](operator-rescaling.md) | [Rescaling safety conditions](operator-rescaling.md#prepare-a-streaming-job-for-live-rescaling) |
| Driver-independent streaming jobs | Detached control actors, CLI reattachment, snapshots, cancellation, and explicit recovery after larger failures. | [Driver fault tolerance](driver-fault-tolerance.md) | [Failure boundaries](driver-fault-tolerance.md#failure-boundaries) |
| Ray Serve execution regions | An eligible `map_batches` chain can run behind an independently deployed Ray Serve endpoint. | [Ray Serve integration](connectors/ray-serve.md) | [Requests, retries, and operations](connectors/ray-serve.md#requests-retries-and-operations) |

## One lazy graph, two Ray-native runtimes

Klein keeps graph construction independent from execution. In `auto` mode, an
unbounded source, any graph vertex without a batch lowering, or
`udf.ignore-exception=true` selects the native streaming runtime for the whole
job. Otherwise a fully batch-lowerable bounded graph becomes lazy Ray Data
operations.

This gives applications one `DataStream` shape without hiding the runtime
boundary. Batch and streaming regions are not mixed within one submitted job,
and forcing a mode cannot add a missing implementation. Use the
[compatibility matrix](operator-compatibility.md) when combining execution
families.

The dynamic adapter exposes public readers and Dataset methods from the
installed supported Ray release through `ray.klein.read_*`, `ctx.data`, and
`stream.data`. Ray Data expressions used by `with_column` and `filter` also
have documented streaming forms; most other arbitrary Dataset operations
remain batch-only.

## Stateful, event-time, and relational streaming

The native streaming runtime combines three related programming models:

- Managed keyed state provides value, list, and map state, TTL cleanup,
  processing-time and event-time timers, key groups, and restore across a
  supported parallelism change.
- Event-time processing carries ordered watermarks over the same channels as
  data and checkpoint barriers. Idle-input detection prevents a quiet physical
  input from holding back the whole graph indefinitely.
- Continuous SQL represents changing results as insert, update-before,
  update-after, and delete rows. The Table API and connector DDL make those
  changelog contracts explicit instead of treating an unbounded query as a
  bounded table.

Start with [Managed state](ray-native-state.md), [Event time](event-time.md),
and [SQL and Table APIs](sql.md). Their supported combinations are summarized
in [Operator compatibility](operator-compatibility.md).

## Recovery and checkpoint-aware outputs

A streaming checkpoint coordinates source positions, managed state, timers,
and sink preparation. Durable storage is the cluster-loss boundary; Ray's
Object Store is only an optional recovery cache. The filesystem and Iceberg
integrations publish checkpoint-aware output. Other sources and sinks,
including Kafka, Redis, the RocketMQ source, console, SQL, and custom
integrations, retain their documented system-specific guarantees.

Read these contracts together:

- [Delivery semantics](delivery-semantics.md) explains how source replay,
  state, checkpoints, and external effects compose.
- [Checkpoint storage](checkpoint-storage.md) covers durable locations and
  object integrity.
- [Restore and rescale](checkpoint-recovery.md) covers compatible
  resubmission from a completed checkpoint.
- [Filesystem output](connectors/filesystem.md) and
  [Iceberg output](connectors/iceberg.md) document their publication
  boundaries.

Klein does not label an entire job exactly-once merely because its internal
state is consistent. End-to-end behavior still depends on every source and
destination in the graph.

## Live control without driver ownership

Submitted streaming jobs use detached control actors, so the original Python
driver can exit while the job continues. Another process can inspect or stop
the job with the CLI or public state API. The Dashboard provides the same
snapshot model and can request a supported operator rescale.

Live rescaling changes one physical operator at an ordered, checkpointed cut.
It is an explicit operation rather than a built-in metric-driven streaming
autoscaler. Operator type, job health, key-group capacity, available Ray
resources, and checkpoint stabilization all constrain the request.

Use [Driver fault tolerance](driver-fault-tolerance.md),
[Observability](observability.md), [CLI reference](cli-reference.md), and
[Live operator rescaling](operator-rescaling.md) as one operational set.
Detached actors survive a driver exit, but they do not replace durable recovery
after Ray head, JobManager-process, or whole-cluster loss.

## Optional Ray Serve execution regions

Klein can replace one connected linear region of synchronous NumPy/default
`map_batches` transforms with an asynchronous client to a separately deployed
Ray Serve application. This is useful when model-serving lifecycle and scaling
must remain independent from the surrounding batch or streaming job.

The remote service is outside Klein's checkpoint transaction. Requests can be
retried after ambiguous network failures, so served functions should be
deterministic and free of non-idempotent external side effects. See the
[Ray Serve integration guide](connectors/ray-serve.md) for topology,
serialization, timeout, retry, and security constraints.

## Choose the next guide

| If you want to... | Continue with... |
| --- | --- |
| Build a first end-to-end stream | [Production streaming walkthrough](production-streaming.md) |
| Implement UDFs and control ordering or resources | [DataStream programming](datastream-programming-guide.md) |
| Select an input, output, lookup, or execution integration | [Connector catalog](connectors/index.md) |
| Understand unsupported combinations before designing a job | [Operator compatibility](operator-compatibility.md) and [Known limitations](limitations.md) |
| Approve a deployment | [Production readiness checklist](production-readiness.md) |

Klein is alpha software. Treat the feature guides as behavioral contracts for
the documented compatible release, then validate the exact topology and
connectors in a representative environment.
