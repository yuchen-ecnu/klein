---
myst:
  html_meta:
    description: "Klein for Ray user guides for pipelines, state, event time, SQL, autoscaling, checkpointing, and operations."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-user-guides)=
# User guides

If you're new to Klein, complete [Getting started](getting-started.md) and read
[Key concepts](key-concepts.md) first. Use [Feature highlights](features.md) for
a separate product-oriented map of hybrid execution, state, event time, SQL,
recovery, live rescaling, detached jobs, and Ray Serve. The guides below cover
pipeline development and production behavior.

## Build dataflows

- [Program with DataStream and UDFs](datastream-programming-guide.md) is the
  complete guide to records, batches, callable lifecycles, asynchronous work,
  ordering, errors, resources, and partitioning.
- {doc}`Develop a pipeline <development>` provides a compact first reference
  for transformations, resources, partitioning, and integrations.
- [Manage the job lifecycle](job-lifecycle.md) covers contexts, sinks,
  compilation, submission, handles, namespaces, cancellation, and cleanup.
- [Build a production streaming pipeline](production-streaming.md) follows a
  Kafka job from event-time assignment through checkpointed file output,
  operations, and restore.
- [Check operator compatibility](operator-compatibility.md) lists batch and
  streaming support, partitioning, changelog behavior, and sink boundaries.
- [Choose and configure connectors](connectors/index.md) provides a dedicated
  directory tree for every built-in input, output, lookup, and Serve integration.
- [Use Ray Data operations](ray-data-interop.md) explains bounded readers and the `stream.data` adapter.
- [Query streams with SQL](sql.md) covers SQLGlot planning, temporary views, and Flink-style Table data definition language (DDL).
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
