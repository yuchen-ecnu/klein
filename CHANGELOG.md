<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project intends to follow [Semantic Versioning](https://semver.org/) after 1.0.

## [Unreleased]

### Added

- Standalone `ray-klein` distribution contributing the `ray.klein` subpackage.
- Standard build metadata, CI, security policy, governance, contribution guide,
  issue templates, and Apache-2.0 licensing artifacts.
- Ray-style module readers such as `ray.klein.read_csv`, plus the
  version-adaptive `stream.data` namespace for installed Dataset methods.
- Unified configuration from `RAY_KLEIN_*`, mappings, JSON or `key=value`
  strings, and an explicit process-global `KleinContext`.
- SQLGlot-based SQL over bounded DataStreams with caller-scope discovery,
  explicit table bindings, persistent temporary views, and native Ray Dataset
  lowering for filters, joins, aggregates, ordering, limits, and unions.
- Flink-style `CREATE TABLE ... WITH`, `DROP TABLE`, and `INSERT INTO` syntax
  backed by lazy, extensible source/sink connector factories.
- URI-aware durable checkpoints for local filesystems, S3-compatible object
  stores, and Google Cloud Storage, with Flink-style job directories,
  `_metadata` commit markers, integrity checks, discovery, and retention.
- A stable cluster state API for job inventory, operator graphs, throughput,
  backpressure, checkpoints, redacted configuration, failure details, and
  cancellation, backed by a detached head-node actor and last-good snapshots.
- Component-scoped text and JSON operational logging with stable event names,
  Ray job/task context, credential-field redaction, and architecture checks for
  logging and stdout boundaries.
- Driver-independent control-plane attachment through a detached, restartable
  Ray actor, plus a documented durable-checkpoint recovery boundary.
- PEP 561 typing metadata, a lazy top-level API, signed-off contribution checks,
  documentation deployment, dependency review, CodeQL, secret scanning,
  OpenSSF Scorecard, nightly integration, and release SBOM generation.

### Changed

- The generic KV connector was replaced by an explicit Redis API built around
  `RedisConnectionConfig`, `RedisSinkConfig`, `RedisSink`, and
  `DataStream.write_redis`. Redis hashes, lists, and sets now use idempotent,
  transactional replacement semantics, and list writes preserve input order.
- Tests are organized into explicit unit, state, architecture, Ray integration,
  and opt-in external-service tiers, with shared fixtures, bounded waits,
  temporary-path isolation, strict pytest configuration, and tiered CI jobs.
- Imports and source layout are unified under `ray.klein` for future inclusion
  in the main Ray repository.
- Compatibility is scoped to the Ray 2.56 minor line; the earlier 2.50 target
  was dropped after dependency auditing found known vulnerabilities fixed in
  later Ray releases.
- Embedded streaming startup no longer claims a fixed private Ray metrics port;
  applications can pre-initialize Ray when custom runtime settings are needed.
- Protobuf is constrained below 7 because supported Ray Serve versions use a descriptor
  attribute removed by protobuf 7.
- CI enforces a 65% branch-coverage floor with focused unit, state,
  architecture, integration, and external connector tiers.
- Handwritten Ray Data source and sink mirrors were replaced by lazy dynamic
  calls through the `data` adapters.
- Kafka's public read/write contract now matches Ray Data 2.56, including its
  bounded `read_kafka` offset model and native serializers. Continuous
  `KafkaSource` accepts confluent-kafka settings directly through
  `consumer_config` and owns deterministic partition/checkpoint state.
- The raw checkpoint directory key is now `execution.checkpointing.dir`
  instead of `state.checkpoints.dir` and accepts object-store URIs.
- Durable checkpoint directories use an independent metadata revision, keeping
  storage publication order separate from source barrier identifiers.
- Source shutdown separates cooperative `cancel()` from one-time resource
  cleanup in `close()`. Every source, sink, and collect subtask now materializes
  exactly one lifecycle instance from a class.
- Built-in metrics are registered through kind-specific counter, gauge, and
  histogram contracts instead of unchecked union casts.
- Console sinks now write parseable JSON Lines records to stdout while
  operational logs use stderr. Metrics use a `ray_klein` hierarchy, stable job,
  task, and operator labels, and unit-suffixed names.
- Optional Kafka, Redis, RocksDB, and Ray Serve dependencies no longer load or
  install with the minimal package, and in-memory managed state is the safe
  default while durable checkpoints remain the recovery boundary.
- Asynchronous emit-pipeline shutdown is cancellation-safe and cannot leave a
  worker task pending when an event loop closes.

### Removed

- The Java-style `KVSystemType`, `KVDataType`, `KVSink`, `KVFetcher`,
  `KVFilter`, magic KV record fields, and `DataStream.write_kv` APIs.
- Private Red-Ray DataOps, EDS, RedTable, RedKV, registry, and endpoint dependencies.
- DuckDB as a SQL parser/execution dependency and the single-task Arrow bridge.
- Deprecated configuration aliases, un-namespaced job execution, and the
  `parallelism` source argument.
- Single-value checkpoint modes and the progress-snapshot naming layer; source
  checkpoints now use independent record and duration thresholds.
- Pre-created source/sink lifecycle instances, `kafka_auth_config`, and the
  previous checkpoint metadata schema. These contracts are intentionally not
  adapted at runtime.
