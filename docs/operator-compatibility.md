---
myst:
  html_meta:
    description: "Klein for Ray operator support matrix for batch and streaming execution, ordering, state, and changelog behavior."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Operator compatibility and execution semantics

Use this page before combining operators in one graph. `auto` selects streaming
when a source is unbounded, any graph vertex lacks a batch lowering, or
`udf.ignore-exception=true`; only a fully batch-lowerable, bounded graph with
that policy disabled selects batch. Selection happens for the whole job, not
independently for each branch.

`Yes` below means Klein provides a native implementation for that execution
mode. `Ray Data` means the behavior, partitioning, retries, and schema rules are
owned by the installed compatible Ray Data version.

## Sources and adapters

| API | Batch | Streaming | Notes |
|---|---:|---:|---|
| Dynamic `ray.klein.read_*` Ray Data factory | Yes | No | The selected public Ray Data factory defines its arguments and result schema. |
| `from_ray_dataset()` | Yes | No | Keeps the existing Dataset lazy; it is not converted to an unbounded source. |
| `from_items()` / `from_values()` | Yes | Yes | Bounded by default; a finite native streaming source is used when the graph selects streaming. |
| `read_kafka(trigger="once")` | Yes | No | Delegates to Ray Data. |
| `read_kafka(trigger="continuous")` | No | Yes | Checkpoint-aware native source. |
| `read_canal()` | No | Yes | Continuous Kafka input decoded to changelog rows. |
| `read_rocketmq()` | No | Yes | Broker-managed consumer progress; not checkpoint-aligned. |
| `ray.klein.source()` | No by default | Yes | A custom source needs an explicit batch lowering to run in batch; the public helper currently builds native sources. |
| `stream.data.with_column(name, expr)` | Ray Data | Yes | Streaming evaluates Ray expressions per record; `download()` uses bounded ordered async I/O. |
| `stream.data.filter(expr=expr)` | Ray Data | Yes | Supports Ray Data expression predicates in both modes. |
| Other `stream.data.*` | Ray Data | No | Other Dataset transforms and terminal consumers remain batch-only. |

## DataStream transformations

| API | Batch | Streaming | Partitioning, state, and ordering |
|---|---:|---:|---|
| `map()` | Yes | Yes | One output per input unless the UDF error policy drops the record. Preserves per-input order within a subtask. |
| `flat_map()` | Yes | Yes | Zero or more outputs per input. Outputs yielded for one input remain ordered. |
| `filter()` | Yes | Yes | Preserves retained-record order within a subtask. |
| `map_batches()` | Yes | Yes | Uses the requested batch format; streaming batches are bounded by size/timeout. |
| `map_reduce()` | No | Yes | Composite adaptive-shuffle → batch-process → keyed-reduce pipeline. |
| `union()` | Yes | Yes | Does not define a total order across inputs. |
| `assign_timestamps_and_watermarks()` | No | Yes | Adds ordered event-time control messages. |
| `key_by()` / `group_by()` | No | Yes | Hashes stable serialized keys into key groups and returns `KeyedStream`. |
| `KeyedStream.process()` | No | Yes | Managed keyed state and processing/event-time timers. |
| `KeyedStream.window().reduce()` | No | Yes | Tumbling, sliding, or session event-time windows. |
| `join()` / `interval_join()` | No | Yes | Two-input keyed event-time interval join with managed state. |
| `DataStream.sql()` / `ray.klein.sql()` | Yes | Subset | See [SQL support](sql.md#how-does-sql-execute) for the two planner feature sets. |

Stateful operators require non-negative integer millisecond timestamps. A
timestamp selector alone does not advance event time: a source or
`WatermarkStrategy` must emit watermarks. Late windows and interval-join rows
whose cleanup time is already behind the current watermark are dropped and
counted by `late_records_dropped`.

## Partitioning methods

`broadcast()`, `rescale()`, `round_robin()`, `adaptive_shuffle()`, and
`partition_by()` configure the next edge in the native streaming graph. They do
not control Ray Data partitions in batch mode. For batch repartitioning and
sorting, use the matching `stream.data` Dataset method.

| Method | Native streaming behavior | Typical use |
|---|---|---|
| `broadcast()` | Sends every record to every downstream subtask. | Small reference/config streams. |
| `round_robin()` | Cycles records across all downstream subtasks. | Even distribution without key affinity. |
| `rescale()` | Connects each upstream subtask to a subset of downstream subtasks. | Localized scale changes with less fan-out. |
| `adaptive_shuffle()` | Reroutes around downstream write timeouts. | Uneven or temporarily busy workers. |
| `partition_by(callable)` | Routes by the callable's selected target/key contract. | Application-specific placement. |
| `key_by(callable)` | Stable key-group partitioning. | Managed keyed state, windows, and joins. |

Partitioning methods mutate the edge used by the next operator. Apply them
immediately before the operation whose input placement they should control.

## Terminal operations and sinks

| API | Batch | Streaming | Delivery behavior |
|---|---:|---:|---|
| `show()`, `take()`, `take_all()`, `schema()` | Yes | Yes | Lazy diagnostic terminals; `take()` drains after reaching its limit. |
| `write_json/csv/parquet()` | Ray Data | Yes | Streaming output is checkpoint-transactional. |
| `write_iceberg()` | Ray Data | Append | Streaming appends are checkpoint-transactional; overwrite and upsert remain batch-only. |
| `write_text()` | No | Yes | Native checkpoint-transactional text sink. |
| `write_kafka()` | Ray Data | Yes | Native streaming output is at-least-once. |
| `write_sql()` | Ray Data | Yes | Native streaming DB-API output is at-least-once. |
| `write_redis()` | No | Yes | Native streaming sink; replacement commands are retry-safe but the end-to-end job remains at-least-once. |
| `write(SinkFunction, ...)` | No by default | Yes | Semantics are defined by the custom sink. |
| `write(TwoPhaseCommitSinkFunction, ...)` | No by default | Yes | Prepared output is published only after durable checkpoint metadata. |

See [Delivery and consistency guarantees](delivery-semantics.md) before mixing
a source and sink, and [Performance tuning](performance-tuning.md) before
changing concurrency or buffer sizes.

## Changelog compatibility

Ordinary mappings are append-only `INSERT` rows. Continuous SQL and Canal JSON
can produce `ChangelogRow` values with insert, update-before, update-after, and
delete kinds. A sink or Table factory must declare and validate the row kinds
it accepts. Filesystem, Redis, Kafka, SQL, and arbitrary custom sinks do not
automatically materialize retractions into an up-to-date table; choose an
upsert/retraction-aware sink when consuming updating queries.
