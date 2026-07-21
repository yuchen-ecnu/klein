---
myst:
  html_meta:
    description: "Definitions of Klein for Ray streaming, state, checkpoint, SQL, scheduling, and operations terminology."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-glossary)=
# Glossary

This glossary defines terms as Klein uses them. A similarly named Ray, Flink,
database, or broker concept can have a different lifecycle or guarantee.

Actor
: A stateful Ray process addressed through a handle. Klein's native streaming
  runtime uses actors for long-lived tasks and coordinators.

Aligned checkpoint
: A checkpoint in which an operator waits for the same barrier epoch on every
  active physical input before snapshotting state and forwarding the barrier.
  Post-barrier input is held until the cut is complete.

At-least-once
: A delivery contract that preserves records inside its stated recovery
  boundary but can repeat a logical record or external effect after failure.

Backpressure
: Upstream slowing caused by bounded downstream capacity or a slow external
  operation. In Klein it is visible through queue, transport, and
  backpressure-duration metrics.

Barrier
: An ordered control message separating records before and after one
  checkpoint epoch. A barrier cannot overtake earlier records on the same edge.

Batch execution
: The execution path that lowers a compatible bounded Klein graph to Ray Data
  Dataset operations.

Bounded source
: A source with a finite input that can eventually report completion. Bounded
  describes input lifetime, not data size.

Changelog row
: A mapping carrying a `RowKind`: insert (`+I`), update-before (`-U`),
  update-after (`+U`), or delete (`-D`). Dynamic-table queries and CDC formats
  can emit changelog rows.

Checkpoint
: A coordinated, durable recovery point containing source positions, managed
  state, timers, watermarks, and supported sink committables. A completed
  checkpoint is published only after its referenced state is durable.

Checkpoint domain
: A weakly connected component of the physical streaming graph that shares a
  checkpoint epoch. Disconnected components can make checkpoint progress
  independently.

Checkpoint-transactional sink
: A sink that keeps prepared output private until the matching checkpoint is
  durable and whose commit can be retried idempotently. This term describes
  visibility at the sink boundary, not every side effect in an end-to-end job.

Chaining
: Combining compatible adjacent operators into one physical task to remove an
  actor and serialization boundary. The chain becomes one scheduling, failure,
  and live-rescaling unit.

Collector
: The runtime interface through which a source or operator emits records and
  ordered control messages downstream.

Committable
: A serializable description of prepared sink output. The checkpoint
  coordinator persists it and performs the second-phase commit after the global
  checkpoint becomes durable.

Concurrency
: The number of physical workers for an operator. In Ray Data, a `(min, max)`
  tuple can describe an autoscaling actor pool; in native streaming, the lower
  value is the initial parallelism and live changes are explicit.

Control plane
: The components that compile, schedule, monitor, checkpoint, recover, and
  control a job. They coordinate the data plane but do not proxy every record.

Data plane
: The ordered physical paths over which streaming tasks exchange records,
  micro-batches, barriers, and watermarks.

DataStream
: A lazy logical stream plus the operations required to produce it. Calling a
  transformation builds a graph; a sink and terminal execution submit it.

Detached actor
: A Ray actor whose lifetime is not owned by the driver that created it. Klein
  uses detached control actors so a submitted streaming job can outlive its
  original driver.

Driver
: The Python process that builds the logical graph through module-level APIs
  and submits it. The driver is distinct from the streaming JobManager.

Dynamic table
: The relational view of a changing stream. Each input change can update the
  materialized table and produce one or more changelog rows.

Event time
: Time carried by records and advanced by watermarks, rather than the worker's
  wall clock. Event time drives event-time windows and timers.

Execution mode
: `batch`, `streaming`, or `auto`. The selected mode applies to the whole job.

Execution graph
: The physical graph after logical optimization, chaining, partitioning, and
  expansion by concurrency. Its vertices represent deployable task instances.

Idle input
: A physical input temporarily excluded from a multi-input watermark minimum
  because it has no data. It becomes active before a resumed record is
  processed.

Interactive mode
: A development mode in which a bounded terminal operation runs its graph and
  returns a result immediately. It is separate from in-process debug mode.

Job ID
: The identifier used by the state API and published snapshots. In the current
  streaming runtime it is the Klein Ray namespace; consumers should still read
  the `job_id` field instead of deriving it.

JobManager
: The per-job control actor that owns job status, the execution graph,
  scheduling, recovery, progress snapshots, and live operator rescaling.

Key group
: A stable bucket in the keyed-state hash space. Contiguous key-group ranges
  move between subtasks when stateful operator parallelism changes.

Keyed state
: Managed state scoped first to an operator and then to the current record key.
  Klein exposes value, list, and map handles through `KeyedStateContext`.

KleinContext
: The advanced explicit owner used to isolate graph construction. Ordinary
  module-level APIs keep this implementation detail out of application code.

Logical graph
: The lazy user-facing graph before optimization, chaining, resource planning,
  and physical task expansion.

Managed state
: State whose lifecycle, partitioning, snapshot, restore, TTL, and rescaling
  are coordinated by Klein rather than manually stored in a UDF object.

Maximum parallelism
: The fixed number of key groups configured by
  `state.keyed.max-parallelism`. It bounds keyed operator parallelism and is
  checkpoint compatibility metadata.

Micro-batch
: A bounded group of logical records transferred together in the native
  streaming data plane. It reduces RPC overhead without changing record-level
  operator semantics.

Namespace
: The Ray namespace containing a streaming job's named actors. An explicit
  `job.namespace` is useful for operations; otherwise Klein creates a unique
  `klein-<job>-<id>` value.

Object Store
: Ray's distributed shared-memory store for immutable objects. Klein can cache
  large snapshot fragments there for fast recovery, but the Object Store is not
  durable checkpoint storage.

Operator
: A logical transformation or source/sink stage. Chaining can place several
  logical functions in one physical operator.

Operator parallelism
: The number of physical subtasks executing an operator. It is distinct from
  Ray cluster node count and from `DataStream.rescale()` partition routing.

Partitioner
: The routing policy from an upstream operator to downstream subtasks. Built-in
  policies include forward, round-robin, rescale, broadcast, key, and adaptive
  routing.

Processing time
: Time from the task process's clock. It can drive processing-time timers but
  does not describe when an event occurred at the source.

Replay buffer
: Bounded upstream data retained until downstream progress acknowledges it. It
  supports single-task at-least-once recovery and fails fast at its configured
  byte limit.

Resource plan
: The compiled description of operator concurrency, CPU/GPU requests, and graph
  edges returned by `explain()` and optionally persisted for inspection or
  override.

Restore path
: The completed checkpoint URI supplied through `execution.savepoint.path`
  when resubmitting a job. Despite the option name, Klein does not yet create a
  separately managed Flink-style savepoint.

Row kind
: The change operation attached to a changelog row: insert, update-before,
  update-after, or delete.

Sink
: A terminal operator that publishes or collects a stream. Creating one
  registers it for the next execution; passing sink roots to `execute()` can
  select a subset explicitly. The older interactive terminal behavior is
  deprecated.

Source
: An operator that introduces records and source progress into a graph. A
  source is bounded or unbounded and can participate in checkpoint recovery.

State backend
: The worker-local implementation of managed state. Klein currently provides
  memory and optional RocksDB backends; both rely on durable checkpoints for
  recovery across worker or cluster loss.

State TTL
: A policy that expires keyed state after an idle duration according to its
  update and visibility rules. TTL bounds storage at the cost of forgetting old
  relationships.

Streaming execution
: Klein's native long-running actor runtime with ordered transport, managed
  state, watermarks, checkpoints, replay, and recovery.

Subtask
: One physical parallel instance of an operator, identified by a zero-based
  subtask index.

Timer
: A keyed callback registered for a processing-time or event-time timestamp and
  included in managed state snapshots.

Two-phase commit
: A sink protocol that prepares output during processing and publishes it only
  after checkpoint durability. Commit and abort operations must be idempotent.

Unbounded source
: A source that can continue producing records indefinitely and therefore
  selects native streaming execution in `auto` mode.

Watermark
: An event-time progress statement. A multi-input operator uses the minimum
  watermark of all active inputs, and an idle input does not hold back that
  minimum.

Window
: A finite event-time grouping assigned per key. Klein supports tumbling,
  sliding, and session window assigners through the native API.
