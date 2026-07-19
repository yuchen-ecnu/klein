---
myst:
  html_meta:
    description: "Klein for Ray delivery guarantees across sources, checkpoints, state, replay, and sinks."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Delivery and consistency guarantees

End-to-end delivery is the composition of source progress, operator state,
Klein checkpoints, replay, and sink publication. The weakest boundary in that
chain determines what an application can observe after failure.

Klein's general progress contract is **at-least-once**. Managed state is
restored consistently with checkpointed source positions, but an arbitrary
external sink may already have accepted output that is replayed after recovery.
Do not describe a job as exactly-once merely because it enables checkpoints.

## Terms

- **At-most-once** can lose records but does not deliberately replay them.
- **At-least-once** does not lose records inside its stated recovery boundary,
  but the same logical record may be processed or published more than once.
- **Checkpoint-transactional visibility** keeps prepared sink output private
  until checkpoint metadata is durable and makes commit retry idempotent.
- **Idempotent effect** means repeating an operation produces the same external
  state. It is an application or connector property, not a synonym for
  exactly-once processing.

## Source progress

| Source | Recovery position | Important boundary |
|---|---|---|
| Bounded Ray Data readers | Ray Data execution/retry state | Klein streaming checkpoints do not govern a batch reader. |
| Finite collection source | Source cursor in Klein checkpoint | Restore resumes from the checkpointed next record. |
| Continuous Kafka | Per-topic/partition next offsets in Klein checkpoint | Group offsets are committed only after the corresponding Klein checkpoint is durable. |
| Canal JSON over Kafka | Kafka offsets plus the entire decoded FlatMessage | A checkpoint cannot split recovery in the middle of one Kafka message. |
| RocketMQ | Broker-managed consumer-group progress | Broker acknowledgement can move ahead of the last durable Klein checkpoint; a full job rollback may lose those messages. |
| Custom `SourceFunction` | Opaque value from `snapshot_state()` | Correctness depends on capturing the next read position and implementing idempotent `notify_checkpoint_complete()`. |

For a custom source, advance the local next-position state before calling
`SourceContext.collect()`. The checkpoint barrier emitted by that call can then
record the position corresponding to all preceding records.

## Stateful operators and replay

Checkpoint barriers align source positions, managed keyed state, timers,
operator watermarks, and prepared sink committables. A restored stateful
operator therefore sees the state that belongs to the restored input position.

The replay buffer retains emitted native-streaming batches until downstream
progress acknowledges them. It improves single-task recovery and prevents an
upstream task from forgetting data that a failed downstream task had not made
durable. Disabling `pipeline.replay-buffer.enabled` changes the recovery path to
a broader job restart; it does not upgrade or downgrade an external sink's
transaction semantics.

## Sink behavior

| Sink | Streaming guarantee | Duplicate/loss guidance |
|---|---|---|
| Filesystem | Checkpoint-transactional visibility | Final part publication is idempotent. Readers must ignore `.klein-staging`. |
| `TwoPhaseCommitSinkFunction` | Checkpoint-transactional when correctly implemented | `transaction_id` commit and abort operations must be idempotent across retries. |
| Kafka | At-least-once | Klein waits for delivery acknowledgements but does not use Kafka transactions. Use a stable key/version or downstream deduplication. |
| SQL | At-least-once | Use an idempotent statement, unique key, or database-native upsert. |
| Redis | At-least-once external effect | Built-in replacement operations are idempotent for one key/value state, but cross-key atomicity is not provided. |
| Console | At-least-once diagnostic output | Duplicate JSON Lines are expected after replay. |
| Custom `SinkFunction` | Connector-defined, normally at-least-once | `flush()` is a barrier boundary, not an external transaction by itself. |
| Ray Data writer | Ray Data-defined | Consult the selected Dataset writer; Klein streaming checkpoints are not involved. |

## Failure-scenario checklist

Before production, test the following with the chosen source/sink pair:

1. Kill one transform worker while records are in flight.
2. Kill a sink worker after the external write but before checkpoint completion.
3. Restart the coordinator after checkpoint metadata is written.
4. Stop the submitting driver while the detached job continues.
5. Recreate the job from a durable checkpoint after losing the whole cluster.

For every test, record the accepted loss and duplicate envelope, how a duplicate
is detected, and which identifier makes the external effect idempotent. See
[Restore and rescale a job](checkpoint-recovery.md) for the recovery procedure
and [Observe Klein jobs](observability.md) for the relevant metrics.

