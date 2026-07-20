---
myst:
  html_meta:
    description: "Measure and tune Klein for Ray concurrency, batching, partitioning, buffers, replay, state, checkpoints, and placement."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Performance tuning

Tune from measurements, one boundary at a time. Increasing concurrency or
buffer capacity can move a bottleneck downstream, increase checkpoint
alignment time, and enlarge the replay window without increasing end-to-end
throughput.

## Establish a baseline

Record the following under representative input, state size, and external
service latency:

- records in/out per operator;
- processing p50/p95/p99 latency;
- input-buffer utilization and backpressure duration;
- source lag and idle inputs;
- checkpoint duration, alignment time, persistence time, and failures;
- managed-state size and snapshot/restore duration;
- replay-buffer records and bytes;
- CPU, memory, Object Store pressure, spilling, and network use per Ray node.

Use `ray.klein.explain()` to save the logical plan with every benchmark. A
different plan or concurrency topology is a different baseline.

## Diagnose by symptom

| Symptom | Inspect first | Likely actions |
|---|---|---|
| Source lag rises, downstream idle | Source poll duration, assigned partitions, source CPU | Increase source concurrency up to useful partitions; increase connector poll batch size; verify broker throttling. |
| One transform is CPU-bound | Operator processing latency and CPU | Increase that operator's concurrency/CPU; use `map_batches`; vectorize the UDF. |
| GPU/model calls underutilized | Batch fill time, async pending work, service latency | Increase `batch_size` carefully, set a bounded timeout, or use `async_buffer_size`; do not chain async operators. |
| High input-buffer utilization | Slow downstream operator, write timeout, backpressure metrics | Fix the consumer first; then adjust concurrency or batching. Larger buffers only absorb bursts. |
| Checkpoints are slow | Alignment, state snapshot, persistence, sink commit histograms | Reduce skew/in-flight buffers, use local RocksDB, review object-store latency and sink commit time. |
| Replay memory approaches limit | Replay bytes, downstream acknowledgements, checkpoint cadence | Remove downstream stalls, shorten the durability interval, or raise the limit only with measured memory headroom. |
| Placement startup times out | Cluster free resources and placement-group bundle | Reduce requests, add nodes, change placement strategy, or select balanced deployment. |
| Too many tiny output files | Checkpoint cadence and rolling thresholds | Increase checkpoint interval/records or file rolling limits while preserving recovery objectives. |

Checkpoint cadence is applied per weakly connected physical execution-graph
domain, not once per source subtask. All sources in one domain share an epoch;
physically disconnected lanes remain independent and can still produce one set
of files per lane and interval.

## Set operator concurrency

`concurrency` creates physical subtasks for native streaming operators and maps
to the relevant Ray Data operation in batch where supported. Choose it from the
scarcer of input parallelism, external-service capacity, and cluster resources.

- Kafka source concurrency above the topic-partition count creates idle
  subtasks.
- Keyed operator concurrency cannot exceed `state.keyed.max-parallelism`.
- Sink concurrency multiplies connections and transaction fan-out.
- Increasing concurrency during restore redistributes key groups; it does not
  change the max-parallelism compatibility value.

Keep explicit operator names when comparing plans and metrics across tuning
runs. [Autoscaling and live operator rescaling](operator-rescaling.md)
explains when a batch concurrency range is dynamic, how to resize a running
streaming operator, and why `DataStream.rescale()` does not change parallelism.

## Choose row or batch processing

Use `map()` and `flat_map()` for record-oriented logic. Use `map_batches()`
when a library benefits from columnar/vectorized input or amortized model calls.

The streaming batch boundary is controlled by an operator's `batch_size` and
`batch_timeout`. A large batch improves throughput but increases latency,
working memory, checkpoint alignment delay, and replay size. A timeout is
required when sparse traffic must not wait indefinitely for a full batch.

`pipeline.internal.batch-size`, `pipeline.internal.batch-max-rows`, and
`pipeline.internal.batch-max-bytes` control downstream transport micro-batches;
they are distinct from a user UDF's batch size. The first reached record, row,
byte, or idle-time threshold flushes a target lane. Output row/byte limits and
`pipeline.emit-queue.max-batches` are safety bounds, not throughput targets.

## Operator chaining and columnar passthrough

Operator chaining removes an actor hop only when adjacent operators have a
forward edge, matching resource/runtime contracts, no managed state, and no
async execution. Disable `pipeline.operator-chaining.enabled` when isolating a
hot operator, debugging lifecycle behavior, or assigning different resources.

`pipeline.columnar-passthrough.enabled` is on by default and keeps compatible
batches column-oriented across an edge. It removes column-to-row-to-column
conversion for batch-heavy UDFs; keyed/custom partitioning still performs
row-level slicing. Large duplicated broadcast batches are placed in Ray's
Object Store once after `pipeline.transport.object-store-threshold-bytes`.

## Partitioning and skew

- Use `key_by()` only when key affinity or state is required. A hot key remains
  hot regardless of total parallelism.
- Use `round_robin()` for even stateless distribution.
- Use `rescale()` to limit edge fan-out when upstream/downstream concurrency
  ratios align.
- Use `broadcast()` only for small streams; its network and processing cost
  multiplies by downstream concurrency.
- `adaptive_shuffle()` reacts to downstream write timeouts. It cannot repair a
  logically hot key whose state must stay on one subtask.

For batch execution, use `stream.data.repartition`, sorting, or the matching Ray
Data operation; Klein streaming partitioners do not tune Ray Data partitions.

## State backend and checkpoints

Memory state has low overhead for moderate state. RocksDB moves mutation-heavy
state to node-local storage and is usually preferable when state no longer fits
comfortably in worker memory. Put its local directory on fast disposable SSD.

The Object Store snapshot cache avoids repeatedly moving large immutable
snapshots through coordinator memory. Lowering its minimum size caches more
objects but increases Object Store metadata and capacity pressure.

Checkpoint cadence is a recovery-time objective:

- shorter intervals reduce the replay window and time since the last durable
  state, but add barrier, snapshot, storage, and sink-commit overhead;
- longer intervals improve steady-state throughput but increase recovery work
  and pending external transactions.

Record and duration triggers race; whichever fires first resets both. Do not
disable both unless the job intentionally accepts no periodic recovery point or
checkpoint-driven sink publication.

## Replay and backpressure bounds

The replay buffer keeps output until downstream progress confirms it is safe to
drop. `pipeline.replay-buffer.max-bytes` is a hard guard: exceeding it fails the
task into normal recovery before process OOM. Raising it should be the last step
after fixing stalled acknowledgements and verifying node memory.

`pipeline.input-buffer.size` and `pipeline.input-buffer.max-bytes` bound logical
rows and estimated payload bytes. One oversized columnar block is admitted only
into an otherwise empty inbox. The same dual bound applies to output buffering,
so wide records cannot bypass a row-only limit.

After every change, compare throughput, tail latency, checkpoint duration,
failure recovery time, and peak memory. Keep the change only when the full job,
not one isolated operator, improves.
