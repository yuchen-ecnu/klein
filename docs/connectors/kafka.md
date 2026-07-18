---
myst:
  html_meta:
    description: "Configure Klein for Ray Kafka bounded and continuous input, output, checkpoint offsets, and Table DDL."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-kafka-connector)=
# Kafka

Klein reads Kafka either as a bounded Ray Data input or as a native continuous
source. Kafka output lowers to Ray Data in batch and uses a native producer in
streaming. Install the connector dependency with:

```bash
python -m pip install "ray-klein[kafka]"
```

## Read Kafka

```python
import ray

events = ray.klein.read_kafka(
    ["orders", "refunds"],
    bootstrap_servers="kafka:9092",
    trigger="continuous",
    start_offset="earliest",
    consumer_config={"group.id": "billing-pipeline"},
    concurrency=8,
)
```

Set `trigger="once"` for bounded Ray Data input or `"continuous"` for a
long-running checkpoint-aware source.

### Input options

| Argument | Default | Meaning |
|---|---:|---|
| `topics` | Required | One topic or a sequence of topics. |
| `bootstrap_servers` | Required | Broker address string or sequence. A value in `consumer_config` is ignored. |
| `trigger` | `"once"` | `once` for bounded input; `continuous` for native streaming. |
| `start_offset` | `"earliest"` | Integer, datetime, `earliest`, `latest`, or per-topic/per-partition mapping. |
| `end_offset` | `"latest"` | Bounded end offset; a non-default value is unsupported for continuous input. |
| `consumer_config` | `None` | Additional Confluent consumer properties. JSON object in Table DDL. |
| `override_num_blocks` | `None` | Ray Data output blocks; for continuous input it must equal `concurrency` if both are set. |
| `concurrency` | `None` | Continuous source subtasks. Not accepted by `trigger="once"`. |
| `partition_discovery_interval_ms` | `30000` | Continuous partition refresh interval. |
| `max_batch_size` | `1000` | Maximum records emitted by a continuous poll batch. |
| `timeout_ms` | `None` | Poll/read timeout; continuous input uses 1000 ms when unset. |
| `num_cpus`, `num_gpus` | `None` | Source task resources. |
| `memory`, `ray_remote_args` | `None` | Bounded Ray Data resource options; unsupported for continuous input. |

An integer start offset must be non-negative. A naive `datetime` is interpreted
as UTC. A partition-specific mapping has this shape:

```python
start_offset = {
    "orders": {0: 1200, 1: "earliest"},
    "refunds": {0: "latest"},
}
```

Continuous `timeout_ms`, `partition_discovery_interval_ms`, and
`max_batch_size` must be positive integers when set. Topic names, broker
addresses, and configured partition IDs are validated before polling.

### Input record schema

Continuous input emits one mapping per Kafka record:

| Field | Type | Meaning |
|---|---|---|
| `topic` | `str` | Topic name. |
| `partition` | `int` | Partition number. |
| `offset` | `int` | Kafka record offset. |
| `key` | `bytes \| None` | Unmodified key bytes. |
| `value` | `bytes \| None` | Unmodified value bytes. |
| `timestamp` | `int \| None` | Kafka timestamp in milliseconds. |
| `timestamp_type` | implementation value | Kafka timestamp type. |
| `headers` | `dict` | Header names and values. |

Deserialize `key` and `value` explicitly in the next transform so schema and
error handling remain application-owned.

### Partition assignment and recovery

The continuous source sorts discovered topic partitions and assigns them by
partition index modulo source parallelism. It periodically discovers new
partitions without restarting the job. Empty polls still report source idleness
so time-based checkpoints and watermarks can progress.

Resume position is chosen in this order:

1. offsets restored from a durable Klein checkpoint;
2. positions already held by the running source;
3. committed offsets for the configured Kafka consumer group;
4. `start_offset`.

Klein forces `enable.auto.offset.store=false` and disables automatic commits;
`enable.auto.commit=true` is rejected. If `group.id` is absent, Klein uses
`ray-klein-{job_id}`. It commits Kafka group offsets only after the corresponding
Klein checkpoint is durable and retries a failed commit. This prevents the
consumer group from moving ahead of recoverable Klein state.

## Write Kafka

```python
events.write_kafka(
    "normalized-orders",
    bootstrap_servers="kafka:9092",
    key_field="order_id",
    key_serializer="string",
    value_serializer="json",
    producer_config={"compression.type": "zstd"},
    concurrency=8,
)
```

### Output options

| Argument | Default | Meaning |
|---|---:|---|
| `topic` | Required | Exactly one output topic. |
| `bootstrap_servers` | Required | Broker addresses; overrides any same property in `producer_config`. |
| `key_field` | `None` | Optional mapping field used as the Kafka key. |
| `key_serializer` | `"string"` | `string`, `json`, or `bytes`. |
| `value_serializer` | `"json"` | `json`, `string`, or `bytes`. |
| `producer_config` | `None` | Additional Confluent producer properties. |
| `ray_remote_args` | `None` | Ray Data batch task arguments; `num_cpus` and `num_gpus` also size native sink tasks. |
| `concurrency` | `None` | Sink parallelism. |

`json` uses JSON encoding, `string` uses `str(value).encode("utf-8")`, and
`bytes` preserves an existing `bytes` value but UTF-8 encodes `str(value)` for
other types. If `key_field` is absent or its value is `None`, the producer sends
no Kafka key. The value serializer receives the whole record mapping.

The streaming producer flushes outstanding deliveries at checkpoint barriers
and waits for acknowledgements, but it does not use Kafka transactions. Output
is therefore **at-least-once**: recovery can replay records that Kafka already
accepted before the failed checkpoint became durable. Use an idempotent
downstream key/version design when duplicates matter.

## Use Table DDL

The connector identifier is `kafka`. `consumer_config`, `producer_config`, and
`ray_remote_args` are JSON strings in SQL:

```sql
CREATE TABLE orders (
    order_id BIGINT,
    payload BYTES
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'bootstrap_servers' = 'kafka:9092',
    'trigger' = 'continuous',
    'start_offset' = 'earliest',
    'concurrency' = '8',
    'value_serializer' = 'json'
);
```

Source tables require `topic` or `topics` plus `bootstrap_servers`. Sink tables
require exactly one topic. Supported keys are the snake-case API names shown in
the option tables: `trigger`, offsets, consumer/producer config, resources,
block/concurrency settings, discovery and batch size, and serializers. See
[SQL and Table DDL](../sql.md) for statement syntax and row-kind validation.

## Operational notes

- Keep Klein checkpoint storage durable; Kafka offset commits alone cannot
  restore operator state.
- Use a stable `group.id` when recovery must survive a new Klein job identity.
- Scale continuous source concurrency no higher than useful topic-partition
  parallelism. Idle subtasks do not increase Kafka throughput.
- Monitor checkpoint duration, consumer lag, records consumed/produced, and
  producer error metrics described in [Observability](../observability.md).
