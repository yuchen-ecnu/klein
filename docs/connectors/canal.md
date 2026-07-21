---
myst:
  html_meta:
    description: "Decode Canal MySQL binlog FlatMessage JSON carried by Kafka into checkpointed Klein changelog rows."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-canal-json-format)=
# Canal JSON format

Klein consumes MySQL binlog changes that Canal Server publishes to Kafka in
FlatMessage JSON format. Canal JSON is a Kafka value format rather than a
physical connector. Install the Kafka dependency with:

```bash
python -m pip install "ray-klein[kafka]"
```

Configure Canal with Kafka MQ mode and JSON messages:

```properties
canal.serverMode = kafka
canal.mq.flatMessage = true
canal.mq.topic = canal-orders
```

Canal's protobuf MQ payload (`canal.mq.flatMessage=false`) and the Canal TCP
client protocol are not supported by this format.

## Read binlog changes

```python
import ray
import ray.klein

orders = ray.klein.read_kafka(
    "canal-orders",
    bootstrap_servers="kafka:9092",
    trigger="continuous",
    consumer_config={"group.id": "orders-cdc"},
    start_offset="earliest",
    concurrency=4,
    value_format="canal-json",
    format_options={
        "include_metadata": True,
        "ddl_handling": "ignore",
    },
)
```

For convenience, `ray.klein.read_canal(...)` is a thin typed wrapper around
this call. It selects continuous Kafka and supplies `value_format` and
`format_options`; it does not introduce a Canal connector or a second source
implementation.

The format is continuous-only and uses the Kafka connector's deterministic
partition assignment, checkpointed offsets, partition discovery, and durable
checkpoint commit behavior.

### Change mapping

| Canal type | Klein rows |
|---|---|
| `INSERT` | One `RowKind.INSERT` for every `data` row. |
| `DELETE` | One `RowKind.DELETE` for every `data` row. |
| `UPDATE` | An `UPDATE_BEFORE` and `UPDATE_AFTER` pair for every row. |
| DDL | Ignored, emitted as an INSERT metadata row, or rejected according to `ddl_handling`. |
| Transaction/control event | Ignored when it has no row data. |

For UPDATE, Canal's `data` entry is the after image while its aligned `old`
entry contains the old values of changed columns. Klein overlays `old` on
`data` to reconstruct the complete before image. Missing or misaligned `old`
images fail fast because silently emitting an incorrect retraction would
corrupt downstream state.

Canal FlatMessage values remain strings or `None`; Klein does not infer Python
values from `sqlType` or `mysqlType`. Cast fields explicitly in application or
SQL logic when numeric, temporal, or binary types are required.

### Metadata columns

`format_options["include_metadata"]=True` adds reserved columns without
replacing the row payload:

- `__canal_id`, `__canal_database`, `__canal_table`, and
  `__canal_event_type`;
- `__canal_execute_time`, `__canal_build_time`, `__canal_gtid`, and
  `__canal_pk_names` when present;
- `__canal_kafka_topic`, `__canal_kafka_partition`, and
  `__canal_kafka_offset`.

Set the option to `False` to emit only source-table columns. Reserved metadata
wins if a source table itself uses one of these names.

### DDL handling

`format_options["ddl_handling"]` accepts:

- `ignore` (default): advance the Kafka offset without emitting a row;
- `emit`: emit an INSERT metadata row containing `__canal_is_ddl=True` and
  `__canal_sql`;
- `fail`: stop on a DDL event.

## Recovery semantics

A single Canal Kafka record can contain several database rows, and every
UPDATE expands to two changelog rows. Klein checkpoints both the Kafka offset
and the next changelog-row index inside the current Kafka record. A recovery
therefore resumes at the exact before/after boundary instead of dropping or
replaying half of an UPDATE. Kafka group offsets are still committed only after
the corresponding Klein checkpoint is durable.

## Use Table DDL

Use the Kafka connector with the `canal-json` format. Canal-specific SQL
options use the `canal-json.` namespace:

```sql
CREATE TABLE orders (
    id STRING,
    status STRING
) WITH (
    'connector' = 'kafka',
    'format' = 'canal-json',
    'topic' = 'canal-orders',
    'bootstrap_servers' = 'kafka:9092',
    'consumer_config' = '{"group.id":"orders-cdc"}',
    'start_offset' = 'earliest',
    'canal-json.include-metadata' = 'false',
    'canal-json.ddl-handling' = 'ignore',
    'concurrency' = '4'
);
```

`format='canal-json'` defaults the table source to continuous mode. Set
`trigger='continuous'` explicitly when a self-documenting DDL is preferred.
The format is source-only; Kafka sinks continue to use `value_serializer`.

## Ordering note

Klein preserves the order delivered by each Kafka partition. End-to-end binlog
ordering still depends on Canal's topic and partition routing. Hashing by a
primary key preserves order for that key but a primary-key change can route the
before and after operations differently; choose Canal's routing configuration
according to the downstream ordering requirement.
