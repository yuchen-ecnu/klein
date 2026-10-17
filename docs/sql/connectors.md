<!-- SPDX-License-Identifier: Apache-2.0 -->

# Table DDL and connectors

Use SQL DDL to declare input and output tables, then query them or submit
`INSERT INTO ... SELECT`. This page covers the SQL entry point. The
[connector catalog](../connectors/index.md) documents each connector's
Python API, options, and delivery guarantees; a Python connector is usable
through Table DDL only when a table factory is registered for it. Built-in
Table DDL factories are `filesystem`, `kafka`, and `print`.

## Define connectors with Flink-style Table DDL

Catalog tables follow Flink Table DDL: the schema is logical metadata and the
`WITH` map selects and configures a connector factory. Creating a table does
not open files, create Kafka consumers, or launch Ray tasks.

```python
import ray.klein as klein

pipeline = klein.pipeline(name="table-insert")
pipeline.execute_sql("""
    CREATE TEMPORARY TABLE input_events (
        event_id BIGINT NOT NULL,
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topics' = 'events',
        'bootstrap_servers' = 'localhost:9092',
        'start_offset' = 'earliest',
        'end_offset' = 'latest',
        'override_num_blocks' = '4'
    )
""")

pipeline.execute_sql("""
    CREATE TABLE output_rows (
        event_id BIGINT,
        payload STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/tmp/output',
        'format' = 'parquet'
    )
""")

handle = pipeline.execute_sql("""
    INSERT INTO output_rows
    SELECT event_id, payload FROM input_events
""")
handle.wait()
```

A single `INSERT INTO` submits directly. Group multiple inserts with one
statement set when they must share a job:

```python
pipeline.execute_sql("""
    CREATE TABLE audit_rows (event_id BIGINT, payload STRING) WITH (
        'connector' = 'print'
    )
""")
statements = pipeline.create_statement_set()
statements.add_insert_sql("INSERT INTO output_rows SELECT * FROM input_events")
statements.add_insert_sql("INSERT INTO audit_rows SELECT * FROM input_events")
statements.execute().wait()
```

In streaming mode, the filesystem sink is checkpoint-transactional. Part files
remain below a hidden `.klein-staging` path until their committable is present
in durable checkpoint metadata, then the coordinator publishes them
idempotently. Flink-style options such as `sink.parallelism`,
`sink.rolling-policy.file-size`, `sink.rolling-policy.rollover-interval`, and
`sink.rolling-policy.inactivity-interval` configure the native sink. See the
[Filesystem connector](../connectors/filesystem.md) for the full lifecycle and
option table.

Built-in connector identifiers are `filesystem`, `kafka`, and `print`.
The dedicated [connector catalog](../connectors/index.md) lists their complete
option sets, defaults, data shapes, and delivery guarantees.
Applications can implement `TableFactory` and register it with
`SQLSession.register_table_factory()`. A factory validates table options at
`CREATE TABLE` time and binds a source or sink only when the table is read or
used by `INSERT INTO`.
Third-party packages can publish factories through the
`ray.klein.table_factories` Python entry-point group.

Kafka is deliberately not a second Klein-specific API. Its table options and
Python methods reuse Ray Data 2.56 names: `topics`, `bootstrap_servers`,
`trigger`, `start_offset`, `end_offset`,
`consumer_config`, resource options, and `timeout_ms` for reads; `topic`,
`key_field`, serializers, `producer_config`, `ray_remote_args`, and
`concurrency` for writes. Complex option values use JSON strings.

Message encodings remain formats owned by the physical connector. For example,
Canal CDC uses `'connector'='kafka'`, `'format'='canal-json'`, with
`canal-json.*` format options; it is not registered as a separate connector.

The connector also validates the read-side `concurrency`,
`partition_discovery_interval_ms`, and `max_batch_size` options used by the
Python continuous source. A Kafka table with `'trigger' = 'continuous'`
selects the streaming SQL planner automatically.
