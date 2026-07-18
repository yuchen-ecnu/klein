---
myst:
  html_meta:
    description: "Use Klein for Ray Redis value lookups, missing-key filters, and buffered output."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-redis-connector)=
# Redis

The Redis integration enriches or filters records in batch and streaming
pipelines and writes keyed values through a native streaming sink. Install it
with `python -m pip install "ray-klein[redis]"`.

```python
from datetime import timedelta
from ray.klein.integrations.redis import (
    RedisConnectionConfig,
    RedisDataType,
    RedisSinkConfig,
)

connection = RedisConnectionConfig(
    host="redis.internal",
    port=6379,
    database=0,
    timeout=timedelta(seconds=5),
)
```

## Configure connections

`RedisConnectionConfig` is immutable and shared by lookups and output:

| Field | Default | Meaning and constraint |
|---|---:|---|
| `host` | Required | Non-empty hostname or address. |
| `port` | `6379` | Integer from 1 through 65535. |
| `database` | `0` | Non-negative database number. |
| `username` | `None` | Optional ACL username. |
| `password` | `None` | Optional password; omitted from object representation. |
| `max_connections` | `10` | Positive per-operator connection-pool capacity. |
| `timeout` | 5 seconds | Positive connect and socket timeout. |
| `max_retries` | `5` | Non-negative Redis client retry count. |
| `max_retry_delay` | 10 seconds | Positive exponential-backoff cap. |
| `connection_options` | `{}` | Additional `BlockingConnectionPool` arguments. |

Each operator instance creates and owns its connection pool. Use a secret
manager or runtime environment for credentials rather than committing a
password in pipeline source.

## Enrich records

Pass the callable class and its constructor arguments to `map` so each worker
owns its Redis client:

```python
from ray.klein.integrations.redis import RedisDataType, RedisValueLookup

enriched = stream.map(
    RedisValueLookup,
    fn_constructor_args=[connection, lambda row: row["customer_id"]],
    fn_constructor_kwargs={
        "data_type": RedisDataType.HASH,
        "hash_fields": ["tier", "region"],
        "result_field": "customer",
    },
)
```

`RedisValueLookup` supports `STRING`, `HASH`, `LIST`, and `SET`. Its options are
`key_prefix`, key `delimiter` (default `:`), optional hash fields, and
`result_field` (default `redis_value`). Redis byte responses are decoded as
UTF-8. A missing string returns `None`; selected missing hash fields also
become `None`.

Batch-enabled transforms pipeline multiple non-transactional reads. Streaming
transforms issue one logical lookup per record. A Redis error is recorded and
raised so the normal task retry policy can apply.

## Keep only missing keys

`RedisMissingKeyFilter` retains a record only when its extracted, optionally
prefixed key does not exist:

```python
from ray.klein.integrations.redis import RedisMissingKeyFilter

new_records = stream.filter(
    RedisMissingKeyFilter,
    fn_constructor_args=[connection, lambda row: row["id"]],
    fn_constructor_kwargs={"key_prefix": "processed"},
)
```

The filter supports both execution modes and pipelines `EXISTS` calls in a
batch-enabled operator. A lookup followed by another client write is not an
atomic check-and-set; concurrent producers can race.

## Write keyed values

```python
stream.write_redis(
    connection,
    key=lambda row: row["id"],
    value=lambda row: row["attributes"],
    config=RedisSinkConfig(
        data_type=RedisDataType.HASH,
        key_prefix="customer",
        ttl=timedelta(hours=24),
        batch_size=200,
        flush_interval=timedelta(seconds=1),
        buffer_capacity=10_000,
    ),
    concurrency=4,
)
```

### Sink configuration

| `RedisSinkConfig` field | Default | Meaning and constraint |
|---|---:|---|
| `data_type` | `STRING` | `STRING`, `HASH`, `SET`, or `LIST`. |
| `key_prefix` | `None` | Optional non-empty namespace prefix. |
| `delimiter` | `:` | Non-empty prefix/key separator. |
| `ttl` | `None` | Positive expiration duration applied after replacement. |
| `batch_size` | `100` | Positive Redis pipeline batch size. |
| `flush_interval` | 1 second | Positive maximum buffering interval. |
| `buffer_capacity` | `5000` | Bounded buffer capacity; at least `batch_size`. |

`write_redis` also accepts `num_cpus`, `num_gpus`, `concurrency`, operator
`batch_size`, `batch_timeout` (default 3 seconds), and `name`. The operator
batch size controls how Klein delivers records to the sink; the identically
named `RedisSinkConfig.batch_size` controls the sink's internal Redis pipeline.

The sink uses `SET` for strings. It transactionally replaces hashes, lists, and
sets using `DEL` followed by `HSET`, `RPUSH`, or `SADD`; an empty collection
leaves the key deleted. Repeating the same replacement is idempotent and list
order is preserved.

## Delivery guarantees and limits

Redis output is native streaming-only and automatically makes the overall job
streaming. The sink flushes at a checkpoint barrier, but Redis mutations happen
before checkpoint durability and are not a Klein two-phase commit. Delivery is
therefore **at-least-once**. Deterministic replacement makes an identical replay
idempotent per key, but concurrent writers, non-deterministic values, and
cross-key ordering can still change the result.

Redis has no built-in Table DDL factory. Use the DataStream API or implement a
[custom TableFactory](custom.md). Connector metrics and retry logs are described
in [Observability](../observability.md); API types are listed in the
{doc}`Redis API reference <../api/redis>`.
