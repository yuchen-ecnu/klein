---
myst:
  html_meta:
    description: "Read continuous Apache RocketMQ topics with Klein for Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-rocketmq-connector)=
# RocketMQ

Klein reads Apache RocketMQ topics as unbounded `DataStream` inputs through the
official remoting-protocol Python client. The connector supports clustering and
broadcasting consumers, Tag expressions, orderly callbacks, ACL credentials,
SSL, client consume threads, and Ray source parallelism.

## Install

```bash
python -m pip install "ray-klein[rocketmq]"
```

`rocketmq-client-python` wraps the native `librocketmq` library. Install a
compatible `librocketmq` on every Ray worker, or use a client wheel that bundles
it. This connector targets remoting-protocol RocketMQ deployments; it is not a
RocketMQ 5 gRPC SimpleConsumer implementation.

## Read a topic

```python
import ray

ray.klein.reset_context()

orders = ray.klein.read_rocketmq(
    "orders",
    name_server_address="nameserver:9876",
    consumer_group="ray-klein-orders",
    tag_expression="paid || refunded",
    concurrency=4,
)

orders.map(
    lambda row: {
        "message_id": row["message_id"],
        "body": row["value"].decode("utf-8"),
    }
).show()

ray.klein.execute("rocketmq-orders").wait()
```

In clustering mode, all source subtasks join the same consumer group and
RocketMQ distributes messages among them. Set `concurrency` to the desired
number of Ray source subtasks. Broadcasting mode requires `concurrency=1`;
otherwise every source subtask would emit its own copy.

## Options

| Option | Default | Meaning |
|---|---|---|
| `topic` | Required | Topic to subscribe to. |
| `name_server_address` | Required | RocketMQ NameServer address, such as `host:9876`; separate multiple addresses with `;`. |
| `consumer_group` | Required | Existing or permitted RocketMQ consumer group. |
| `tag_expression` | `"*"` | RocketMQ Tag subscription expression. |
| `message_model` | `"clustering"` | `"clustering"` or `"broadcasting"`. |
| `orderly` | `False` | Register the orderly client callback. |
| `access_key`, `access_secret` | `None` | ACL credentials; both must be provided together. |
| `channel` | `"KLEIN"` | Credential channel passed to the native client. |
| `ssl_enabled` | `False` | Enable native-client SSL. |
| `ssl_property_file` | `None` | Native-client SSL properties file. |
| `consumer_threads` | `20` | Callback threads inside each RocketMQ client. |
| `max_pending_messages` | `1000` | Per-subtask bounded handoff queue. |
| `poll_timeout_ms` | `1000` | Source queue wait used for idle detection and cancellation. |
| `message_trace_enabled` | `False` | Enable RocketMQ client message tracing. |
| `concurrency` | `None` | Number of Ray source subtasks. |

The stable 2.0.0 client supports the core consumer, filtering, orderly mode,
message models, and ACL settings. SSL and message tracing require a client
build that exposes the corresponding native setters; Klein raises a targeted
startup error if either option is enabled against an older build.

## Record shape

Each message becomes a mapping with these fields:

| Field | Type | Description |
|---|---|---|
| `topic` | `str` | Message topic. |
| `message_id` | `str` | RocketMQ message ID. |
| `key` | `bytes \| None` | Message keys as raw bytes. |
| `value` | `bytes \| None` | Message body as raw bytes. |
| `tags` | `bytes \| None` | Message tags as raw bytes. |
| `queue_id`, `queue_offset` | `int` | Logical queue and queue offset. |
| `commit_log_offset` | `int` | Broker commit-log offset. |
| `born_timestamp`, `store_timestamp` | `int` | Millisecond timestamps from RocketMQ. |
| `reconsume_times` | `int` | Retry count observed by the client. |
| `delay_time_level` | `int` | Delay level. |
| `store_size` | `int` | Stored message size. |
| `prepared_transaction_offset` | `int` | Transaction preparation offset. |

## Delivery and recovery

The Python SDK exposes a PushConsumer callback and does not expose partition
assignment, seek, or checkpoint-controlled offset commits. Klein copies each
native message into a bounded queue and returns `CONSUME_SUCCESS` only after the
source thread has emitted the record and force-flushed its local transport batch
into the next task. If emission fails or the source is cancelled first, it
returns `RECONSUME_LATER`.

RocketMQ consumer-group progress is therefore the source recovery boundary,
not Klein's checkpoint metadata. A full job rollback can lose records that the
broker acknowledged after the last durable Klein checkpoint. Use an idempotent
downstream sink and retain message IDs when deduplication matters. Workloads
that require checkpoint-coordinated source offsets should use the Kafka
connector or a custom pull-based `SourceFunction`.
