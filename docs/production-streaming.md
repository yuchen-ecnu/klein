---
myst:
  html_meta:
    description: "Build, run, observe, stop, and restore a production-shaped Kafka streaming pipeline with Klein for Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Production streaming walkthrough

This tutorial builds a continuous Kafka pipeline with event-time windows,
managed state, durable checkpoints, and checkpoint-transactional JSON output.
It is production-shaped but deliberately small enough to adapt.

## Prerequisites

- a Ray cluster reachable from the submission environment;
- Kafka brokers and an `orders` topic;
- shared object storage reachable by every Ray worker;
- `ray-klein[kafka]` installed on the driver and workers;
- credentials supplied through workload identity or the platform, not source
  code.

Input values are UTF-8 JSON objects:

```json
{"order_id":"o-1","customer_id":"c-7","amount":12.5,"event_time_ms":1760000000000}
```

## Build the pipeline

Save this as `orders_pipeline.py` and replace the broker and storage URIs:

```python
from __future__ import annotations

import json
from datetime import timedelta

import ray
from ray.klein import TumblingWindow, WatermarkStrategy


def decode_order(record: dict) -> dict:
    if record["value"] is None:
        raise ValueError("Kafka order value cannot be null")
    order = json.loads(record["value"].decode("utf-8"))
    return {
        "order_id": str(order["order_id"]),
        "customer_id": str(order["customer_id"]),
        "amount": float(order["amount"]),
        "event_time_ms": int(order["event_time_ms"]),
    }


def add_amount(left: dict, right: dict) -> dict:
    return {
        "customer_id": left["customer_id"],
        "amount": left["amount"] + right["amount"],
        "event_time_ms": max(left["event_time_ms"], right["event_time_ms"]),
    }


def build_pipeline() -> None:
    ray.klein.configure(
        {
            "execution.runtime.mode": "streaming",
            "execution.checkpointing.dir": "s3://platform/klein-checkpoints",
            "execution.checkpointing.num-retained": 3,
            "execution.checkpointing.trigger.interval-duration": "30s",
            "execution.checkpointing.trigger.interval-records": 10_000,
            "state.backend.type": "rocksdb",
            "state.backend.local-dir": "/mnt/nvme/klein-state",
            "state.keyed.max-parallelism": 32768,
            "job.namespace": "orders-production",
        }
    )

    raw_orders = ray.klein.read_kafka(
        "orders",
        bootstrap_servers="kafka-1:9092,kafka-2:9092",
        trigger="continuous",
        start_offset="earliest",
        consumer_config={"group.id": "klein-orders-production"},
        concurrency=4,
        partition_discovery_interval_ms=30_000,
        max_batch_size=1_000,
    )

    orders = raw_orders.map(decode_order, name="DecodeOrder")
    timed_orders = orders.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            timedelta(seconds=10),
            lambda row: row["event_time_ms"],
        ).with_idleness(timedelta(minutes=1))
    )

    totals = (
        timed_orders.key_by(lambda row: row["customer_id"])
        .window(
            TumblingWindow(timedelta(minutes=5)),
            timestamp_selector=lambda row: row["event_time_ms"],
            allowed_lateness=timedelta(seconds=30),
            state_ttl=timedelta(days=1),
        )
        .reduce(add_amount, concurrency=4, name="FiveMinuteCustomerTotal")
    )

    totals.write_json(
        "s3://platform/klein-output/orders-five-minute/",
        filename_prefix="customer-total",
        max_rows_per_file=100_000,
        rollover_interval=timedelta(minutes=15),
        concurrency=4,
    )


def main() -> None:
    ray.init(address="auto")
    build_pipeline()
    print(ray.klein.explain("orders"))
    handle = ray.klein.execute("orders")
    print(f"namespace={handle.namespace}")
    try:
        handle.wait()
    except KeyboardInterrupt:
        handle.cancel(timeout=60)


if __name__ == "__main__":
    main()
```

The window emits after the watermark passes the five-minute window end plus 30
seconds of allowed lateness. A one-minute idle timeout prevents an empty Kafka
partition from blocking every window. `state_ttl` is a safety bound, not the
normal window cleanup mechanism.

## Inspect before submission

Run the file from an environment attached to the target cluster:

```bash
python orders_pipeline.py
```

Review the printed plan for:

- four Kafka source subtasks and useful Kafka partitions;
- a key partition before `FiveMinuteCustomerTotal`;
- the expected stateful window operator and four sink subtasks;
- no accidental Ray Data-only node in the streaming graph;
- CPU/GPU requests the cluster can schedule.

The process blocks in `handle.wait()`. A production Ray Job submission may exit
after recording `handle.namespace`; the detached job continues.

## Observe and stop

From another process with access to the same cluster:

```bash
ray-klein status orders-production
ray-klein attach orders-production
ray-klein stop orders-production
```

Watch source lag, watermark lag, late records, state size, backpressure, and
checkpoint completion. Final JSON parts appear only after the checkpoint that
contains their committables is durable. Hidden `.klein-staging` objects are not
completed output.

Ctrl+C in `attach` only detaches. `stop` requests cancellation and does not
create a special savepoint. Before a planned stop, record a recent completed
checkpoint URI if the job must be restored.

## Restore after cluster loss

Find the latest completed directory, for example:

```text
s3://platform/klein-checkpoints/orders-production/chk-42
```

Add it to the same graph configuration before `execute()`:

```python
ray.klein.configure({
    "execution.savepoint.path": (
        "s3://platform/klein-checkpoints/orders-production/chk-42"
    )
})
```

Keep the graph, operator names, state descriptors, serializers, and
`state.keyed.max-parallelism` compatible. Follow
[Restore and rescale a job](checkpoint-recovery.md) for a full checklist.

## Adapt the example

- Replace JSON output with Kafka or SQL only after designing idempotency for
  their at-least-once streaming effects.
- Increase concurrency only after measuring source partitions, CPU, state
  distribution, sink capacity, and checkpoint duration.
- Use a `KeyedProcessFunction` instead of a window when the job needs custom
  state and timers.
- Use Canal JSON input when Kafka values contain MySQL change events and the
  downstream sink understands changelog row kinds.

See [Delivery guarantees](delivery-semantics.md),
[Performance tuning](performance-tuning.md), and
[Troubleshooting](troubleshooting.md) before production rollout.
