<!-- SPDX-License-Identifier: Apache-2.0 -->

<p align="center">
  <img src="https://raw.githubusercontent.com/yuchen-ecnu/klein/main/docs/_static/klein-logo.png" alt="Klein" width="440">
</p>

# Stateful stream processing on Ray

Klein is a Python library for continuous data processing on Ray. Build
applications with managed keyed state, event-time windows, and checkpoint
recovery. The `DataStream` and SQL APIs also support bounded data pipelines.

![Events flow through Klein's stateful processing into results, with event time, managed state, and checkpoint recovery.](https://raw.githubusercontent.com/yuchen-ecnu/klein/main/docs/_static/klein-overview.png)

[Documentation](https://yuchen-ecnu.github.io/klein/latest/) ·
[中文文档](https://yuchen-ecnu.github.io/klein/latest/zh_CN/) ·
[GitHub](https://github.com/yuchen-ecnu/klein) ·
[Examples](https://yuchen-ecnu.github.io/klein/latest/examples.html)

## Install

Use Python **3.10–3.12** on Linux or macOS. Klein includes a supported Ray
dependency (`ray[data]>=2.56.1,<2.57`).

```bash
python -m pip install ray-klein==0.1.0a2
```

For Kafka integration:

```bash
python -m pip install "ray-klein[kafka]==0.1.0a2"
```

## Quick start

```python
import ray.klein as klein

pipeline = klein.pipeline(name="quick-start")
rows = (
    pipeline.from_items(
        [
            {"name": "Ada", "amount": 4},
            {"name": "Grace", "amount": 7},
        ]
    )
    .map(lambda row: {**row, "amount": row["amount"] * 2})
    .collect()
    .result()
)

print(rows)
# [{'name': 'Ada', 'amount': 8}, {'name': 'Grace', 'amount': 14}]
```

Continue with the [streaming walkthrough](https://yuchen-ecnu.github.io/klein/latest/getting-started.html)
or [SQL guide](https://yuchen-ecnu.github.io/klein/latest/sql/getting-started.html).

## Capabilities

- Managed value, list, and map state, with timers and TTL.
- Event-time windows, watermarks, and keyed transformations.
- Durable checkpoints and recovery for supported streaming jobs.
- SQL and Table APIs for bounded and continuous queries.
- Connectors for Ray Data, Kafka, filesystems, Iceberg, Redis, and more.
- A Dashboard and CLI for inspecting and operating jobs.

Delivery guarantees depend on the selected source and sink. Review the
[delivery semantics](https://yuchen-ecnu.github.io/klein/latest/delivery-semantics.html)
and [known limitations](https://yuchen-ecnu.github.io/klein/latest/limitations.html)
for your application.

Klein is an independent **alpha** project, not affiliated with or maintained
by Ray. Public APIs and checkpoint formats may change before 1.0.
Released under the [Apache License 2.0](https://github.com/yuchen-ecnu/klein/blob/main/LICENSE).
