<!-- SPDX-License-Identifier: Apache-2.0 -->

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/_static/klein-logo-dark.svg">
    <img alt="Klein" src="docs/_static/klein-logo.svg" width="720">
  </picture>
</p>

<p align="center"><strong>Stateful stream processing on Ray.</strong></p>

<p align="center">
  <a href="https://github.com/yuchen-ecnu/klein/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/yuchen-ecnu/klein/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="Status: alpha" src="https://img.shields.io/badge/status-alpha-F59E0B">
  <img alt="Python 3.10–3.12" src="https://img.shields.io/badge/python-3.10%E2%80%933.12-3776AB">
  <img alt="Ray 2.56" src="https://img.shields.io/badge/Ray-2.56-02A0CF">
  <a href="LICENSE"><img alt="Apache License 2.0" src="https://img.shields.io/badge/license-Apache--2.0-3DA639"></a>
</p>

Klein for Ray is a stateful stream-processing library built on Ray. A single
`DataStream` API handles bounded Ray Data inputs and long-running streams, with
event time, managed keyed state, checkpoint recovery, and SQL and Table APIs.

> [!WARNING]
> Klein for Ray is independent alpha software. It is not affiliated with,
> endorsed by, or maintained by the Ray project. The `ray.klein` namespace is
> retained as a technical integration point, not as a claim of official status.

## Why Klein for Ray?

Klein is for Ray applications that need record-oriented processing and state
that survives task failures or parallelism changes. Use
[Ray Data](https://docs.ray.io/en/latest/data/data.html) directly for bounded
data preparation, inference, or training ingest that does not need streaming
state or event-time progress.

### Name and mark

“Klein” comes from the Klein bottle and the idea of taking a Möbius loop one
dimension further. The mark turns the bottle's continuous, self-crossing
surface into a data stream; square waypoints preserve the distributed-node
language shared by Ray's data products without claiming official project
status.

| Capability | What Klein provides |
| --- | --- |
| Unified dataflows | One lazy `DataStream` graph for bounded and continuous sources. |
| Native Ray execution | Ray Data lowers compatible bounded work; Ray Core runs long-lived streaming operators. |
| Event time | Watermarks, idle-input detection, windows, and event-time timers. |
| Managed state | Keyed state, TTL, key groups, rescaling, and checkpoint restore. |
| Recovery | Durable checkpoints, source-position restore, and replay-aware sinks. |
| Relational APIs | Bounded SQL plus dynamic tables and explicit changelog rows for continuous queries. |
| Connectors | Ray Data, collections, Kafka, RocketMQ, filesystems, Iceberg, Redis, console, custom connectors, Canal JSON, and Ray Serve integration. |
| Operations | Structured logs, Ray metrics, checkpoint inspection, CLI attach, and a JSON-safe state API. |

### How Klein fits into Ray

| Ray component | Role in Klein |
| --- | --- |
| Ray Core | Runs distributed operators and coordinates streaming recovery. |
| Ray Data | Executes bounded sources, transformations, shuffles, and sinks. |
| Ray Object Store | Can cache sufficiently large immutable checkpoint fragments to accelerate recovery. |

![Klein for Ray component architecture](docs/_static/architecture-overview.png)

The same lazy graph therefore has two execution paths: bounded-compatible work
lowers to Ray Data, while continuous work expands into long-lived Ray actors.
The streaming control plane stays outside the record path; workers exchange
ordered micro-batches directly and use durable checkpoints for cluster-loss
recovery. See the [architecture guide](docs/architecture.md) for the planning,
data-plane, checkpoint, and extension boundaries behind this overview.

The distribution contributes only the `ray.klein` namespace package. It does
not install `ray/__init__.py` or replace files owned by Ray.

## Installation

Klein for Ray currently targets Python 3.10–3.12 and Ray 2.56.x
(`ray[data]>=2.56.1,<2.57`). Install the Alpha release from PyPI:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "ray-klein==0.1.0a1"
```

Install connector dependencies only when needed:

```bash
python -m pip install "ray-klein[kafka]==0.1.0a1"   # continuous Kafka source/sink
python -m pip install "ray-klein[iceberg]==0.1.0a1" # Iceberg catalog and output
python -m pip install "ray-klein[rocketmq]==0.1.0a1" # continuous RocketMQ source
python -m pip install "ray-klein[redis]==0.1.0a1"   # Redis lookup/sink
python -m pip install "ray-klein[rocksdb]==0.1.0a1" # local RocksDB state backend
python -m pip install "ray-klein[serve]==0.1.0a1"   # Ray Serve bridge
```

For development, clone the repository and install the test, documentation, and
tooling dependencies:

```bash
git clone https://github.com/yuchen-ecnu/klein.git
cd klein
python -m pip install -e ".[dev]"
pre-commit install --hook-type pre-commit --hook-type commit-msg
```

## Quick start

Klein builds the bounded graph lazily. A terminal operation registers its sink,
and `execute("job-name")` runs all registered sinks explicitly:

```python
import ray
import ray.klein

stream = (
    ray.klein.from_items(
        [
            {"name": "Ada", "amount": 4},
            {"name": "Grace", "amount": 7},
        ]
    )
    .map(lambda row: {**row, "amount": row["amount"] * 2})
)
stream.take_all()
rows = ray.klein.execute("quick-start").get()

print(rows)
```

```text
[{'name': 'Ada', 'amount': 8}, {'name': 'Grace', 'amount': 14}]
```

Source construction follows `ray.data`: for example,
`ray.klein.read_parquet(...)` creates a bounded stream using the installed Ray
Data reader. Native methods such as `stream.map(...)` use Klein semantics;
`stream.data.map(...)` delegates to the installed Ray Data implementation.

For continuous execution, see the
[Kafka walkthrough](docs/getting-started.md#submit-a-dataflow) and the complete
[connector catalog](docs/connectors/index.md).

Start the self-contained Klein Dashboard on port 8266. Its React application is
packaged with `ray-klein`; Ray-owned navigation opens the native Dashboard on
port 8265:

```bash
ray-klein dashboard --open \
  --ray-dashboard-url http://127.0.0.1:8265
```

## Documentation

| Start here | What it covers |
| --- | --- |
| [Installation](docs/installation.md) | Supported environments, base and optional extras, cluster consistency, verification, upgrades, and removal. |
| [Getting started](docs/getting-started.md) | Installation, bounded pipelines, streaming submission, and configuration. |
| [Key concepts](docs/key-concepts.md) | Execution modes, state, event time, and recovery. |
| [Architecture](docs/architecture.md) | Planning, batch and streaming runtimes, the ordered data plane, checkpoints, recovery, and extension boundaries. |
| [User guides](docs/user-guides.md) | Production streaming, SQL, state, delivery semantics, recovery, deployment, tuning, and operations. |
| [DataStream programming](docs/datastream-programming-guide.md) | Records, batches, UDF forms, ordering, errors, resources, partitioning, and external side effects. |
| [Job lifecycle](docs/job-lifecycle.md) | Contexts, terminal sinks, planning, submission, job handles, cancellation, namespaces, and cleanup. |
| [Operator compatibility](docs/operator-compatibility.md) | Batch/streaming support, partitioning, state, changelog, and sink behavior. |
| [Production walkthrough](docs/production-streaming.md) | Kafka input through event-time state, checkpoints, file output, CLI operations, and restore. |
| [Connector catalog](docs/connectors/index.md) | Every connector's modes, options, defaults, schemas, and guarantees. |
| [Configuration reference](docs/configuration-reference.md) | Every supported key, type, default, constraint, and environment variable. |
| [API reference](docs/api/api.rst) | Public Python classes, functions, and methods. |
| [Observability](docs/observability.md) | Logs, metrics, checkpoints, CLI attach, and the web Dashboard with operator scaling. |
| [Production readiness](docs/production-readiness.md) | A release checklist for compatibility, recovery, capacity, observability, security, and rollback. |
| [Security](docs/security.md) | Trust boundaries, UDF and pickle risks, control-plane exposure, secrets, and hardening. |
| [Limits, stability, and upgrades](docs/limitations.md) | Unsupported combinations, API guarantees, checkpoint compatibility, upgrade rehearsal, and rollback. |
| [CLI reference](docs/cli-reference.md) | Commands, options, JSON output, exit statuses, terminal behavior, and automation examples. |
| [FAQ](docs/faq.md) | Short answers about installation, execution, state, SQL, connectors, and operations. |
| [Troubleshooting](docs/troubleshooting.md) | Installation, planning, connector, watermark, checkpoint, backpressure, and CLI failures. |

### Feature guides

| Feature | Dedicated documentation |
| --- | --- |
| Feature overview | [Feature highlights](docs/features.md) maps every distinctive capability to its programming, operations, and limitation guides. |
| Hybrid Ray execution and dynamic Ray Data access | [Ray Data interoperation](docs/ray-data-interop.md) |
| Managed keyed state, TTL, timers, and key groups | [Managed state](docs/ray-native-state.md) |
| Watermarks, idleness, windows, and interval joins | [Event time](docs/event-time.md) |
| Bounded and continuous relational processing | [SQL and Table APIs](docs/sql.md) |
| Checkpoint-aware recovery and sink guarantees | [Delivery semantics](docs/delivery-semantics.md) |
| Ray Data autoscaling and live streaming rescaling | [Autoscaling and live operator rescaling](docs/operator-rescaling.md) |
| Detached streaming jobs and driver failure | [Driver fault tolerance](docs/driver-fault-tolerance.md) |
| Independently deployed transform regions | [Ray Serve execution integration](docs/connectors/ray-serve.md) |

Build the documentation locally with:

```bash
make docs          # English at docs/_build/html, Chinese at docs/_build/html/zh_CN
make docs-en       # English only
make docs-zh       # Chinese only
make docs-gettext  # Refresh translation templates
KLEIN_DOCS_OFFLINE=1 make docs  # Skip remote intersphinx inventories
```

## Compatibility and stability

The public API is still evolving and may change before 1.0. Klein pins one
tested Ray minor release because some Ray Data extension points are Developer
APIs; read the [compatibility policy](docs/compatibility.md) before changing the
Ray dependency range.

`state.keyed.max-parallelism` is part of checkpoint compatibility. Do not
change it after a job creates checkpoints that must remain restorable.

## Contributing and support

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup,
test tiers, sign-off, and pull request requirements. For help, use the channels
in [SUPPORT.md](SUPPORT.md); report vulnerabilities privately as described in
[SECURITY.md](SECURITY.md).

Project decisions follow [GOVERNANCE.md](GOVERNANCE.md), releases are recorded
in [CHANGELOG.md](CHANGELOG.md), and research users can cite the metadata in
[CITATION.cff](CITATION.cff).

## License

Klein for Ray is licensed under the [Apache License 2.0](LICENSE). See
[NOTICE](NOTICE), [PROVENANCE.md](PROVENANCE.md), and
[TRADEMARKS.md](TRADEMARKS.md) for attribution and project identity.
