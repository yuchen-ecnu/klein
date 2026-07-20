---
myst:
  html_meta:
    description: "Klein for Ray documentation for building stateful batch and streaming dataflows on Ray."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein)=
# Klein for Ray: Stateful stream processing on Ray

Klein for Ray is a stateful stream processing library built on Ray. Use the `DataStream` API to run bounded Ray Data inputs and long-running streaming inputs with event time, managed keyed state, checkpoint recovery, and SQL and Table APIs.

:::{warning}
Klein for Ray is independent alpha software. It is not affiliated with,
endorsed by, or maintained by the Ray project.
:::

```{toctree}
:hidden:
:maxdepth: 2
:caption: Start

installation
getting-started
examples
production-streaming
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Learn

key-concepts
architecture
glossary
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Features

features
ray-data-interop
ray-native-state
event-time
sql
delivery-semantics
operator-rescaling
driver-fault-tolerance
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Develop

user-guides
datastream-programming-guide
job-lifecycle
development
connectors/index
operator-compatibility
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Deploy and operate

production-readiness
deployment
security
checkpoint-storage
checkpoint-recovery
observability
cli-reference
performance-tuning
troubleshooting
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Reference

configuration
configuration-reference
limitations
compatibility
upgrading
api-stability
faq
api/api
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Internals and contribute

local_debug
package-structure
testing
private-api-inventory
releasing
```

## When should you use Klein?

Use Klein when a Ray application needs long-running, record-oriented processing and state that survives task failures or parallelism changes. Klein provides event-time windows, interval joins, state time-to-live (TTL), durable checkpoints, and idle-input-aware watermarks.

Use [Ray Data](https://docs.ray.io/en/latest/data/data.html) directly when a
workload is bounded, its operations are supported by Ray Data, and it doesn't
need streaming state or event-time progress. Klein delegates compatible
bounded execution to Ray Data instead of implementing another batch engine.

## Install Klein

Klein for Ray targets Python 3.10 through 3.12 and Ray 2.56.x
(`ray[data]>=2.56.1,<2.57`). Install the Alpha release from PyPI:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "ray-klein==0.1.0a1"
```

See the complete [installation guide](installation.md) for optional extras,
cluster environment checks, verification, upgrades, and troubleshooting. Read
[Compatibility](compatibility.md) before changing the Ray dependency range.

## Learn Klein

::::{grid} 1 2 2 2
:gutter: 1

:::{grid-item-card} Getting started
:link: getting-started
:link-type: doc

Build and run your first bounded pipeline, then learn how to submit a streaming job.
:::

:::{grid-item-card} Key concepts
:link: key-concepts
:link-type: doc

Learn how Klein represents a dataflow, chooses an execution mode, tracks event time, and recovers state.
:::

:::{grid-item-card} Feature highlights
:link: features
:link-type: doc

Compare hybrid execution, state, event time, continuous SQL, checkpoint-aware
outputs, live rescaling, detached jobs, and Ray Serve integration.
:::

:::{grid-item-card} Architecture
:link: architecture
:link-type: doc

Follow a graph from public APIs through planning, Ray Data or streaming actors,
ordered transport, checkpoints, and recovery.
:::

:::{grid-item-card} User guides
:link: user-guides
:link-type: doc

Configure jobs, use Ray Data and SQL, manage state, store checkpoints, and operate pipelines.
:::

:::{grid-item-card} DataStream programming
:link: datastream-programming-guide
:link-type: doc

Choose row or batch functions, manage callable lifecycles, preserve ordering,
and design serializable, observable user code.
:::

:::{grid-item-card} Production walkthrough
:link: production-streaming
:link-type: doc

Build, observe, stop, and restore a Kafka pipeline with event-time state and checkpoint-transactional output.
:::

:::{grid-item-card} Production readiness
:link: production-readiness
:link-type: doc

Verify compatibility, recovery, delivery, capacity, observability, security,
and rollback before a launch.
:::

:::{grid-item-card} Connector catalog
:link: connectors/index
:link-type: doc

Choose inputs and outputs, then review every option, default, schema, execution
mode, and delivery guarantee.
:::

:::{grid-item-card} API reference
:link: api/api
:link-type: doc

Find public classes, functions, methods, and configuration options.
:::

:::{grid-item-card} Limits and stability
:link: limitations
:link-type: doc

Review unsupported combinations, non-goals, API stability, and alpha upgrade
boundaries before committing to an architecture.
:::
::::

## Explore features

::::{grid} 1 2 2 2
:gutter: 1

:::{grid-item-card} Hybrid Ray execution
:link: ray-data-interop
:link-type: doc

Use one lazy graph with Ray Data lowering for compatible bounded work and
native Ray Core operators for continuous or streaming-only work.
:::

:::{grid-item-card} Managed state and event time
:link: ray-native-state
:link-type: doc

Use keyed state, TTL, timers, key groups, checkpoint restore, and idle-aware
watermarks; continue into the event-time guide for windows and joins.
:::

:::{grid-item-card} Continuous SQL and tables
:link: sql
:link-type: doc

Run bounded or continuous SQL, work with explicit changelog rows, and define
connector tables with DDL.
:::

:::{grid-item-card} Checkpoint-aware output
:link: delivery-semantics
:link-type: doc

Understand how source progress, state, replay, and transactional sink
publication compose into an end-to-end guarantee.
:::

:::{grid-item-card} Live operator rescaling
:link: operator-rescaling
:link-type: doc

Resize a supported running streaming operator through a checkpoint-coordinated
topology change.
:::

:::{grid-item-card} Driver-independent jobs
:link: driver-fault-tolerance
:link-type: doc

Keep detached streaming jobs alive after the submitting driver exits, then
reattach, observe, cancel, or recover them explicitly.
:::

:::{grid-item-card} Ray Serve execution regions
:link: connectors/ray-serve
:link-type: doc

Move an eligible transform chain behind an independently deployed Ray Serve
endpoint and review its retry and checkpoint boundary.
:::

:::{grid-item-card} Feature boundaries
:link: limitations
:link-type: doc

Check execution-mode, connector, recovery, scaling, and alpha-compatibility
limits before choosing an architecture.
:::
::::

## How does Klein use Ray?

In `auto` mode, Klein compiles a fully batch-lowerable bounded dataflow to Ray
Data operations when `udf.ignore-exception=false`. An unbounded source, a
streaming-only vertex, or the record-level ignore-exception policy selects
long-lived operators on Ray Core. Those operators store records in ordered
micro-batches. When enabled, Ray's Object Store can cache sufficiently large
immutable checkpoint fragments, while external storage provides the durable
recovery boundary.

Klein's distribution contributes the `ray.klein` namespace package. It doesn't install or replace Ray's `ray/__init__.py`, which keeps the source tree compatible with a future move into the Ray repository.

## Get help and contribute

Start with the [FAQ](faq.md) and [troubleshooting guide](troubleshooting.md).
For project support channels and the information to include, see the
[support policy](https://github.com/yuchen-ecnu/klein/blob/main/SUPPORT.md).
Read the [contribution guide](https://github.com/yuchen-ecnu/klein/blob/main/CONTRIBUTING.md)
before changing public APIs or runtime behavior. Use the process in the
[security policy](https://github.com/yuchen-ecnu/klein/blob/main/SECURITY.md)
to report a vulnerability.
