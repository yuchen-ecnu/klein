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

getting-started
key-concepts
user-guides
examples
api/api
```

## When should you use Klein?

Use Klein when a Ray application needs long-running, record-oriented processing and state that survives task failures or parallelism changes. Klein provides event-time windows, interval joins, state time-to-live (TTL), durable checkpoints, and idle-input-aware watermarks.

Use [Ray Data](https://docs.ray.io/en/latest/data/data.html) directly when a workload is bounded and doesn't need streaming state or event-time progress. Klein delegates bounded execution to Ray Data instead of implementing another batch engine.

## Install Klein

Klein for Ray targets Python 3.10 through 3.12 and published Ray releases from
2.50.1 through 2.51.x. Install the project from this checkout:

```bash
cd klein
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

See [Compatibility](compatibility.md) before changing the Ray dependency range.

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

:::{grid-item-card} User guides
:link: user-guides
:link-type: doc

Configure jobs, use Ray Data and SQL, manage state, store checkpoints, and operate pipelines.
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
::::

## How does Klein use Ray?

Klein compiles a bounded dataflow to Ray Data operations. For streaming dataflows, Klein runs long-lived operators on Ray Core and stores records in ordered micro-batches. Ray's Object Store shares immutable checkpoint fragments, while external storage provides the durable recovery boundary.

Klein's distribution contributes the `ray.klein` namespace package. It doesn't install or replace Ray's `ray/__init__.py`, which keeps the source tree compatible with a future move into the Ray repository.

## Get help and contribute

Read the [contribution guide](https://github.com/yuchen-ecnu/klein/blob/main/CONTRIBUTING.md)
before changing public APIs or runtime behavior. Use the process in the
[security policy](https://github.com/yuchen-ecnu/klein/blob/main/SECURITY.md)
to report a vulnerability.
