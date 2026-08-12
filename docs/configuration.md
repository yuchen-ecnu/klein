---
myst:
  html_meta:
    description: "Configure Klein with typed options, mappings, strings, global context settings, and environment variables."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Configure Klein

Klein uses lower-case dotted keys with kebab-case compound names. For example,
`execution.runtime.mode` and `pipeline.operator-chaining.enabled`. Underscores
in input keys are normalized to hyphens.

Klein resolves each option from three sources:

| Priority | Source | Example |
| --- | --- | --- |
| 1 (highest) | Explicit pipeline or `Configuration` value | `{"execution.runtime.mode": "streaming"}` |
| 2 | Captured `RAY_KLEIN_*` environment value | `RAY_KLEIN_EXECUTION_RUNTIME_MODE=streaming` |
| 3 | Typed option default | `execution.runtime.mode=auto` |

Klein captures matching environment values when it creates a `Configuration`,
so later environment changes don't alter that configuration. An explicit
`Pipeline` owns its configuration snapshot from construction onward. With the
legacy module-level API, call `ray.klein.configure()` before building the graph.

For every supported key, type, default, constraint, and direct runtime
environment variable, see the [complete configuration reference](configuration-reference.md).

## Set options in Python

Choose the input form that matches how your application receives configuration:

```python
import ray.klein as klein

# Construct the recommended explicit pipeline from a mapping.
pipeline = klein.pipeline(
    {
        "execution.runtime.mode": "streaming",
        "state.backend.type": "rocksdb",
    },
    name="orders",
)

# Separate key=value pairs with commas, semicolons, or whitespace.
pipeline = klein.pipeline(
    "execution.checkpointing.timeout=300; pipeline.operator-chaining.enabled=false",
    name="orders",
)
```

For the implicit deferred pipeline, install the same values before creating a
source:

```python
klein.configure({"execution.runtime.mode": "streaming"})
events = klein.read_kafka(...)
```

Use a JSON object string when values contain dictionaries or lists:

```python
pipeline = klein.pipeline("""{
  "execution.checkpointing.storage-options": {"region": "us-west-2"}
}""", name="orders")
```

When you call `Configuration.set()` with a `ConfigOption`, pass the final typed
Python value. Strings from mappings, `key=value` input, and environment
variables are converted when the runtime reads a typed option:

```python
from datetime import timedelta

from ray.klein.config.configuration import Configuration
from ray.klein.config.event_time_options import EventTimeOptions

config = Configuration({"event-time.idle-input.check-interval": "500ms"})
assert config.get(EventTimeOptions.IDLE_INPUT_CHECK_INTERVAL) == timedelta(milliseconds=500)

config.set(EventTimeOptions.IDLE_INPUT_CHECK_INTERVAL, timedelta(seconds=2))
```

## Set options with environment variables

Every typed option maps dots and hyphens to underscores and adds the `RAY_KLEIN_` prefix:

```bash
export RAY_KLEIN_EXECUTION_RUNTIME_MODE=streaming
export RAY_KLEIN_STATE_BACKEND_TYPE=rocksdb
export RAY_KLEIN_PIPELINE_OPERATOR_CHAINING_ENABLED=false
export RAY_KLEIN_STATE_KEYED_MAX_PARALLELISM=32768
export RAY_KLEIN_EVENT_TIME_IDLE_INPUT_CHECK_INTERVAL=1s
```

Klein converts Boolean values, durations, numbers, mappings, and enumerations with the same converter used by code configuration. Invalid values fail when Klein reads the option instead of silently falling back to a default.

Accepted Boolean strings are `true`, `false`, `1`, `0`, `yes`, `no`, `on`, and
`off`, without case sensitivity. Duration strings combine a number with `ms`,
`s`, `min`, `h`, `d`, or `w`; examples include `250ms`, `30s`, and `1.5h`.
Unquoted numeric values in mappings or `key=value` input are seconds;
environment-variable durations are strings and therefore need a unit. Enum
names and values are case-insensitive. Numeric conversion is lossless: integer
options reject floats and Booleans, float options reject Booleans and
non-finite values, and numeric Boolean input accepts only `0` or `1`. Durations
reject non-finite and out-of-range values.

## Choose a graph owner

Use `klein.pipeline()` for new applications. It creates an isolated graph,
rejects unknown Klein keys by default, and gives directly submitted terminals a
stable default name. The module-level API remains a permissive compatibility
surface with deferred terminal registration.

Construct `KleinContext(configuration)` only when one process needs mutually
isolated builders and the legacy deferred `execute()` contract:

```python
from ray.klein import KleinContext

left_context = KleinContext({"execution.runtime.mode": "batch"})
right_context = KleinContext({"execution.runtime.mode": "streaming"})
```

Don't combine streams from different contexts in one graph.

## Use canonical keys

Configuration keys use a lower-case dotted hierarchy and kebab-case compound names. Use these top-level namespaces:

| Namespace | Purpose |
| --- | --- |
| `execution.*` | Runtime mode, task deployment, restart strategy, and checkpoint coordination. |
| `pipeline.*` | Graph compilation, operator chaining, and batching. |
| `state.*` | State backend, TTL cleanup, key groups, and snapshot caching. |
| `table.*` | Flink-compatible streaming SQL state and planner options. |
| `event-time.*` | Event-time and idle-input coordination. |
| `job.*` | Streaming job deployment, shutdown, health checks, and Ray namespace isolation. |
| `partitioner.*` | Compatibility settings retained for earlier adaptive partitioners. |
| `observability.*` | Dashboard publication, retained job history, and related telemetry. |
| `serve.*` | Ray Serve deployment and embedded HTTP proxy-client settings. |
| `udf.*` | User-function error handling. |

Permissive `Configuration` objects and the legacy APIs accept and retain
unknown keys, which lets applications keep their own metadata beside Klein
settings. Klein doesn't act on those keys. Use the [configuration
reference](configuration-reference.md) to distinguish active Klein options
from application-defined values.

An explicit `Pipeline` uses strict configuration by default. This catches
misspellings at construction time while keeping the module-level API and direct
`KleinContext` construction permissive for compatibility:

```python
import ray.klein as klein

pipeline = klein.pipeline({"execution.runtime.mode": "streaming"})
```

Use `Configuration(..., strict=True)` or `ray.klein.configure(..., strict=True)`
to request the same validation without a `Pipeline`. Set `strict_config=False`
only when a pipeline mapping deliberately contains application metadata.

## Inspect configuration before submission

Build a `Configuration`, inspect or update it with typed options, and then pass
it to the explicit pipeline before constructing the graph:

```python
import ray.klein as klein

from ray.klein import Configuration
from ray.klein.config.execution_options import ExecutionOptions

config = Configuration({"execution.runtime.mode": "streaming"})
mode = config.get(ExecutionOptions.MODE)
print(mode.value)
pipeline = klein.pipeline(config, name="orders")
```

`to_dict()` returns only explicit values. It deliberately doesn't expand
captured environment values or typed defaults:

```python
config = Configuration({"state.backend.type": "memory"})
assert config.to_dict() == {
    "state.backend.type": "memory",
}
pipeline = klein.pipeline(config, name="orders")
```

`unset(key)` removes the explicit value. The next typed `get()` then reveals a
captured environment value, if one exists, or the option default.

Pass a `ConfigOption` to `get()` whenever you want conversion and a typed
default. Passing a plain key string returns only the raw explicit or captured
environment value, or `None` when neither exists.

For deterministic tests or tools, disable process-environment capture or
provide an explicit environment mapping:

```python
from ray.klein.config.configuration import Configuration

without_environment = Configuration(include_environment=False)
controlled = Configuration(
    environment={"RAY_KLEIN_EXECUTION_RUNTIME_MODE": "batch"},
)
copy = Configuration(controlled)
```

Constructing a `Configuration` from another one copies its captured environment
and explicit values, so later mutation of either instance is isolated.

## Configure dashboard publication

Streaming jobs publish read-only snapshots to the Klein state actor by default.
Disable this for a job, or bound the in-memory terminal-job history, with:

```bash
export RAY_KLEIN_OBSERVABILITY_DASHBOARD_ENABLED=true
export RAY_KLEIN_OBSERVABILITY_DASHBOARD_HISTORY_SIZE=100
```

Logging level and format are process-level logging settings rather than job
configuration options. See [Observe Klein jobs](observability.md) for
`RAY_KLEIN_LOG_LEVEL`, `RAY_KLEIN_LOG_FORMAT`, and custom logging YAML.

Input underscores normalize to hyphens, but `to_dict()` and environment-variable mapping always use the canonical key.
