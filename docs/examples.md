---
myst:
  html_meta:
    description: "Runnable Klein for Ray examples for bounded pipelines, streaming state, SQL, and event time."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-examples)=
# Examples

The examples use public `ray.klein` APIs and run from the project root after you install the package.

## Run the quick start

The quick start creates a bounded pipeline and returns its rows through interactive mode:

```{literalinclude} ../examples/quick_start.py
:language: python
:caption: examples/quick_start.py
```

Run it with:

```bash
python examples/quick_start.py
```

## Explore feature examples

The guides include focused examples next to the behavior they explain:

- [Ray Data interoperability](ray-data-interop.md) shows native Dataset transforms and multi-stream dependencies.
- [Connector catalog](connectors/index.md) includes runnable setup examples for
  Kafka, filesystems, Redis, collections, console, custom connectors, and Ray
  Serve.
- [SQL and Table connectors](sql.md) shows caller-scope tables, temporary views, joins, aggregates, and connector DDL.
- [Managed state](ray-native-state.md) shows a keyed running total and event-time windows.
- [Event time](event-time.md) shows bounded-out-of-orderness watermarks and idle-input detection.

Each example states whether it requires a bounded source, a streaming source, or an external service.
