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

The quick start creates a bounded pipeline, attaches a collection sink, and
retrieves its rows from the completed job handle:

```{literalinclude} ../examples/quick_start.py
:language: python
:caption: examples/quick_start.py
```

Run it with:

```bash
python examples/quick_start.py
```

## Run batch SQL

```{literalinclude} ../examples/sql_batch.py
:language: python
:caption: examples/sql_batch.py
```

```bash
python examples/sql_batch.py
```

## Run managed state on a finite streaming source

```{literalinclude} ../examples/stateful_streaming.py
:language: python
:caption: examples/stateful_streaming.py
```

```bash
python examples/stateful_streaming.py
```

This example explicitly selects streaming because managed keyed state has no
Ray Data batch lowering. The finite collection source reaches end-of-data, the
job completes, and ``JobHandle.get()`` returns the collected rows.

## Explore feature examples

The guides include focused examples next to the behavior they explain. Code
containing placeholders or external URIs is illustrative; the standalone files
under `examples/` are the smoke-tested local examples.

- [Production streaming walkthrough](production-streaming.md) provides one
  complete Kafka, watermark, window, checkpoint, file-sink, CLI, and restore
  path and states all external prerequisites.
- [Ray Data interoperability](ray-data-interop.md) shows native Dataset transforms and multi-stream dependencies.
- [Connector catalog](connectors/index.md) includes runnable setup examples for
  Kafka, filesystems, Redis, collections, console, custom connectors, and Ray
  Serve.
- [SQL and Table connectors](sql.md) shows caller-scope tables, temporary views, joins, aggregates, and connector DDL.
- [Managed state](ray-native-state.md) shows a keyed running total and event-time windows.
- [Event time](event-time.md) shows bounded-out-of-orderness watermarks and idle-input detection.

Connector and production examples state whether they require a bounded source,
a streaming source, or an external service.
