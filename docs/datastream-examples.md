---
myst:
  html_meta:
    description: "Runnable Python DataStream API examples for bounded pipelines, managed state, and event time."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-datastream-examples)=
# DataStream API examples

These examples build jobs with the Python DataStream API. Install Klein using
the [DataStream API quick start](getting-started.md#install-klein) and run the
scripts from the project root. Both standalone examples below use in-memory
input and need no external data service. For SQL queries and table DDL, see
the separate [SQL examples](sql/examples.md).

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
job completes, and ``JobHandle.result()`` returns the collected rows.

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
- [Managed state](ray-native-state.md) shows a keyed running total and event-time windows.
- [Event time](event-time.md) shows bounded-out-of-orderness watermarks and idle-input detection.

Connector and production examples state whether they require a bounded source,
a streaming source, or an external service.
