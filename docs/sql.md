---
myst:
  html_meta:
    description: "Learn Klein SQL through dedicated guides for queries, functions, streaming, Table DDL, and execution support."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(ray-native-sql-and-table-connectors)=
# Klein SQL overview

Use this guide when writing SQL queries and connector Table DDL. For Python
transformations such as `map()`, `filter()`, and `key_by()`, use the
[DataStream API guide](datastream.md). Both interfaces use a `Pipeline` and can
exchange `DataStream` results.

Start with [SQL getting started](sql/getting-started.md), then follow the topic
links below. The [SQL API reference](api/sql.rst) provides method signatures.

Klein SQL uses one SQLGlot AST with two Ray-native execution backends. Bounded
batch queries lower to lazy Ray Dataset operations. Continuous queries lower
to Klein operators with managed keyed state, checkpoints, key groups, and
changelog output. SQLGlot is the parser, not an execution engine, and Klein
does not embed DuckDB.

## Choose a SQL entry point

[SQL getting started](sql/getting-started.md#choose-a-sql-entry-point) explains query and catalog entry points.

## Query DataStreams from Python

Start with the [self-contained Python query](sql/getting-started.md#query-datastreams-from-python), then run the [complete SQL example](sql/examples.md).

## Use common SQL built-ins

See [built-in functions](sql/functions.md#use-common-sql-built-ins) for text, numeric, date, JSON, and other scalar expressions.

## Process images and PDFs

See [media functions](sql/functions.md#process-images-and-pdfs) for image and PDF processing, dependencies, and limits.

## Register Python scalar functions

See [scalar functions](sql/functions.md#register-python-scalar-functions) for registration and execution semantics.

## Register batched AI functions

See [batched AI functions](sql/functions.md#register-batched-ai-functions) for backend registration, batching, resources, and supported query forms.

## Run a continuous query

See [Streaming SQL](sql/streaming.md) for changelog output, state TTL, and Top-N behavior.

## Define connectors with Flink-style Table DDL

See [Table DDL and connectors](sql/connectors.md) for catalog tables, inserts, and statement sets.

## How does SQL execute?

See [SQL execution and support](sql/execution.md#how-does-sql-execute) for execution backends, the feature matrix, and `DOWNLOAD` behavior.

## Distinguish queries from the database sink

See [queries and the database sink](sql/execution.md#distinguish-queries-from-the-database-sink) for the separate `DataStream.write_sql()` API and its delivery semantics.
