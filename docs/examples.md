---
myst:
  html_meta:
    description: "Choose runnable Klein examples for the Python DataStream API or SQL."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-examples)=
# Examples

Choose the examples for the interface you use to author your job:

| Interface | Examples |
|---|---|
| Python DataStream API | [Bounded pipelines, managed state, and feature examples](datastream-examples.md) |
| SQL | [Batch queries, continuous queries, and table connectors](sql/examples.md) |

## Before you run

Install Klein as described in the [installation guide](installation.md),
then run the standalone scripts from the project root. The local examples use
in-memory input and do not require an external data service. Connector and
production walkthroughs list their additional dependencies and services.

Code containing placeholders or external URIs is illustrative. Replace those
values and prepare the required services before running it.

## Run the quick start

See the [DataStream API quick-start example](datastream-examples.md#run-the-quick-start).

## Run batch SQL

See the [SQL examples](sql/examples.md).

## Run managed state on a finite streaming source

See the [managed-state example](datastream-examples.md#run-managed-state-on-a-finite-streaming-source).

## Explore feature examples

See [DataStream API feature examples](datastream-examples.md#explore-feature-examples)
or [SQL examples](sql/examples.md).
