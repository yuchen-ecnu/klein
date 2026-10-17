<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL examples

This complete example creates an input table from Python data, runs a SQL
aggregation, and collects the result. From a source checkout with Klein
installed, run:

```bash
python examples/sql_batch.py
```

```{literalinclude} ../../examples/sql_batch.py
:language: python
:lines: 2-
```

Expected output:

```text
[{'customer': 'Ada', 'total': 11}, {'customer': 'Grace', 'total': 3}]
```

For other query patterns, see:

- [Join tables and bind DataStreams](getting-started.md#query-datastreams-from-python).
- [Register scalar and batched AI functions](functions.md).
- [Observe a streaming aggregation's changelog](streaming.md#run-a-continuous-query).
- [Create connector tables and submit inserts](connectors.md#define-connectors-with-flink-style-table-ddl).

For Python transformations and operator examples, use the
[DataStream API guide](../datastream.md).
