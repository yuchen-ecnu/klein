.. SPDX-License-Identifier: Apache-2.0

Top-level namespace and stability
=================================

Application code normally imports both ``ray`` and ``ray.klein`` and then
accesses ``ray.klein``. The following categories are documented application
contracts:

* graph construction and execution: :class:`~ray.klein.DataStream`,
  ``from_items``, ``from_values``, ``source``, ``execute``, ``explain``, and
  ``sql``;
* jobs and runtime context: :class:`~ray.klein.JobHandle`,
  :class:`~ray.klein.JobStatus`, :class:`~ray.klein.RuntimeContext`, and
  :class:`~ray.klein.RuntimeInfo`;
* stateful streaming: :class:`~ray.klein.KeyedStream`,
  :class:`~ray.klein.KeyedProcessFunction`, window assigners, and watermark
  strategies, plus :class:`~ray.klein.api.MissingDataStrategy` for the
  composite map-reduce preprocessing policy;
* connectors and tables: documented read/write methods, SQL session and table
  contracts, and custom source/sink interfaces;
* operations: ``configure_logging``, ``list_job_snapshots``,
  ``get_job_snapshot``, ``rescale_operator``, and ``cancel_job``.

Dynamic Ray Data names
----------------------

Public factories discovered from the installed compatible ``ray.data`` module,
such as ``ray.klein.read_parquet``, are created lazily and are not enumerated in
``ray.klein.__all__``. Their signatures and documentation come from that Ray
version. Use ``dir(ray.klein)`` and ``stream.data.available`` when an
application must support more than one compatible Ray patch.

Runtime bridge exports
----------------------

The standalone distribution currently exposes ``get``, ``aget``, ``kill``,
``exit_actor``, ``get_actor_by_name``, ``get_actor_status``,
``kill_actor_by_name``, ``register_debug_actor``, and ``is_debug_mode`` at the
top level because the bundled CLI and runtime share the lightweight Ray/debug
adapter. These names originate in ``ray.klein._internal`` and are **not** an
application compatibility promise. Do not use them to manage arbitrary Ray
actors or build user dataflows.

An exported name is not a substitute for the documented API boundary. New
application code should use :class:`JobHandle`, the JSON-safe observability
API, and Ray's own public APIs instead of these bridge helpers.

Import guidance
---------------

Use short imports for ordinary graph code and domain packages for specialized
contracts:

.. code-block:: python

   import ray
   from ray.klein import DataStream, JobHandle
   from ray.klein.api import RuntimeContext, SinkFunction, SourceFunction
   from ray.klein.config import ExecutionOptions, RuntimeExecutionMode
   from ray.klein.state import StateTTLConfig, ValueStateDescriptor

The public :class:`~ray.klein.StreamSink` type is useful for annotations in
advanced selective-execution code; ordinary pipelines do not need to import or
pass sink tokens.

``ray.klein.runtime`` and ``ray.klein._internal`` are implementation packages.
The documented partitioner types are the narrow exception used by
``DataStream.partition_by``.

See :doc:`../api-stability` for the alpha compatibility categories,
deprecation policy, and operational-contract rules.
