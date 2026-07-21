.. SPDX-License-Identifier: Apache-2.0

Developing a pipeline
=====================

Klein for Ray builds one directed dataflow graph and chooses batch or streaming
execution from its sources. Create sources directly from :mod:`ray.klein`, add
transforms, attach at least one sink, and execute the current graph.

.. code-block:: python

   import ray
   import ray.klein

   stream = ray.klein.from_items([{"value": 1}, {"value": 2}, {"value": 3}])
   stream.map(lambda row: {"value": row["value"] ** 2}).show()
   ray.klein.execute("squares").wait()

Execution modes
---------------

Bounded Ray Data sources such as CSV, Parquet, JSON, and ``from_items`` use
batch execution. Long-running sources such as Kafka and user-defined
``SourceFunction`` implementations use streaming execution. Applications can
override automatic detection with ``ExecutionOptions.MODE``.

Transform functions
-------------------

``map``, ``flat_map``, and ``filter`` operate on record dictionaries.
``map_batches`` accepts a dictionary of columns and is preferred for vectorized
workloads such as model inference. A callable class can request a
``runtime_context`` constructor argument to inspect its task index or register
metrics.

.. code-block:: python

   import numpy as np

   class Normalize:
       def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
           values = batch["value"]
           return {"value": values / np.maximum(values.max(), 1)}

   stream.map_batches(Normalize, batch_size=64)

Source and sink lifecycle
-------------------------

Pass ``SourceFunction`` and ``SinkFunction`` implementations as classes, along
with constructor arguments, so every physical subtask owns one independent
instance. Passing a pre-created lifecycle object is rejected because it can
share mutable state or network connections across subtasks.

For a streaming source, ``cancel()`` is the cooperative stop signal for the
running loop. Keep resource release in ``close()``; the runtime invokes it once
after the loop exits. Sink ``flush()`` publishes buffered writes at aligned
barriers, while ``close()`` performs final resource cleanup.

Resources and partitioning
--------------------------

Sources, transforms, and sinks accept ``num_cpus``, ``num_gpus``, and
``concurrency``. Streaming graphs also support ``round_robin``, ``rescale``,
``adaptive_shuffle``, and ``partition_by``. Keep resource requests explicit for
production graphs and validate them with ``ray.klein.explain`` before
deployment.

Connectors
----------

The core distribution supports Ray Data connectors, collections, Kafka,
filesystems, Redis, console output, and custom sources or sinks. Ray Serve is
an optional execution integration. The dedicated
:doc:`connector catalog <connectors/index>` provides a capability matrix and a
separate page for every connector, including options, defaults, schemas, and
delivery guarantees.

Service discovery and organization-specific storage clients are intentionally
outside the core package. Optional connectors should be separate distributions
that depend on ``ray-klein`` and may expose a Table factory through the
``ray.klein.table_factories`` entry-point group.

See :ref:`klein-context-api`, :ref:`data-stream-api`, and
:ref:`configuration-options-api` for the generated API reference.
