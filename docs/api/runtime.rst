.. SPDX-License-Identifier: Apache-2.0

User-function runtime API
=========================

Callable classes can request a ``runtime_context`` constructor argument. The
context exposes the physical task identity, effective configuration, batching
settings, and a metric group. Keep it on the worker; it is not a serializable
driver-side job handle.

.. currentmodule:: ray.klein

.. autoclass:: RuntimeContext
   :members:

.. autoclass:: StreamRuntimeContext
   :members:

.. autoclass:: RuntimeInfo
   :members:

Function lifecycle
------------------

.. currentmodule:: ray.klein.api.function

.. autoclass:: Function
   :members:

Metrics
-------

.. currentmodule:: ray.klein.observability.metrics

.. autoclass:: MetricGroup
   :members: counter, gauge, histogram, add_group, all_labels, name, metric_identifier

.. autoclass:: Counter
   :members:

.. autoclass:: Gauge
   :members:

.. autoclass:: Histogram
   :members:

Register a metric once in ``open(runtime_context)`` and update the returned
handle while processing records. Re-registering one name with a different
kind, description, boundary set, or label set raises ``ValueError``.

