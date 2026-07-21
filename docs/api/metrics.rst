.. SPDX-License-Identifier: Apache-2.0

Metrics API
===========

User functions receive a :class:`MetricGroup` through their runtime context.
Lifecycle sources and sinks receive that context in ``open()``; a callable
transform class can request a constructor keyword named ``runtime_context``.
Register metrics once during worker initialization and update the returned
task-local handle while processing. Reusing one name with a different kind,
description, histogram boundary set, or label set raises ``ValueError``.

.. currentmodule:: ray.klein.observability.metrics

.. autoclass:: MetricGroup
   :members: counter, gauge, histogram, add_group, all_labels, name, metric_identifier

.. autoclass:: Counter
   :members:

.. autoclass:: Gauge
   :members:

.. autoclass:: Histogram
   :members:

.. autoclass:: MetricKind
   :members:

.. autoclass:: MetricSpec
   :members:

``JobMetricGroup``, ``TaskMetricGroup``, and ``OperatorMetricGroup`` are
runtime-owned scopes. Application code should use the group supplied by the
runtime instead of constructing those classes. ``KleinMetrics`` is the
canonical built-in catalog used by instrumentation and tests.

See :doc:`../observability` for prefixes, labels, built-in metric names,
Prometheus queries, cardinality rules, and interpretation.
