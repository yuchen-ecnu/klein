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

Register a metric once during worker initialization: use
``open(runtime_context)`` for a lifecycle source or sink, or request the
``runtime_context`` constructor keyword in a callable transform class. Update
the returned handle while processing records. Re-registering one name with a
different kind, description, boundary set, or label set raises ``ValueError``.

See :doc:`metrics` for ``MetricGroup``, metric handles, specifications, and
the runtime-owned metric scopes.
