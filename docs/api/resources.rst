.. SPDX-License-Identifier: Apache-2.0

Resource plan API
=================

``explain()`` returns the JSON representation of a :class:`ResourcePlan`.
Applications normally set resources on sources, transforms, and sinks; the
classes below support offline inspection and validated override files.

.. currentmodule:: ray.klein.api.resource_plan

.. autoclass:: ResourcePlan
   :members:

.. currentmodule:: ray.klein.api.resource_node

.. autoclass:: ResourceNode
   :members:

.. currentmodule:: ray.klein.api.resource_edge

.. autoclass:: ResourceEdge
   :members:

Set ``RAY_KLEIN_RESOURCE_PLAN_PERSIST_PATH`` to write the compiled plan and
``RAY_KLEIN_RESOURCE_PLAN_LOAD_PATH`` to apply a compatible plan during
submission. A loaded plan must contain the same node keys as the compiled
graph; only CPU, GPU, concurrency, batch-size, and asynchronous-buffer fields
are mutable through
:meth:`~ray.klein.api.resource_plan.ResourcePlan.update_node`.

Plan files are deployment inputs, not state snapshots. They contain no source
position or managed state and cannot restore a job.
