.. SPDX-License-Identifier: Apache-2.0

Job lifecycle API
=================

Submitting a single ``Pipeline`` terminal or executing a ``StatementSet``
returns a :class:`JobHandle`. Bounded jobs may already be complete when the
handle is returned; streaming jobs remain live until the source finishes, the
job fails, or the caller cancels it. Application code should program against
``JobHandle`` rather than its concrete implementation.

.. currentmodule:: ray.klein

.. autoclass:: JobHandle
   :members:

.. autoclass:: JobStatus
   :members:

Typical control flow
--------------------

.. code-block:: python

   import ray.klein as klein

   pipeline = klein.pipeline({"execution.runtime.mode": "streaming"}, name="orders")
   stream = pipeline.from_values({"order_id": 1})
   handle = stream.show()
   print(handle.namespace)
   print(handle.status)
   handle.wait()

``wait()`` blocks until a terminal state and is the normal choice for jobs with
one or more side-effect sinks. ``result()`` is the preferred spelling for one
result-producing terminal; ``get()`` remains as a compatibility alias.
``take()``, ``take_all()``, or ``collect()`` must be executed alone.
``take(limit)`` truncates, while ``take_all(limit=...)`` and
``collect(limit=...)`` raise if the complete result exceeds the bound.
``cancel(timeout=...)`` requests a cooperative stop and returns whether
cancellation completed inside the time budget. A live handle's ``namespace``
is the Ray namespace accepted by the ``ray-klein status``, ``attach``, and
``stop`` commands.
