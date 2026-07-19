.. SPDX-License-Identifier: Apache-2.0

Job lifecycle API
=================

``execute()`` returns a :class:`JobHandle`. Bounded jobs may already be
complete when the handle is returned; streaming jobs remain live until the
source finishes, the job fails, or the caller cancels it. Application code
should program against ``JobHandle`` rather than its concrete implementation.

.. currentmodule:: ray.klein

.. autoclass:: JobHandle
   :members:

.. autoclass:: JobStatus
   :members:

Typical control flow
--------------------

.. code-block:: python

   import ray

   handle = ray.klein.execute("orders")
   print(handle.namespace)
   print(handle.status)
   handle.wait()

``wait()`` blocks until a terminal state. ``get()`` also waits and then
returns the batch or collection result. ``cancel(timeout=...)`` requests a
cooperative stop and returns whether cancellation completed inside the time
budget. A live handle's ``namespace`` is the Ray namespace accepted by the
``ray-klein status``, ``attach``, and ``stop`` commands.
