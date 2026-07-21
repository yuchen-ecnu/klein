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
   import ray.klein

   ray.klein.configure({"execution.runtime.mode": "streaming"})
   stream = ray.klein.from_values({"order_id": 1})
   stream.show()
   handle = ray.klein.execute("orders")
   print(handle.namespace)
   print(handle.status)
   handle.wait()

``wait()`` blocks until a terminal state and is the normal choice for jobs with
one or more side-effect sinks. ``get()`` is for one result-producing terminal;
``take()`` or ``take_all()`` must be executed alone. ``cancel(timeout=...)``
requests a cooperative stop and returns whether cancellation completed inside
the time budget. A live handle's ``namespace`` is the Ray namespace accepted by
the ``ray-klein status``, ``attach``, and ``stop`` commands.
