.. SPDX-License-Identifier: Apache-2.0

Exceptions and failure handling
===============================

Klein validates graph and connector arguments early with standard Python
exceptions. Distributed runtime failures are retained by the job and surfaced
through :class:`~ray.klein.JobHandle`, the CLI, logs, and state snapshots.

Public exception types
----------------------

.. currentmodule:: ray.klein.exceptions

.. autoclass:: KleinError

.. currentmodule:: ray.klein

.. autoclass:: SQLQueryError

``RayDataAPIError`` is documented with the :doc:`Ray Data adapter <ray_data>`
because it reports dynamic factory and Dataset-method contract violations.

.. currentmodule:: ray.klein.state

.. autoclass:: StateConflictError

Common exception families
-------------------------

``TypeError``
   An argument or user record has the wrong Python type. Correct the graph,
   connector input, state descriptor, or configuration value before retrying.

``ValueError``
   A value is malformed, outside its allowed range, or represents an
   unsupported combination. SQL planning uses the more specific
   :class:`~ray.klein.SQLQueryError`, which is also a ``ValueError``.

``ModuleNotFoundError``
   An optional integration is missing. Install the connector extra in the
   driver and every eligible worker; do not catch this and continue with a
   partially configured graph.

``RayDataAPIError``
   A dynamic ``ray.klein.read_*`` or ``stream.data`` operation is unavailable
   or used in the wrong adapter category for the installed Ray version.

``StateConflictError``
   A managed-state declaration conflicts with an existing state name or
   contract. Treat this as an application compatibility error rather than a
   transient worker failure.

``KleinError``
   A submitted job reached a failure boundary. ``LiveJobHandle.wait()`` raises
   this with the retained job failure detail. The collection-specific
   ``get()`` path does not perform that explicit failed-status check; call
   ``wait()`` or inspect ``status`` when failure propagation matters.

``TimeoutError``
   A client wait, deployment, data-plane drain, or control operation exceeded
   its time budget. A control timeout does not prove the remote operation was
   rolled back; refresh the job snapshot before retrying.

``FileNotFoundError`` or ``OSError``
   A checkpoint object, local dependency, filesystem path, or external
   resource is unavailable. Verify the URI, credentials, shared visibility,
   and checkpoint completion marker before choosing another restore point.

Retry guidance
--------------

Do not retry every exception at the driver. Native streaming workers and the
JobManager already apply the configured restart policy to eligible task
failures. Repeated driver submissions can create another job namespace or
duplicate external effects.

* Correct ``TypeError``, ``ValueError``, ``SQLQueryError``,
  ``RayDataAPIError``, and missing-dependency failures in code or configuration.
* For a live-job ``KleinError``, inspect the retained failure, task status, and
  latest completed checkpoint before resubmission.
* After a client-side timeout from cancellation or rescaling, query status
  before sending another control request.
* Retry an external connector error only when its connector guide defines the
  source-position or sink-idempotency boundary.

See :doc:`../troubleshooting` for symptom-oriented diagnosis and
:doc:`../delivery-semantics` for the duplicate/loss consequences of recovery.
