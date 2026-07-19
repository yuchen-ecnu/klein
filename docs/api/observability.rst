.. SPDX-License-Identifier: Apache-2.0

Observability and control API
=============================

The state API returns JSON-safe dictionaries intended for automation and
dashboard adapters. Call ``ray.init(address="auto")`` before querying a remote
cluster.

.. currentmodule:: ray.klein

.. autosummary::
   :nosignatures:

   list_job_snapshots
   get_job_snapshot
   cancel_job
   configure_logging

``list_job_snapshots()`` returns current jobs plus retained terminal history.
``get_job_snapshot(job_id)`` returns one snapshot or ``None``.
``cancel_job(job_id, timeout=60)`` addresses a published job by job ID, while
the CLI ``stop`` command addresses its Ray namespace.

Snapshot keys
-------------

Consumers must tolerate additional keys in future versions. Current snapshots
contain job identity and status, namespace, operator topology and task status,
throughput and backpressure summaries, checkpoint history, redacted
configuration, failure details, and ``dashboard_stale`` when the state actor
returns its last good cached value.

The API does not make terminal history durable. Checkpoint storage and state
snapshots have separate lifecycles; see :doc:`../observability`.

