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
   rescale_operator
   cancel_job
   configure_logging

``list_job_snapshots()`` returns current jobs plus retained terminal history.
``get_job_snapshot(job_id)`` returns one snapshot or ``None``.
``rescale_operator(job_id, operator_id, parallelism, timeout=60)`` performs a
local, barrier-aligned parallelism change when the operator supports it and
returns a JSON-safe operation result whose status is ``COMPLETED``, ``NOOP``,
``REJECTED``, or ``FAILED``. The first version does not resize source operators,
transactional sinks, collecting sinks, or lifecycle classes that have not
explicitly opted into concurrent runtime handoff. Parallel and multiple source
operators are supported when rescaling a downstream operator; their
post-commit recovery point uses one shared, direct-input-aligned checkpoint
epoch.
``cancel_job(job_id, timeout=60)`` addresses a published job by job ID, while
the CLI ``stop`` command addresses its Ray namespace.

Snapshot keys
-------------

Consumers must tolerate additional keys in future versions. Current snapshots
contain job identity and status, namespace, operator topology and task status,
throughput and backpressure summaries, checkpoint history, redacted
configuration, failure details, and ``dashboard_stale`` when the state actor
returns its last good cached value.

Operator rows expose interval averages as ``busy_percent`` and
``backpressure_percent``. ``max_busy_percent`` and
``max_backpressure_percent`` contain the hottest subtask values used to color
the Dashboard job graph.

The API does not make terminal history durable. Checkpoint storage and state
snapshots have separate lifecycles; see :doc:`../observability`.
