.. SPDX-License-Identifier: Apache-2.0

.. _sink-api:

Sink API
========

.. currentmodule:: ray.klein

.. autosummary::
   :nosignatures:

   SinkFunction
   SinkCommittable
   SinkCommittableCombiner
   StreamSink
   TwoPhaseCommitSinkFunction

.. autoclass:: StreamSink
   :members: run, wait, result, explain

.. autoclass:: SinkFunction
   :members:

.. autoclass:: SinkCommittable
   :members:

.. autoclass:: SinkCommittableCombiner
   :members:

.. autoclass:: TwoPhaseCommitSinkFunction
   :members:
