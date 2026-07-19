.. SPDX-License-Identifier: Apache-2.0

Partitioning API
================

Prefer the convenience methods on :class:`ray.klein.DataStream`. Implement a
custom partitioner only when the built-in forward, round-robin, rescale,
broadcast, key, and adaptive strategies do not express the required topology.

.. currentmodule:: ray.klein.runtime.partitioning

.. autosummary::
   :nosignatures:

   Partitioner
   ForwardPartitioner
   RoundRobinPartitioner
   RescalePartitioner
   BroadcastPartitioner
   KeyPartitioner
   AdaptivePartitioner
   SimplePartitioner
   WorkerPoolDispatcher

A custom ``Partitioner`` passed to ``DataStream.partition_by`` must produce an
immutable ``PartitionerSpec`` through ``to_spec()``. The spec, rather than a
driver-owned mutable partitioner instance, is serialized into the execution
graph and reconstructed per worker.

.. autoclass:: Partitioner
   :members:
