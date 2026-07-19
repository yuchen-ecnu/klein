.. SPDX-License-Identifier: Apache-2.0

.. _state-api:

State and checkpoint API
========================

.. currentmodule:: ray.klein.state

.. autosummary::
   :nosignatures:

   CheckpointFileScope
   CheckpointFileSystem
   CheckpointLayout
   CheckpointStore
   FileSystemCheckpointStore
   KeyedStateContext
   KeyGroupRange
   ListState
   ListStateDescriptor
   ManagedStateBackend
   MapState
   MapStateDescriptor
   MemoryStateBackend
   ObjectStoreSnapshotCache
   ObjectStoreStateBackend
   RocksDBStateBackend
   SourceCheckpointEntry
   StateCheckpointEntry
   StateCheckpointManifest
   StateHandle
   StatePartition
   StateSnapshot
   StateSnapshotReference
   StateConflictError
   StateTTLConfig
   StateTTLUpdateType
   StateVisibility
   TimerDomain
   TimerEvent
   TimerService
   ValueState
   ValueStateDescriptor

Keyed state handles
-------------------

.. autoclass:: KeyedStateContext
   :members:

.. autoclass:: ValueState
   :members:

.. autoclass:: ListState
   :members:

.. autoclass:: MapState
   :members:

.. autoclass:: ValueStateDescriptor
   :members:

.. autoclass:: ListStateDescriptor
   :members:

.. autoclass:: MapStateDescriptor
   :members:

TTL and timers
--------------

.. autoclass:: StateTTLConfig
   :members:

.. autoclass:: TimerService
   :members:

.. autoclass:: TimerEvent
   :members:

Checkpoint and backend classes are advanced extension contracts. Ordinary
applications select a backend and checkpoint URI through configuration and use
the keyed handles above rather than constructing checkpoint stores directly.
