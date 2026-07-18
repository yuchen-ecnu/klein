# SPDX-License-Identifier: Apache-2.0
"""Unit tests for committer notify dispatch (sync + async fire-and-reap) and the
coordinator's idempotent per-committer ack.

No Ray: the coordinator handle is a mock that records notify calls, and
``klein.get`` is monkeypatched so the reap/retry control flow can be driven
deterministically.
"""

import unittest

from ray.klein.runtime.coordinator import checkpoint_strategy as ss_mod
from ray.klein.runtime.coordinator.checkpoint import Checkpoint, CheckpointStatus
from ray.klein.runtime.coordinator.checkpoint_strategy import _CoordinatorClient
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId


class IdempotentAckTest(unittest.TestCase):
    def test_duplicate_committer_counted_once(self):
        source = ExecutionVertexId(1, 0)
        c0 = ExecutionVertexId(2, 0)
        c1 = ExecutionVertexId(2, 1)
        checkpoint = Checkpoint(
            barrier_id=1,
            required_acknowledgements=2,
            trigger_sources=(source,),
        )
        checkpoint.mark_in_progress()
        checkpoint.acknowledge(committer=c0)
        checkpoint.acknowledge(committer=c0)  # duplicate (e.g. retried notify)
        self.assertEqual(checkpoint.acknowledgements, 1)
        self.assertEqual(checkpoint.status, CheckpointStatus.IN_PROGRESS)
        checkpoint.acknowledge(committer=c1)
        self.assertEqual(checkpoint.acknowledgements, 2)
        # Target reached -> NOTIFYING, ready for the coordinator to finalize.
        self.assertEqual(checkpoint.status, CheckpointStatus.NOTIFYING)


class _Ref:
    """Stands in for an ObjectRef; carries the (barrier_id) it notified."""

    def __init__(self, barrier_id):
        self.barrier_id = barrier_id


class _MockCoordinator:
    def __init__(self):
        self.calls = []  # every notify_checkpoint_aligned invocation

    def notify_checkpoint_aligned(self, barrier_id, vertex_id):
        self.calls.append((barrier_id, vertex_id))
        return _Ref(barrier_id)


class AsyncNotifyTest(unittest.TestCase):
    def setUp(self):
        self.coord = _MockCoordinator()
        # Control klein.get: refs in self.ready resolve, others raise (not done).
        self.ready = set()
        self._orig_get = ss_mod.klein.get

        def fake_get(obj, timeout=None):
            if isinstance(obj, _Ref):
                if obj.barrier_id in self.ready:
                    return True
                raise TimeoutError("not ready")
            return self._orig_get(obj, timeout=timeout)

        ss_mod.klein.get = fake_get

    def tearDown(self):
        ss_mod.klein.get = self._orig_get

    def test_sync_mode_blocks_each_barrier(self):
        self.ready.add(1)  # irrelevant in sync mode, but harmless
        client = _CoordinatorClient(self.coord, "c0", async_notify=False)
        client.notify_complete(1)
        # Synchronous: exactly one call, nothing pending.
        self.assertEqual(self.coord.calls, [(1, "c0")])
        self.assertEqual(client._pending_notifies, {})

    def test_async_fires_without_blocking(self):
        client = _CoordinatorClient(self.coord, "c0", async_notify=True)
        client.notify_complete(1)  # not ready yet -> stays pending
        self.assertEqual(self.coord.calls, [(1, "c0")])
        self.assertIn(1, client._pending_notifies)

    def test_async_reaps_completed_on_next_notify(self):
        client = _CoordinatorClient(self.coord, "c0", async_notify=True)
        client.notify_complete(1)
        self.ready.add(1)  # barrier 1's ack now lands
        client.notify_complete(2)  # reaps 1 (done), fires 2
        self.assertNotIn(1, client._pending_notifies)
        self.assertIn(2, client._pending_notifies)
        # barrier 1 fired exactly once (reaped, not re-fired).
        self.assertEqual(self.coord.calls.count((1, "c0")), 1)

    def test_async_refires_stragglers(self):
        client = _CoordinatorClient(self.coord, "c0", async_notify=True)
        client.notify_complete(1)  # not ready
        client.notify_complete(2)  # reap attempt on 1 fails -> re-fire 1, fire 2
        # barrier 1 was fired twice (initial + re-fire); idempotent ack dedups it.
        self.assertEqual(self.coord.calls.count((1, "c0")), 2)
        self.assertIn(1, client._pending_notifies)
        self.assertIn(2, client._pending_notifies)

    def test_flush_pending_blocks_until_all_acked(self):
        client = _CoordinatorClient(self.coord, "c0", async_notify=True)
        client.notify_complete(1)
        client.notify_complete(2)
        # Make both resolve, then flush.
        self.ready.update({1, 2})
        client.flush_pending()
        self.assertEqual(client._pending_notifies, {})

    def test_flush_pending_noop_in_sync_mode(self):
        self.ready.add(1)  # sync mode blocks on the ack, so let it resolve
        client = _CoordinatorClient(self.coord, "c0", async_notify=False)
        client.notify_complete(1)
        client.flush_pending()  # nothing pending -> returns immediately
        self.assertEqual(client._pending_notifies, {})
