# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the Chandy-Lamport barrier aligner.

_BarrierAligner is the per-operator alignment counter that decides when a
checkpoint barrier (or EndOfData) is fully aligned across all upstream subtasks,
and reclaims orphaned partial counts after a coordinator rebuild. It's pure
in-process logic — tested directly with ExecutionVertexId keys, no Ray.
"""

import unittest

from ray.klein.runtime.coordinator.checkpoint_strategy import _BarrierAligner
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import Barrier, DeliveryChannel, EndOfData


def _src(job_vertex_id, index=0):
    return ExecutionVertexId(job_vertex_id, index)


class BarrierAlignerReceiveTest(unittest.TestCase):
    def test_single_upstream_aligns_on_first(self):
        src = _src(1)
        aligner = _BarrierAligner({src: 1})
        self.assertTrue(aligner.receive(Barrier(10, source_id=src)))

    def test_fan_in_aligns_only_when_all_arrive(self):
        # split=3 -> the same barrier id must arrive 3 times before aligning.
        src = _src(1)
        aligner = _BarrierAligner({src: 3})
        self.assertFalse(aligner.receive(Barrier(10, source_id=src)))
        self.assertFalse(aligner.receive(Barrier(10, source_id=src)))
        self.assertTrue(aligner.receive(Barrier(10, source_id=src)))

    def test_unknown_source_treated_as_single(self):
        # Total lookup: a source absent from the split table aligns on count 1
        # (a missing entry can only legitimately mean a single upstream).
        aligner = _BarrierAligner({})
        self.assertTrue(aligner.receive(Barrier(10, source_id=_src(99))))

    def test_distinct_barrier_ids_counted_independently(self):
        src = _src(1)
        aligner = _BarrierAligner({src: 2})
        self.assertFalse(aligner.receive(Barrier(10, source_id=src)))
        # A different id starts its own count, doesn't complete id 10.
        self.assertFalse(aligner.receive(Barrier(11, source_id=src)))
        self.assertTrue(aligner.receive(Barrier(10, source_id=src)))
        self.assertTrue(aligner.receive(Barrier(11, source_id=src)))

    def test_aligned_barrier_clears_inflight(self):
        # Once aligned, the id is removed; a stray late copy starts fresh.
        src = _src(1)
        aligner = _BarrierAligner({src: 2})
        aligner.receive(Barrier(10, source_id=src))
        self.assertTrue(aligner.receive(Barrier(10, source_id=src)))
        # A 3rd copy of an already-aligned barrier begins a new count of 1.
        self.assertFalse(aligner.receive(Barrier(10, source_id=src)))


class BarrierAlignerDirectInputTest(unittest.TestCase):
    @staticmethod
    def _channel(
        sender: ExecutionVertexId,
        edge_index: int,
        topology_epoch: str = "epoch-1",
    ) -> DeliveryChannel:
        return DeliveryChannel(sender, "upstream", edge_index, 0, topology_epoch)

    def test_shared_epoch_ignores_root_source_and_aligns_direct_senders_once(self):
        root_a, root_b = _src(1), _src(2)
        upstream_a, upstream_b = _src(3, 0), _src(3, 1)
        aligner = _BarrierAligner(
            {root_a: 1, root_b: 1},
            (upstream_a, upstream_b),
        )

        self.assertFalse(aligner.receive(Barrier(10, source_id=root_a), upstream_a))
        self.assertTrue(aligner.receive(Barrier(10, source_id=root_b), upstream_b))
        self.assertFalse(aligner.receive(Barrier(10, source_id=root_a), upstream_a))

    def test_duplicate_sender_does_not_complete_an_epoch(self):
        root = _src(1)
        upstream_a, upstream_b = _src(2, 0), _src(2, 1)
        aligner = _BarrierAligner({root: 2}, (upstream_a, upstream_b))

        self.assertFalse(aligner.receive(Barrier(7, source_id=root), upstream_a))
        self.assertFalse(aligner.receive(Barrier(7, source_id=root), upstream_a))
        self.assertTrue(aligner.receive(Barrier(7, source_id=root), upstream_b))

    def test_repeated_sender_counts_unique_delivery_channels(self):
        root = _src(1)
        upstream = _src(2)
        first = self._channel(upstream, 0)
        second = self._channel(upstream, 1)
        aligner = _BarrierAligner({root: 1}, (upstream, upstream))
        barrier = Barrier(7, source_id=root)

        self.assertFalse(aligner.receive(barrier, upstream, first))
        self.assertFalse(aligner.receive(barrier, upstream, first))
        self.assertTrue(aligner.receive(barrier, upstream, second))

    def test_repeated_sender_requires_delivery_channel_identity(self):
        root = _src(1)
        upstream = _src(2)
        aligner = _BarrierAligner({root: 1}, (upstream, upstream))

        with self.assertRaisesRegex(ValueError, "delivery_channel is required"):
            aligner.receive(Barrier(7, source_id=root), upstream)

    def test_repeated_sender_tracks_terminal_lanes_independently(self):
        root = _src(1)
        upstream = _src(2)
        first = self._channel(upstream, 0)
        second = self._channel(upstream, 1)
        aligner = _BarrierAligner({root: 1}, (upstream, upstream))

        self.assertFalse(aligner.receive(EndOfData(7, source_id=root), upstream, first))
        mixed = Barrier(7, source_id=root)
        self.assertTrue(aligner.receive(mixed, upstream, second))
        self.assertIs(type(aligner.barrier_to_forward(mixed)), Barrier)
        self.assertFalse(aligner.last_alignment_is_terminal)

        terminal = EndOfData(8, source_id=root)
        self.assertTrue(aligner.receive(terminal, upstream, second))
        self.assertIs(type(aligner.barrier_to_forward(terminal)), EndOfData)
        self.assertTrue(aligner.last_alignment_is_terminal)

    def test_reconfigure_preserves_terminal_lane_identity_without_early_alignment(self):
        root = _src(1)
        upstream = _src(2)
        first = self._channel(upstream, 0)
        second = self._channel(upstream, 1)
        aligner = _BarrierAligner({root: 1}, (upstream, upstream))

        self.assertFalse(aligner.receive(EndOfData(7, source_id=root), upstream, first))
        self.assertTrue(aligner.receive(Barrier(7, source_id=root), upstream, second))

        # The vertex-only compatibility path still retains the exact terminal
        # DeliveryChannel identity up to the new sender multiplicity.
        aligner.reconfigure_inputs((upstream, upstream))

        # A delayed/new-id barrier from the exact terminal lane is a no-op. It
        # must not satisfy the one remaining active lane after reconfiguration.
        self.assertFalse(aligner.receive(Barrier(8, source_id=root), upstream, first))
        self.assertFalse(aligner.last_alignment_is_terminal)
        terminal = EndOfData(8, source_id=root)
        self.assertTrue(aligner.receive(terminal, upstream, second))
        self.assertTrue(aligner.last_alignment_is_terminal)

    def test_reconfigure_removes_disappeared_terminal_lane_and_tombstones_late_barrier(self):
        root = _src(1)
        removed, survivor = _src(2, 0), _src(2, 1)
        removed_channel = self._channel(removed, 0)
        survivor_channel = self._channel(survivor, 0)
        aligner = _BarrierAligner({root: 1}, (removed, survivor))

        self.assertFalse(aligner.receive(EndOfData(7, source_id=root), removed, removed_channel))
        self.assertTrue(aligner.receive(Barrier(7, source_id=root), survivor, survivor_channel))

        aligner.reconfigure_inputs(
            (survivor,),
            input_channels=(survivor_channel,),
        )

        self.assertEqual(aligner._terminal_inputs, {})
        self.assertFalse(aligner.receive(Barrier(8, source_id=root), removed, removed_channel))
        self.assertTrue(aligner.receive(Barrier(8, source_id=root), survivor, survivor_channel))
        self.assertFalse(aligner.last_alignment_is_terminal)

    def test_reconfigure_new_topology_epoch_does_not_inherit_terminal_state(self):
        root = _src(1)
        upstream = _src(2)
        previous = self._channel(upstream, 0, "epoch-1")
        replacement = self._channel(upstream, 0, "epoch-2")
        aligner = _BarrierAligner({root: 1}, (upstream,))

        self.assertTrue(aligner.receive(EndOfData(7, source_id=root), upstream, previous))
        self.assertTrue(aligner.last_alignment_is_terminal)

        aligner.reconfigure_inputs(
            (upstream,),
            input_channels=(replacement,),
        )

        self.assertEqual(aligner._terminal_inputs, {})
        self.assertFalse(aligner.receive(Barrier(8, source_id=root), upstream, previous))
        self.assertTrue(aligner.receive(Barrier(8, source_id=root), upstream, replacement))
        self.assertFalse(aligner.last_alignment_is_terminal)

    def test_reconfigured_inventory_waits_for_replacement_when_peer_arrives_first(self):
        root = _src(1)
        upstream = _src(2)
        previous_first = self._channel(upstream, 0, "epoch-1")
        previous_second = self._channel(upstream, 1, "epoch-1")
        replacement_first = self._channel(upstream, 0, "epoch-2")
        replacement_second = self._channel(upstream, 1, "epoch-2")
        aligner = _BarrierAligner(
            {root: 1},
            (upstream, upstream),
            (previous_first, previous_second),
        )

        self.assertFalse(aligner.receive(EndOfData(7, source_id=root), upstream, previous_first))
        self.assertTrue(aligner.receive(Barrier(7, source_id=root), upstream, previous_second))
        aligner.reconfigure_inputs(
            (upstream, upstream),
            input_channels=(replacement_first, replacement_second),
        )

        self.assertFalse(aligner.receive(Barrier(8, source_id=root), upstream, replacement_second))
        self.assertFalse(aligner.receive(Barrier(9, source_id=root), upstream, previous_first))
        self.assertTrue(aligner.receive(Barrier(8, source_id=root), upstream, replacement_first))
        self.assertFalse(aligner.last_alignment_is_terminal)

    def test_coordinated_barrier_does_not_count_a_retired_topology_epoch(self):
        root = _src(1)
        upstream = _src(2)
        previous_first = self._channel(upstream, 0, "epoch-1")
        previous_second = self._channel(upstream, 1, "epoch-1")
        replacement_first = self._channel(upstream, 0, "epoch-2")
        replacement_second = self._channel(upstream, 1, "epoch-2")
        aligner = _BarrierAligner(
            {root: 1},
            (upstream, upstream),
            (previous_first, previous_second),
        )
        aligner.reconfigure_inputs(
            (upstream, upstream),
            input_channels=(replacement_first, replacement_second),
        )
        barrier = Barrier(8, source_id=root, coordinated=True)

        self.assertFalse(aligner.receive(barrier, upstream, previous_first))
        self.assertFalse(aligner.receive(barrier, upstream, replacement_second))
        self.assertTrue(aligner.receive(barrier, upstream, replacement_first))

    def test_unknown_direct_sender_is_rejected(self):
        root, expected, unknown = _src(1), _src(2), _src(99)
        aligner = _BarrierAligner({root: 1}, (expected,))

        with self.assertRaisesRegex(RuntimeError, "unexpected checkpoint barrier sender"):
            aligner.receive(Barrier(3, source_id=root), unknown)

    def test_mixed_terminal_epoch_forwards_normal_until_all_inputs_finish(self):
        root_a, root_b = _src(1), _src(2)
        upstream_a, upstream_b = _src(3, 0), _src(3, 1)
        aligner = _BarrierAligner(
            {root_a: 1, root_b: 1},
            (upstream_a, upstream_b),
        )

        self.assertFalse(aligner.receive(EndOfData(10, source_id=root_a), upstream_a))
        last = Barrier(10, source_id=root_b)
        self.assertTrue(aligner.receive(last, upstream_b))
        self.assertIs(type(aligner.barrier_to_forward(last)), Barrier)
        self.assertFalse(aligner.last_alignment_is_terminal)

        last = EndOfData(11, source_id=root_b)
        self.assertTrue(aligner.receive(last, upstream_b))
        self.assertIs(type(aligner.barrier_to_forward(last)), EndOfData)
        self.assertTrue(aligner.last_alignment_is_terminal)

    def test_terminal_barrier_arriving_last_is_downgraded_for_mixed_inputs(self):
        root_a, root_b = _src(1), _src(2)
        upstream_a, upstream_b = _src(3, 0), _src(3, 1)
        aligner = _BarrierAligner(
            {root_a: 1, root_b: 1},
            (upstream_a, upstream_b),
        )

        self.assertFalse(aligner.receive(Barrier(10, source_id=root_a), upstream_a))
        last = EndOfData(10, source_id=root_b)
        self.assertTrue(aligner.receive(last, upstream_b))
        self.assertIs(type(aligner.barrier_to_forward(last)), Barrier)
        self.assertFalse(aligner.last_alignment_is_terminal)

    def test_abort_discards_partial_epoch_and_ignores_late_barriers(self):
        root = _src(1)
        upstream_a, upstream_b = _src(2, 0), _src(2, 1)
        aligner = _BarrierAligner({root: 2}, (upstream_a, upstream_b))
        self.assertFalse(aligner.receive(Barrier(5, source_id=root), upstream_a))

        self.assertTrue(aligner.abort(5))
        self.assertFalse(aligner.receive(Barrier(5, source_id=root), upstream_b))
        self.assertFalse(aligner.receive(Barrier(6, source_id=root), upstream_a))
        self.assertTrue(aligner.receive(Barrier(6, source_id=root), upstream_b))


class BarrierAlignerEofTest(unittest.TestCase):
    def test_eof_aligns_when_all_sources_report(self):
        s1, s2 = _src(1), _src(2)
        aligner = _BarrierAligner({s1: 1, s2: 1})
        self.assertFalse(aligner.receive_eof(EndOfData(1, source_id=s1)))
        self.assertTrue(aligner.receive_eof(EndOfData(2, source_id=s2)))

    def test_eof_single_source(self):
        s1 = _src(1)
        aligner = _BarrierAligner({s1: 1})
        self.assertTrue(aligner.receive_eof(EndOfData(1, source_id=s1)))


class BarrierAlignerResetTest(unittest.TestCase):
    def test_reset_drops_orphans_at_or_below_cutoff(self):
        src = _src(1)
        aligner = _BarrierAligner({src: 5})  # never aligns within this test
        for bid in (3, 7, 12):
            aligner.receive(Barrier(bid, source_id=src))
        # Cutoff 7 -> ids 3 and 7 reclaimed, 12 (new epoch) survives.
        reclaimed = aligner.reset_inflight_before(7)
        self.assertEqual(reclaimed, 2)
        # The surviving id keeps its partial count: 1 more arrival != aligned.
        self.assertFalse(aligner.receive(Barrier(12, source_id=src)))

    def test_reset_is_idempotent(self):
        src = _src(1)
        aligner = _BarrierAligner({src: 5})
        aligner.receive(Barrier(3, source_id=src))
        self.assertEqual(aligner.reset_inflight_before(5), 1)
        self.assertEqual(aligner.reset_inflight_before(5), 0)

    def test_reset_with_no_inflight_returns_zero(self):
        aligner = _BarrierAligner({_src(1): 2})
        self.assertEqual(aligner.reset_inflight_before(100), 0)
