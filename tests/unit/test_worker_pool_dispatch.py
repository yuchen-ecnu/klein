# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the worker-pool dispatch ring and rescale task assignment.

These two pieces of pure logic underpin failover rerouting (a full/dead
downstream advances the ring to the next worker) and rescaling (each upstream
instance owns a disjoint, gap-free slice of downstreams). Tested directly,
without Ray actors.
"""

import unittest

from ray.klein.runtime.partitioning import (
    ForwardPartitioner,
    RescalePartitioner,
    RoundRobinPartitioner,
    WorkerPoolDispatcher,
)


class WorkerPoolDispatcherTest(unittest.TestCase):
    def test_rejects_empty_assignment(self):
        with self.assertRaises(ValueError):
            WorkerPoolDispatcher([])

    def test_current_starts_at_first(self):
        d = WorkerPoolDispatcher([3, 5, 7])
        self.assertEqual(d.current(), 3)

    def test_advance_round_robins_and_wraps(self):
        d = WorkerPoolDispatcher([3, 5, 7])
        self.assertEqual(d.advance(), 5)
        self.assertEqual(d.advance(), 7)
        self.assertEqual(d.advance(), 3)  # wraps

    def test_single_target_advance_is_stable(self):
        d = WorkerPoolDispatcher([9])
        self.assertEqual(d.current(), 9)
        self.assertEqual(d.advance(), 9)
        self.assertEqual(d.advance(), 9)


class DistributeTasksTest(unittest.TestCase):
    """RescalePartitioner.distribute_tasks: the static upstream->downstream map."""

    def _assert_full_disjoint_cover(self, source_parallelism, target_parallelism):
        # Every downstream index must be owned by exactly one upstream instance.
        seen = []
        for task_index in range(source_parallelism):
            seen.extend(RescalePartitioner.distribute_tasks(source_parallelism, target_parallelism, task_index))
        self.assertEqual(sorted(seen), list(range(target_parallelism)))
        self.assertEqual(len(seen), len(set(seen)), "assignments overlap")

    def test_scale_up_1_to_4(self):
        # task 0 -> {0}, plus the 1->N stride; cover all 4 downstreams disjointly.
        self._assert_full_disjoint_cover(1, 4)

    def test_scale_up_2_to_4(self):
        self.assertEqual(RescalePartitioner.distribute_tasks(2, 4, 0), [0, 2])
        self.assertEqual(RescalePartitioner.distribute_tasks(2, 4, 1), [1, 3])
        self._assert_full_disjoint_cover(2, 4)

    def test_scale_down_4_to_2(self):
        # N->1 collapse: each upstream maps to a single downstream by modulo.
        self.assertEqual(RescalePartitioner.distribute_tasks(4, 2, 0), [0])
        self.assertEqual(RescalePartitioner.distribute_tasks(4, 2, 1), [1])
        self.assertEqual(RescalePartitioner.distribute_tasks(4, 2, 2), [0])
        self.assertEqual(RescalePartitioner.distribute_tasks(4, 2, 3), [1])

    def test_equal_parallelism_is_identity(self):
        for i in range(4):
            self.assertEqual(RescalePartitioner.distribute_tasks(4, 4, i), [i])

    def test_scale_up_3_to_7_disjoint_cover(self):
        self._assert_full_disjoint_cover(3, 7)

    def test_spec_and_runtime_share_the_same_rescale_topology(self):
        spec = RescalePartitioner().to_spec()
        self.assertEqual(spec.target_indices(2, 5, 1), (1, 3))

    def test_forward_topology_rejects_parallelism_drift(self):
        with self.assertRaisesRegex(ValueError, "equal source and target parallelism"):
            ForwardPartitioner().to_spec().target_indices(1, 2, 0)

    def test_round_robin_starts_at_partition_zero(self):
        partitioner = RoundRobinPartitioner()
        partitioner.open(type("Context", (), {"task_index": 0, "parallelism": 1})(), 3)
        self.assertEqual([partitioner.partition(None)[0] for _ in range(4)], [0, 1, 2, 0])
