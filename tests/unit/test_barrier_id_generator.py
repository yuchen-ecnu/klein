# SPDX-License-Identifier: Apache-2.0
"""Unit tests for BarrierIdGenerator, the checkpoint-barrier id allocator.

A rebuilt coordinator gets a fresh generator and reseeds it above the last
persisted high-water (plus a stride covering unpersisted-but-in-flight ids), so
barrier ids never wrap back through values a downstream aligner might still be
tracking. That no-collision-across-restart property is the whole point, so it's
pinned here directly.
"""

import unittest

from ray.klein.runtime.coordinator.barrier_id_generator import BarrierIdGenerator


class BarrierIdGeneratorTest(unittest.TestCase):
    def test_starts_at_zero(self):
        self.assertEqual(BarrierIdGenerator().current, 0)

    def test_generate_is_monotonic(self):
        gen = BarrierIdGenerator()
        self.assertEqual([next(gen) for _ in range(3)], [1, 2, 3])
        self.assertEqual(gen.current, 3)

    def test_reseed_jumps_past_high_water_by_stride(self):
        gen = BarrierIdGenerator()
        gen.reseed(100)
        self.assertEqual(gen.current, 100 + BarrierIdGenerator.RESEED_STRIDE)
        # The next id is strictly above the restored high-water + stride, so it
        # can't collide with an orphan id <= high_water still pinned downstream.
        self.assertEqual(next(gen), 101 + BarrierIdGenerator.RESEED_STRIDE)

    def test_reseed_zero_is_noop(self):
        # high_water 0 means nothing was ever persisted -> don't jump the stride.
        gen = BarrierIdGenerator()
        gen.reseed(0)
        self.assertEqual(gen.current, 0)

    def test_reseed_is_idempotent(self):
        gen = BarrierIdGenerator()
        gen.reseed(100)
        first = gen.current
        gen.reseed(100)
        self.assertEqual(gen.current, first)

    def test_reseed_never_moves_backwards(self):
        # A lower high-water than the current id must not rewind the counter
        # (max guard) — otherwise a stale restore could reissue live ids.
        gen = BarrierIdGenerator()
        gen.reseed(1000)
        high = gen.current
        gen.reseed(5)  # lower than current
        self.assertEqual(gen.current, high)

    def test_generate_then_reseed_then_generate_strictly_increases(self):
        gen = BarrierIdGenerator()
        a = next(gen)
        gen.reseed(50)
        b = next(gen)
        self.assertGreater(b, a)
        self.assertGreater(b, 50)
