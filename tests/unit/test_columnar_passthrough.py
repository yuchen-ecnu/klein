# SPDX-License-Identifier: Apache-2.0
"""Unit tests for columnar-passthrough routing + block helpers.

Covers the pure logic the columnar path adds — partitioner row-routing
(partition_columnar) and the format-aware block helpers — without Ray, so the
key-affinity and slice/concat invariants are exercised in isolation.
"""

import unittest

import numpy as np
import pyarrow as pa

from ray.klein._internal.block import (
    block_num_rows,
    block_row_dict,
    concat_blocks,
    slice_block_rows,
)
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.collector.collector import OutputCollector
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning import (
    BroadcastPartitioner,
    ForwardPartitioner,
    KeyPartitioner,
    SimplePartitioner,
)


class _RC:
    task_index = 0
    parallelism = 1
    config = Configuration()


def _open(part, n):
    part.open(_RC(), [object()] * n)
    return part


class BlockHelpersTest(unittest.TestCase):
    def test_num_rows_across_formats(self):
        self.assertEqual(block_num_rows({"a": [1, 2, 3]}), 3)
        self.assertEqual(block_num_rows({"a": np.arange(4)}), 4)
        self.assertEqual(block_num_rows({"a": pa.array([1, 2])}), 2)
        self.assertEqual(block_num_rows({}), 0)
        self.assertEqual(block_num_rows(None), 0)

    def test_slice_rows_list(self):
        out = slice_block_rows({"a": [10, 11, 12, 13], "b": ["x", "y", "z", "w"]}, [1, 3])
        self.assertEqual(out, {"a": [11, 13], "b": ["y", "w"]})

    def test_slice_rows_numpy_is_view_like(self):
        out = slice_block_rows({"a": np.array([5, 6, 7, 8])}, [0, 2])
        np.testing.assert_array_equal(out["a"], np.array([5, 7]))

    def test_slice_rows_pyarrow(self):
        out = slice_block_rows({"a": pa.array([5, 6, 7, 8])}, [1, 2])
        self.assertEqual(out["a"].to_pylist(), [6, 7])

    def test_row_dict(self):
        self.assertEqual(block_row_dict({"a": [1, 2, 3], "b": [4, 5, 6]}, 1), {"a": 2, "b": 5})

    def test_concat_single_block_is_identity(self):
        block = {"a": [1, 2]}
        self.assertIs(concat_blocks([block]), block)

    def test_concat_lists(self):
        out = concat_blocks([{"a": [1, 2]}, {"a": [3]}])
        self.assertEqual(out, {"a": [1, 2, 3]})

    def test_concat_numpy(self):
        out = concat_blocks([{"a": np.array([1, 2])}, {"a": np.array([3, 4])}])
        np.testing.assert_array_equal(out["a"], np.array([1, 2, 3, 4]))

    def test_concat_pyarrow(self):
        out = concat_blocks([{"a": pa.array([1])}, {"a": pa.array([2, 3])}])
        self.assertEqual(out["a"].to_pylist(), [1, 2, 3])


class ContentIndependentRoutingTest(unittest.TestCase):
    """Forward/Broadcast ship the whole batch (row_indices None) — no slicing."""

    def test_forward_whole_batch(self):
        part = _open(ForwardPartitioner(), 3)
        rec = Record({"id": [1, 2, 3]}, num_rows=3)
        routes = part.partition_columnar(rec, 3)
        self.assertEqual(routes, [(0, None)])

    def test_broadcast_whole_batch_to_all(self):
        part = _open(BroadcastPartitioner(), 3)
        rec = Record({"id": [1, 2, 3]}, num_rows=3)
        routes = part.partition_columnar(rec, 3)
        self.assertEqual(routes, [(0, None), (1, None), (2, None)])


class KeyRoutingTest(unittest.TestCase):
    def test_single_key_whole_batch_no_slice(self):
        # All rows share a key -> one whole-batch route (copy-free fast path).
        part = _open(KeyPartitioner(key_selector=lambda r: r["k"]), 4)
        rec = Record({"k": [7, 7, 7]}, num_rows=3)
        routes = part.partition_columnar(rec, 3)
        self.assertEqual(len(routes), 1)
        self.assertIsNone(routes[0][1])

    def test_rows_split_by_key_preserve_affinity(self):
        part = _open(KeyPartitioner(key_selector=lambda r: r["k"]), 4)
        block = {"k": [1, 2, 1, 2, 3]}
        rec = Record(block, num_rows=5)
        routes = part.partition_columnar(rec, 5)
        # Every row index appears exactly once across the buckets.
        all_idx = sorted(i for _t, idxs in routes for i in idxs)
        self.assertEqual(all_idx, [0, 1, 2, 3, 4])
        # Rows with the same key land in the same bucket (same target).
        target_of = {}
        for t, idxs in routes:
            for i in idxs:
                target_of[i] = t
        self.assertEqual(target_of[0], target_of[2])  # key 1
        self.assertEqual(target_of[1], target_of[3])  # key 2
        # Reconstructing per-bucket slices matches stable key-group ownership.
        from ray.klein.state.key_group_range import key_group_for_key, key_group_owner

        for t, idxs in routes:
            for i in idxs:
                expected = key_group_owner(key_group_for_key(block["k"][i], 128), 128, 4)
                self.assertEqual(t, expected)

    def test_custom_partitioner_per_row(self):
        # Route even ids to 0, odd to 1.
        part = _open(SimplePartitioner(lambda rec, n: [rec.block["id"] % 2]), 2)
        block = {"id": [0, 1, 2, 3]}
        rec = Record(block, num_rows=4)
        routes = dict(part.partition_columnar(rec, 4))
        self.assertEqual(sorted(routes[0]), [0, 2])
        self.assertEqual(sorted(routes[1]), [1, 3])


class _CapturingDownstream:
    def __init__(self):
        self.received = []

    def put(self, records, timeout=None, sender_vertex_id=None, batch_sequence=None):
        from ray.klein.runtime.message import PutAck

        self.received.append(records)
        return PutAck(True, len(self.received), -1)


class CollectorColumnarRoutingTest(unittest.TestCase):
    """OutputCollector routes a columnar Record through the batch holder, keeping
    the data column-oriented and (for keyBy) sliced per target."""

    def _collector(self, targets, names, partitioner):
        c = OutputCollector(
            targets,
            partitioner,
            output_buffer_size=100,
            target_operator_names=names,
            put_timeout=1,
        )
        # The collector defaults to an internal batch size of 1 before open(),
        # so every push flushes immediately (inline path) — no override needed.
        c.configure_pipelining(False)
        return c

    def test_forward_columnar_ships_whole_batch(self):
        d = _CapturingDownstream()
        part = _open(ForwardPartitioner(), 1)
        c = self._collector([d], ["d0"], part)
        c.collect(Record({"id": [1, 2, 3]}, num_rows=3))
        # One emit, carrying the intact columnar batch (no per-row explosion).
        self.assertEqual(len(d.received), 1)
        emitted = d.received[0]
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].block, {"id": [1, 2, 3]})
        self.assertEqual(emitted[0].num_rows, 3)

    def test_keyby_columnar_splits_per_target(self):
        d0, d1, d2, d3 = (_CapturingDownstream() for _ in range(4))
        targets = [d0, d1, d2, d3]
        part = _open(KeyPartitioner(key_selector=lambda r: r["k"]), 4)
        c = self._collector(targets, ["d0", "d1", "d2", "d3"], part)
        block = {"k": [1, 2, 1, 2]}
        c.collect(Record(block, num_rows=4))
        # Rows grouped by key -> each target that received data got a column
        # slice whose rows all hash to that target.
        for i, d in enumerate(targets):
            for emitted in d.received:
                rec = emitted[0]
                for kv in rec.block["k"]:
                    from ray.klein.state.key_group_range import key_group_for_key, key_group_owner

                    self.assertEqual(key_group_owner(key_group_for_key(kv, 128), 128, 4), i)
        # Every input row delivered exactly once.
        total = sum(r.num_rows for d in targets for e in d.received for r in e)
        self.assertEqual(total, 4)
