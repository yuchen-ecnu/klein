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
    arrow_block_to_mapping,
    block_num_rows,
    block_row_dict,
    concat_blocks,
    slice_block_rows,
    to_arrow_record_batch,
    wrapper_batch_data,
)
from ray.klein.api.row_kind import RowKind
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning import (
    BroadcastPartitioner,
    ForwardPartitioner,
    KeyPartitioner,
    SimplePartitioner,
)
from tests.unit.task_output_utils import open_task_output


class _RC:
    task_index = 0
    parallelism = 1
    config = Configuration()


def _open(part, n):
    part.open(_RC(), n)
    return part


class BlockHelpersTest(unittest.TestCase):
    def test_num_rows_across_formats(self):
        self.assertEqual(block_num_rows({"a": [1, 2, 3]}), 3)
        self.assertEqual(block_num_rows({"a": np.arange(4)}), 4)
        self.assertEqual(block_num_rows({"a": pa.array([1, 2])}), 2)
        self.assertEqual(block_num_rows({}), 0)
        self.assertEqual(block_num_rows(None), 0)
        self.assertEqual(block_num_rows(pa.record_batch({"a": [1, 2, 3]})), 3)

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

    def test_arrow_record_batch_slice_row_and_concat(self):
        first = pa.record_batch({"a": [1, 2], "b": [b"x", b"y"]})
        second = pa.record_batch({"a": [3], "b": [b"z"]})

        sliced = slice_block_rows(first, [1])
        combined = concat_blocks([sliced, second])

        self.assertIsInstance(combined, pa.RecordBatch)
        self.assertEqual(combined.to_pydict(), {"a": [2, 3], "b": [b"y", b"z"]})
        self.assertEqual(block_row_dict(combined, 1), {"a": 3, "b": b"z"})

    def test_fixed_shape_numpy_tensor_round_trips_at_udf_boundary(self):
        values = np.arange(24, dtype=np.uint8).reshape(3, 2, 4)

        batch = to_arrow_record_batch({"image": values}, expected_rows=3)
        restored = arrow_block_to_mapping(batch, "numpy")

        self.assertIsInstance(batch.column(0), pa.FixedShapeTensorArray)
        np.testing.assert_array_equal(restored["image"], values)

    def test_binary_with_embedded_nulls_round_trips_through_numpy_boundary(self):
        payload = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"

        batch = to_arrow_record_batch({"image": [payload, payload]}, expected_rows=2)
        restored = arrow_block_to_mapping(batch, "numpy")
        repacked = to_arrow_record_batch(restored, expected_rows=2)

        self.assertEqual(restored["image"].dtype, object)
        self.assertEqual(repacked.column("image").to_pylist(), [payload, payload])

    def test_numpy_batch_keeps_binary_as_objects_including_trailing_nulls(self):
        payload = b"binary\x00\x00"

        wrapped = wrapper_batch_data([payload], "numpy")

        self.assertEqual(wrapped.dtype, object)
        self.assertEqual(wrapped.tolist(), [payload])

    def test_fixed_width_numpy_binary_preserves_embedded_nulls(self):
        payload = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
        values = np.asarray([payload, payload])

        batch = to_arrow_record_batch({"image": values}, expected_rows=2)

        self.assertEqual(batch.column("image").to_pylist(), [payload, payload])

    def test_nested_binary_list_stays_a_list_column_across_numpy_boundary(self):
        page = b"%PDF\x00page"
        values = [[page, page], [page, page]]
        batch = to_arrow_record_batch({"pages": values}, expected_rows=2)

        restored = arrow_block_to_mapping(batch, "numpy")
        repacked = to_arrow_record_batch(restored, expected_rows=2)

        self.assertEqual(restored["pages"].dtype, object)
        self.assertEqual(restored["pages"].shape, (2,))
        self.assertTrue(pa.types.is_list(repacked.column("pages").type))
        self.assertEqual(repacked.column("pages").to_pylist(), values)


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
        part = _open(KeyPartitioner(key_selector=lambda r: r["k"], max_parallelism=128), 4)
        rec = Record({"k": [7, 7, 7]}, num_rows=3)
        routes = part.partition_columnar(rec, 3)
        self.assertEqual(len(routes), 1)
        self.assertIsNone(routes[0][1])

    def test_rows_split_by_key_preserve_affinity(self):
        part = _open(KeyPartitioner(key_selector=lambda r: r["k"], max_parallelism=128), 4)
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
    """TaskOutput routes a columnar Record through one edge, keeping
    the data column-oriented and (for keyBy) sliced per target."""

    def _collector(self, targets, names, partitioner):
        return open_task_output(
            targets,
            partitioner,
            tuple(range(len(targets))),
            names,
        )

    def test_forward_columnar_ships_whole_batch(self):
        d = _CapturingDownstream()
        part = _open(ForwardPartitioner(), 1)
        c = self._collector([d], ["d0"], part)
        c.collect(Record({"id": [1, 2, 3]}, num_rows=3))
        # One emit, carrying the intact columnar batch (no per-row explosion).
        self.assertEqual(len(d.received), 1)
        emitted = d.received[0]
        self.assertEqual(len(emitted), 1)
        self.assertIsInstance(emitted[0].block, pa.RecordBatch)
        self.assertEqual(emitted[0].block.to_pydict(), {"id": [1, 2, 3]})
        self.assertEqual(emitted[0].num_rows, 3)

    def test_keyby_columnar_splits_per_target(self):
        d0, d1, d2, d3 = (_CapturingDownstream() for _ in range(4))
        targets = [d0, d1, d2, d3]
        part = _open(KeyPartitioner(key_selector=lambda r: r["k"], max_parallelism=128), 4)
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

                    self.assertEqual(key_group_owner(key_group_for_key(kv.as_py(), 128), 128, 4), i)
        # Every input row delivered exactly once.
        total = sum(r.num_rows for d in targets for e in d.received for r in e)
        self.assertEqual(total, 4)

    def test_columnar_route_slices_changelog_sidecar_with_its_rows(self):
        even, odd = _CapturingDownstream(), _CapturingDownstream()
        collector = self._collector(
            [even, odd],
            ["even", "odd"],
            _open(SimplePartitioner(lambda record, _count: [record.block["id"] % 2]), 2),
        )
        record = Record({"id": [0, 1, 2, 3]}, num_rows=4)
        record.row_kinds = (
            RowKind.INSERT,
            RowKind.DELETE,
            RowKind.UPDATE_BEFORE,
            RowKind.UPDATE_AFTER,
        )

        collector.collect(record)

        even_record = even.received[0][0]
        odd_record = odd.received[0][0]
        self.assertEqual(even_record.block.column("id").to_pylist(), [0, 2])
        self.assertEqual(even_record.row_kinds, (RowKind.INSERT, RowKind.UPDATE_BEFORE))
        self.assertEqual(odd_record.block.column("id").to_pylist(), [1, 3])
        self.assertEqual(odd_record.row_kinds, (RowKind.DELETE, RowKind.UPDATE_AFTER))

    def test_row_transport_is_promoted_to_one_row_arrow_batch(self):
        downstream = _CapturingDownstream()
        collector = self._collector([downstream], ["d0"], _open(ForwardPartitioner(), 1))

        collector.collect(Record({"id": 1, "name": "Ada"}))

        emitted = downstream.received[0][0]
        self.assertIsInstance(emitted.block, pa.RecordBatch)
        self.assertEqual(emitted.block.to_pydict(), {"id": [1], "name": ["Ada"]})
        self.assertEqual(emitted.num_rows, 1)

    def test_row_microbatch_is_built_as_one_arrow_record_batch(self):
        downstream = _CapturingDownstream()
        collector = open_task_output(
            [downstream],
            ForwardPartitioner(),
            (0,),
            ["d0"],
            config_values={"pipeline.internal.batch-size": 3},
        )

        collector.collect(Record({"id": 1, "name": "Ada"}))
        collector.collect(Record({"id": 2, "name": "Linus"}))
        collector.collect(Record({"id": 3, "name": "Grace"}))

        self.assertEqual(len(downstream.received), 1)
        self.assertEqual(len(downstream.received[0]), 1)
        emitted = downstream.received[0][0]
        self.assertIsInstance(emitted.block, pa.RecordBatch)
        self.assertEqual(
            emitted.block.to_pydict(),
            {"id": [1, 2, 3], "name": ["Ada", "Linus", "Grace"]},
        )
        self.assertEqual(emitted.num_rows, 3)

    def test_arrow_incompatible_python_value_uses_compatibility_path(self):
        downstream = _CapturingDownstream()
        collector = self._collector([downstream], ["d0"], _open(ForwardPartitioner(), 1))
        value = object()

        collector.collect(Record({"value": value}))

        emitted = downstream.received[0][0]
        self.assertIs(emitted.block["value"], value)
        self.assertIsNone(emitted.num_rows)

    def test_unknown_mapping_subclass_keeps_python_semantics(self):
        class SemanticRow(dict):
            pass

        downstream = _CapturingDownstream()
        collector = self._collector([downstream], ["d0"], _open(ForwardPartitioner(), 1))
        row = SemanticRow(id=1)
        row.marker = "keep"

        collector.collect(Record(row))

        emitted = downstream.received[0][0]
        self.assertIs(emitted.block, row)
        self.assertEqual(emitted.block.marker, "keep")

    def test_columnar_passthrough_switch_keeps_legacy_row_wire(self):
        downstream = _CapturingDownstream()
        collector = open_task_output(
            [downstream],
            ForwardPartitioner(),
            (0,),
            ["d0"],
            config_values={"pipeline.columnar-passthrough.enabled": False},
        )

        collector.collect(Record({"id": 1}))

        emitted = downstream.received[0][0]
        self.assertEqual(emitted.block, {"id": 1})
        self.assertIsNone(emitted.num_rows)
