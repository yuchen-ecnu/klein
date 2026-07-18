# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Record / KeyRecord equality and PutAck fields."""

import unittest

import numpy as np

from ray.klein.runtime.message import KeyRecord, PutAck, Record


class RecordEqualityTest(unittest.TestCase):
    def test_scalar_equal(self):
        self.assertEqual(Record({"id": 1}), Record({"id": 1}))

    def test_scalar_not_equal(self):
        # The new not-Iterable branch: scalar values that differ are unequal.
        self.assertNotEqual(Record({"id": 1}), Record({"id": 2}))

    def test_type_mismatch_not_equal(self):
        self.assertNotEqual(Record({"id": 1}), Record({"id": "1"}))

    def test_non_record_not_equal(self):
        self.assertNotEqual(Record({"id": 1}), 42)
        self.assertNotEqual(Record({"id": 1}), "Record({'id': 1})")

    def test_iterable_equal(self):
        self.assertEqual(Record({"v": [1, 2, 3]}), Record({"v": [1, 2, 3]}))

    def test_iterable_not_equal(self):
        self.assertNotEqual(Record({"v": [1, 2, 3]}), Record({"v": [1, 2, 4]}))

    def test_numpy_equal(self):
        self.assertEqual(Record({"v": np.array([1, 2])}), Record({"v": np.array([1, 2])}))

    def test_numpy_shape_mismatch_is_not_equal(self):
        # numpy raises ValueError comparing mismatched shapes; the guard treats
        # that as "not equal" rather than letting the exception escape.
        a = Record({"v": np.array([1, 2, 3])})
        b = Record({"v": np.array([1, 2])})
        self.assertNotEqual(a, b)

    def test_missing_key_not_equal(self):
        self.assertNotEqual(Record({"a": 1, "b": 2}), Record({"a": 1}))
        self.assertNotEqual(Record({"a": 1}), Record({"a": 1, "b": 2}))

    def test_records_are_unhashable_because_their_blocks_are_mutable(self):
        with self.assertRaises(TypeError):
            hash(Record({"id": 1}))


class KeyRecordEqualityTest(unittest.TestCase):
    def test_same_key_and_block_equal(self):
        self.assertEqual(KeyRecord("k", {"id": 1}), KeyRecord("k", {"id": 1}))

    def test_different_key_not_equal(self):
        self.assertNotEqual(KeyRecord("k1", {"id": 1}), KeyRecord("k2", {"id": 1}))

    def test_keyrecord_not_equal_plain_record(self):
        # Type-exact comparison: a KeyRecord is never equal to a plain Record.
        self.assertNotEqual(KeyRecord("k", {"id": 1}), Record({"id": 1}))


class PutAckTest(unittest.TestCase):
    def test_forwarded_sequence_default(self):
        self.assertEqual(PutAck(True, 5).forwarded_sequence, -1)

    def test_forwarded_sequence_explicit(self):
        acknowledgement = PutAck(False, 99, 7)
        self.assertEqual(acknowledgement.forwarded_sequence, 7)
        self.assertFalse(acknowledgement.accepted)
