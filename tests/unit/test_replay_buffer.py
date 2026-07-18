# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the single-fault at-least-once replay buffer.

Drives OutputCollector directly with an in-process mock downstream so the
seq-assignment, forwarded-watermark truncation, overflow guard, worker-pool
reroute interaction, and replay-op extraction are tested without Ray.
"""

import asyncio
import unittest
from unittest.mock import MagicMock

from ray.klein.runtime.collector.collector import OutputCollector
from ray.klein.runtime.message import PutAck, Record
from ray.klein.runtime.partitioning import AdaptivePartitioner, ForwardPartitioner


class MockDownstream:
    """Stands in for a downstream StreamTask actor handle.

    ``put`` returns a PutAck; ``forwarded`` (settable per test) is echoed back as
    the watermark. ``full_for`` makes the first N puts report a full inbox so the
    worker-pool reroute path is exercised.
    """

    def __init__(self, forwarded=-1, full_for=0):
        self.received = []
        self.forwarded = forwarded
        self.full_for = full_for

    def put(self, records, timeout=None, sender_vertex_id=None, batch_sequence=None):
        if self.full_for > 0:
            self.full_for -= 1
            return PutAck(False, 99, self.forwarded)
        self.received.append(records)
        return PutAck(True, len(self.received), self.forwarded)


def _drive(coro):
    return asyncio.run(coro)


def _records(n, base=0):
    return [Record({"id": base + i}) for i in range(n)]


def _new_collector(targets, names, partitioner=None, enabled=True):
    c = OutputCollector(
        targets,
        partitioner or ForwardPartitioner(),
        output_buffer_size=100,
        target_operator_names=names,
        put_timeout=1,
    )
    # ForwardPartitioner.partition needs _partition_count; open() sets it, but the
    # tests below call _aemit_records directly with explicit indices, so just set
    # what those need.
    c.configure_replay(enabled, sender_vertex_id="SENDER")
    return c


class ReplayBufferTest(unittest.TestCase):
    def test_landed_records_buffered_with_ascending_seq(self):
        d = MockDownstream()
        c = _new_collector([d], ["d0"])
        for i in range(3):
            _drive(c._aemit_records(0, _records(1, i)))
        ops = c.replay_ops_for_name("d0")
        seqs = [operation.sequence for operation in ops]
        self.assertEqual(seqs, [1, 2, 3])
        self.assertEqual(len(d.received), 3)

    def test_forwarded_watermark_truncates_buffer(self):
        # Downstream acks "forwarded through seq 2" -> entries 1,2 drop, 3 stays.
        d = MockDownstream(forwarded=-1)
        c = _new_collector([d], ["d0"])
        for i in range(3):
            _drive(c._aemit_records(0, _records(1, i)))
        self.assertEqual(len(c.replay_ops_for_name("d0")), 3)
        # Next put returns forwarded=2.
        d.forwarded = 2
        _drive(c._aemit_records(0, _records(1, 99)))  # seq 4
        remaining = [operation.sequence for operation in c.replay_ops_for_name("d0")]
        self.assertEqual(remaining, [3, 4])

    def test_advance_forwarded_is_monotonic(self):
        d = MockDownstream()
        c = _new_collector([d], ["d0"])
        for i in range(3):
            _drive(c._aemit_records(0, _records(1, i)))
        c.advance_forwarded(0, 2)
        self.assertEqual(len(c.replay_ops_for_name("d0")), 1)
        # A stale (lower) watermark is ignored.
        c.advance_forwarded(0, 1)
        self.assertEqual(len(c.replay_ops_for_name("d0")), 1)

    def test_disabled_does_not_buffer(self):
        d = MockDownstream()
        c = _new_collector([d], ["d0"], enabled=False)
        for i in range(3):
            _drive(c._aemit_records(0, _records(1, i)))
        self.assertEqual(c.replay_ops_for_name("d0"), [])

    def test_replay_op_does_not_rebuffer(self):
        d = MockDownstream()
        c = _new_collector([d], ["d0"])
        _drive(c._aemit_records(0, _records(1)))
        self.assertEqual(len(c.replay_ops_for_name("d0")), 1)
        # Re-delivery (is_replay=True) must NOT append a new buffer entry.
        _drive(c._aemit_records(0, _records(1), is_replay=True, replay_sequence=1))
        self.assertEqual(len(c.replay_ops_for_name("d0")), 1)
        self.assertEqual(len(d.received), 2)

    def test_seq_assigned_on_landing_index_under_reroute(self):
        # Worker-pool: target 0 is full once, so the batch reroutes to target 1.
        # The seq must land on index 1 (no hole punched in index 0's sequence).
        d0 = MockDownstream(full_for=1)
        d1 = MockDownstream()
        part = AdaptivePartitioner()

        class _RC:
            task_index = 0
            parallelism = 1

        part.open(_RC(), [d0, d1])
        c = _new_collector([d0, d1], ["d0", "d1"], partitioner=part)
        _drive(c._aemit_records(0, _records(1)))
        # Landed on index 1 after reroute; index 0 buffer empty, index 1 has seq 1.
        self.assertEqual(c.replay_ops_for_name("d0"), [])
        idx1 = c.replay_ops_for_name("d1")
        self.assertEqual([operation.sequence for operation in idx1], [1])

    def test_replay_ops_for_unknown_name_empty(self):
        d = MockDownstream()
        c = _new_collector([d], ["d0"])
        _drive(c._aemit_records(0, _records(1)))
        self.assertEqual(c.replay_ops_for_name("nope"), [])

    def test_replay_metric_counts_rows_and_shrinks_on_forwarded_watermark(self):
        d = MockDownstream()
        c = _new_collector([d], ["d0"])
        sizes = []
        c.attach_runtime_metrics(sizes.append, MagicMock(), MagicMock())

        _drive(c._aemit_records(0, [Record({"id": [1, 2, 3]}, num_rows=3)]))
        _drive(c._aemit_records(0, _records(1, 4)))
        self.assertEqual(sizes[-1], 4)

        c.advance_forwarded(0, 2)
        self.assertEqual(sizes[-1], 0)

    def test_full_inbox_records_backpressure_event_and_duration(self):
        d = MockDownstream(full_for=1)
        c = _new_collector([d], ["d0"])
        events = MagicMock()
        duration = MagicMock()
        c.attach_runtime_metrics(lambda _size: None, events, duration)

        _drive(c._aemit_records(0, _records(1)))

        events.inc.assert_called_once_with()
        duration.observe.assert_called_once()
        self.assertGreaterEqual(duration.observe.call_args.args[0], 0)
