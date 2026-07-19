# SPDX-License-Identifier: Apache-2.0
"""Delivery journal and downstream replay protocol tests."""

import asyncio
import unittest
from unittest.mock import MagicMock

from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.message import PutAck, Record
from ray.klein.runtime.partitioning import AdaptivePartitioner, ForwardPartitioner
from tests.unit.task_output_utils import open_task_output


class MockDownstream:
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


class ChannelDownstream(MockDownstream):
    def __init__(self):
        super().__init__()
        self.channels = []

    def try_put(self, records, sender_vertex_id=None, batch_sequence=None, delivery_channel=None):
        self.channels.append(delivery_channel)
        return self.put(records, sender_vertex_id=sender_vertex_id, batch_sequence=batch_sequence)


def _records(n, base=0):
    return [Record({"id": base + i}) for i in range(n)]


def _new_output(targets, names, partitioner=None, enabled=True, max_bytes=256 * 1024 * 1024):
    output = open_task_output(
        targets,
        partitioner or ForwardPartitioner(),
        tuple(range(len(targets))),
        names,
    )
    output.configure_replay(enabled, sender_vertex_id="SENDER", max_bytes=max_bytes)
    return output


def _pending(output, downstream_name):
    return output.replay_commands_for(downstream_name)


def _sequences(output, downstream_name):
    return [command.command.sequence for command in _pending(output, downstream_name)]


class DeliveryJournalTest(unittest.TestCase):
    def test_landed_records_are_journaled_with_ascending_sequences(self):
        downstream = MockDownstream()
        output = _new_output([downstream], ["d0"])
        for record in _records(3):
            output.collect(record)

        self.assertEqual(_sequences(output, "d0"), [1, 2, 3])
        self.assertEqual(len(downstream.received), 3)

    def test_forwarded_watermark_truncates_journal(self):
        downstream = MockDownstream()
        output = _new_output([downstream], ["d0"])
        for record in _records(3):
            output.collect(record)
        downstream.forwarded = 2
        output.collect(Record({"id": 99}))

        self.assertEqual(_sequences(output, "d0"), [3, 4])

    def test_disabled_journal_retains_nothing(self):
        output = _new_output([MockDownstream()], ["d0"], enabled=False)
        for record in _records(3):
            output.collect(record)

        self.assertEqual(_pending(output, "d0"), [])

    def test_replay_command_does_not_rejournal(self):
        downstream = MockDownstream()
        output = _new_output([downstream], ["d0"])
        output.collect(Record({"id": 1}))

        asyncio.run(output.send_commands(_pending(output, "d0")))

        self.assertEqual(_sequences(output, "d0"), [1])
        self.assertEqual(len(downstream.received), 2)

    def test_sequence_is_committed_on_actual_reroute_target(self):
        first = MockDownstream(full_for=1)
        second = MockDownstream()
        output = _new_output([first, second], ["d0", "d1"], AdaptivePartitioner())

        output.collect(Record({"id": 1}))

        self.assertEqual(_pending(output, "d0"), [])
        self.assertEqual(_sequences(output, "d1"), [1])

    def test_unknown_downstream_has_no_replay_commands(self):
        output = _new_output([MockDownstream()], ["d0"])
        output.collect(Record({"id": 1}))

        self.assertEqual(_pending(output, "unknown"), [])

    def test_acknowledgements_are_monotonic_and_publish_row_metrics(self):
        journal = DeliveryJournal(1)
        sizes = []
        journal.configure(True, "sender", 1024 * 1024)
        journal.attach_observers(sizes.append)
        journal.record_delivery(0, [Record({"id": [1, 2, 3]}, num_rows=3)], 1)
        journal.record_delivery(0, [Record({"id": 4})], 2)
        self.assertEqual(sizes[-1], 4)

        journal.acknowledge(0, 2)
        journal.acknowledge(0, 1)

        self.assertEqual(sizes[-1], 0)
        self.assertEqual(journal.pending_for(0), ())

    def test_retained_bytes_shrink_when_acknowledged(self):
        journal = DeliveryJournal(1)
        byte_sizes = []
        journal.configure(True, "sender", 1024 * 1024)
        journal.attach_observers(None, byte_sizes.append)
        journal.record_delivery(0, _records(1), 1)
        self.assertGreater(byte_sizes[-1], 1)

        journal.acknowledge(0, 1)

        self.assertEqual(byte_sizes[-1], 0)

    def test_retained_byte_guard_fails_before_growth(self):
        journal = DeliveryJournal(1)
        journal.configure(True, "sender", 1)

        with self.assertRaisesRegex(MemoryError, "above configured"):
            journal.record_delivery(0, _records(1), 1)

    def test_full_inbox_records_backpressure_metrics(self):
        output = _new_output([MockDownstream(full_for=1)], ["d0"])
        events = MagicMock()
        duration = MagicMock()
        output.attach_runtime_metrics(lambda _size: None, lambda _size: None, events, duration)

        output.collect(Record({"id": 1}))

        events.inc.assert_called_once_with()
        duration.observe.assert_called_once()
        self.assertGreaterEqual(duration.observe.call_args.args[0], 0)

    def test_explicit_channel_ack_releases_without_another_put(self):
        downstream = ChannelDownstream()
        output = open_task_output([downstream], ForwardPartitioner(), (0,), ["d0"])
        output.configure_replay(
            True,
            sender_vertex_id="vertex",
            max_bytes=1024 * 1024,
            sender_task_name="upstream-task",
        )
        output.collect(Record({"id": 1}))

        channel = downstream.channels[0]
        self.assertEqual(channel.sender_task_name, "upstream-task")
        self.assertEqual((channel.edge_index, channel.target_index), (0, 0))
        self.assertEqual(_sequences(output, "d0"), [1])

        output.acknowledge_delivery(channel.edge_index, channel.target_index, 1)

        self.assertEqual(_sequences(output, "d0"), [])
