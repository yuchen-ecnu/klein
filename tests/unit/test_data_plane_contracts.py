# SPDX-License-Identifier: Apache-2.0
"""Boundary tests for routing validation and weighted buffering."""

import asyncio
from types import SimpleNamespace

import pytest

from ray.klein.runtime.collector.edge_output import DeliveryMode
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning import (
    AdaptivePartitioner,
    ForwardPartitioner,
    Partitioner,
    SimplePartitioner,
)
from ray.klein.runtime.worker.pump import InboxEnvelope, inbox_envelope_rows
from ray.klein.runtime.worker.weighted_queue import WeightedQueue
from tests.unit.task_output_utils import open_task_output


def _open(partitioner, count: int):
    partitioner.open(SimpleNamespace(task_index=0, parallelism=1), count)
    return partitioner


def _collector(
    partitioner,
    target_count: int,
    *,
    max_rows: int = 10,
    control_targets=None,
    delivery_mode: DeliveryMode = DeliveryMode.INLINE,
):
    return open_task_output(
        [object() for _ in range(target_count)],
        partitioner,
        tuple(range(target_count)) if control_targets is None else tuple(control_targets),
        [f"target-{index}" for index in range(target_count)],
        max_rows=max_rows,
        namespace="job-ns",
        delivery_mode=delivery_mode,
    )


def test_router_rejects_out_of_range_custom_target_at_boundary() -> None:
    collector = _collector(_open(SimplePartitioner(lambda _record, count: [count]), 2), 2)

    with pytest.raises(ValueError, match=r"outside \[0, 2\)"):
        collector.collect(Record({"id": 1}))


def test_custom_partitioner_without_complete_spec_contract_fails_at_construction() -> None:
    class MissingSpecPartitioner(Partitioner):
        def partition(self, record):
            return [0]

    # CPython 3.12 quotes the missing method name while older versions do not.
    # Match the semantic parts of the error rather than interpreter wording.
    with pytest.raises(TypeError, match=r"abstract method.*to_spec"):
        MissingSpecPartitioner()


def test_router_rejects_retry_ring_outside_control_topology() -> None:
    collector = _collector(_open(AdaptivePartitioner(), 2), 2, control_targets=(0,))

    with pytest.raises(ValueError, match="outside its control topology"):
        collector.collect(Record({"id": 1}))


def test_output_buffer_bound_fails_fast_before_unbounded_growth() -> None:
    collector = _collector(
        _open(ForwardPartitioner(), 1),
        1,
        max_rows=2,
        control_targets=(0,),
        delivery_mode=DeliveryMode.PIPELINED,
    )

    collector.collect(Record({"id": 1}))
    collector.collect(Record({"id": 2}))
    with pytest.raises(BufferError, match="output edge would retain 3 rows"):
        collector.collect(Record({"id": 3}))


def test_output_buffer_byte_bound_catches_wide_records() -> None:
    collector = open_task_output(
        [object()],
        _open(ForwardPartitioner(), 1),
        (0,),
        ["target-0"],
        max_rows=10,
        delivery_mode=DeliveryMode.PIPELINED,
        config_values={"pipeline.output-buffer.max-bytes": 300},
    )

    collector.collect(Record({"blob": b"x" * 128}))
    with pytest.raises(BufferError, match=r"output edge would retain .* bytes"):
        collector.collect(Record({"blob": b"y" * 128}))


@pytest.mark.asyncio
async def test_weighted_inbox_capacity_is_rows_not_envelopes() -> None:
    inbox = WeightedQueue(3, inbox_envelope_rows)
    await inbox.put(InboxEnvelope(Record({"id": [1, 2, 3]}, num_rows=3)))
    blocked = asyncio.create_task(inbox.put(InboxEnvelope(Record({"id": 4}))))
    await asyncio.sleep(0)

    assert inbox.qsize() == 3
    assert inbox.envelope_count == 1
    assert not blocked.done()

    await inbox.get()
    await blocked
    assert inbox.qsize() == 1


@pytest.mark.asyncio
async def test_oversized_columnar_envelope_is_exclusive_but_makes_progress() -> None:
    inbox = WeightedQueue(2, inbox_envelope_rows)
    await inbox.put(InboxEnvelope(Record({"id": [1, 2, 3]}, num_rows=3)))

    assert inbox.qsize() == 3
    assert (await inbox.get()).payload.num_rows == 3


@pytest.mark.asyncio
async def test_cancelled_weighted_put_never_enqueues_ambiguously() -> None:
    inbox = WeightedQueue(1, inbox_envelope_rows)
    await inbox.put(InboxEnvelope(Record({"id": 1})))

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            inbox.put(InboxEnvelope(Record({"id": 2}))),
            timeout=0.001,
        )

    assert (await inbox.get()).payload.block == {"id": 1}
    assert inbox.qsize() == 0
    assert inbox.envelope_count == 0


@pytest.mark.asyncio
async def test_weighted_inbox_enforces_bytes_and_supports_immediate_admission() -> None:
    inbox = WeightedQueue(
        10,
        inbox_envelope_rows,
        max_bytes=3,
        size_bytes=lambda envelope: len(envelope.payload.block["blob"]),
    )
    await inbox.put(InboxEnvelope(Record({"blob": b"abc"})))

    assert inbox.byte_size == 3
    assert not await inbox.try_put(InboxEnvelope(Record({"blob": b"d"})))

    await inbox.get()
    assert await inbox.try_put(InboxEnvelope(Record({"blob": b"d"})))
    assert inbox.byte_size == 1
