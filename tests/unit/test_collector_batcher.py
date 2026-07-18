# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable

import numpy as np
import pyarrow
import pytest

from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.runtime.collector.input_batcher import InputBatcher
from ray.klein.runtime.message import Barrier, Record, Watermark

BATCH_FORMATS = ("default", "native", "pyarrow", "numpy")


@pytest.fixture()
def output_buffer() -> list[Record]:
    return []


def make_batcher(
    output_buffer: list[Record],
    *,
    batch_size: int | None,
    batch_timeout: int = 300,
    batch_format: str = "native",
) -> InputBatcher:
    return InputBatcher(
        RuntimeInfo(
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            batch_format=batch_format,
        ),
        output_buffer.append,
    )


def simulate(batcher: InputBatcher, records: Iterable[Record]) -> None:
    for record in records:
        batcher.accumulate_then_flush(record)


def target(values: list[int], batch_format: str):
    if batch_format == "native":
        return values
    if batch_format == "pyarrow":
        return pyarrow.array(values)
    return np.array(values)


@pytest.mark.parametrize("batch_format", BATCH_FORMATS)
def test_no_batching_passes_records_through(output_buffer: list[Record], batch_format: str) -> None:
    records = [
        Barrier(1),
        Record({"id": 1}),
        Record({"id": 2}),
        Barrier(2),
        Record({"id": 3}),
        Record({"id": 4}),
        Barrier(3),
    ]

    simulate(
        make_batcher(output_buffer, batch_size=None, batch_format=batch_format),
        records,
    )

    assert output_buffer == records


@pytest.mark.parametrize("batch_format", BATCH_FORMATS)
def test_single_row_batching(output_buffer: list[Record], batch_format: str) -> None:
    simulate(
        make_batcher(output_buffer, batch_size=1, batch_format=batch_format),
        [
            Barrier(1),
            Record({"id": 1}),
            Record({"id": 2}),
            Barrier(2),
            Record({"id": 3}),
            Barrier(3),
            Record({"id": 4}),
            Barrier(4),
        ],
    )

    assert output_buffer == [
        Barrier(1),
        Record({"id": target([1], batch_format)}),
        Record({"id": target([2], batch_format)}),
        Barrier(2),
        Record({"id": target([3], batch_format)}),
        Barrier(3),
        Record({"id": target([4], batch_format)}),
        Barrier(4),
    ]


@pytest.mark.parametrize("batch_format", BATCH_FORMATS)
def test_barrier_flushes_partial_batch(output_buffer: list[Record], batch_format: str) -> None:
    simulate(
        make_batcher(output_buffer, batch_size=4, batch_format=batch_format),
        [
            Barrier(1),
            Record({"id": 1}),
            Record({"id": 2}),
            Barrier(2),
            Record({"id": 3}),
            Barrier(3),
            Record({"id": 4}),
            Barrier(4),
        ],
    )

    assert output_buffer == [
        Barrier(1),
        Record({"id": target([1, 2], batch_format)}),
        Barrier(2),
        Record({"id": target([3], batch_format)}),
        Barrier(3),
        Record({"id": target([4], batch_format)}),
        Barrier(4),
    ]


def test_watermark_flushes_partial_batch_before_control(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=4)

    batcher.accumulate_then_flush(Record({"id": 1}))
    batcher.accumulate_then_flush(Record({"id": 2}))
    batcher.accumulate_then_flush(Watermark(10))

    assert output_buffer == [Record({"id": [1, 2]}), Watermark(10)]


@pytest.mark.parametrize("batch_format", BATCH_FORMATS)
@pytest.mark.parametrize(
    ("batch_size", "input_records", "expected_batches"),
    [
        pytest.param(
            4,
            [
                Barrier(1),
                Record({"id": 1}),
                Record({"id": 2}),
                Barrier(2),
                Record({"id": 3}),
                Barrier(3),
                Record({"id": 4}),
                Barrier(4),
                Record({"id": 5}),
                Record({"id": 6}),
                Barrier(5),
            ],
            [([1, 2], 2), ([3], 3), ([4], 4), ([5, 6], 5)],
            id="batch-size-four",
        ),
        pytest.param(
            10,
            [
                Barrier(1),
                Record({"id": 1}),
                Record({"id": 2}),
                Barrier(2),
                Record({"id": 3}),
                Barrier(3),
                Record({"id": 4}),
                Barrier(4),
            ],
            [([1, 2], 2), ([3], 3), ([4], 4)],
            id="batch-size-larger-than-input",
        ),
    ],
)
def test_barrier_flush_is_independent_of_timeout(
    output_buffer: list[Record],
    batch_format: str,
    batch_size: int,
    input_records: list[Record],
    expected_batches: list[tuple[list[int], int]],
) -> None:
    batcher = make_batcher(
        output_buffer,
        batch_size=batch_size,
        batch_timeout=5,
        batch_format=batch_format,
    )

    simulate(batcher, input_records)
    batcher.flush()

    expected: list[Record] = [Barrier(1)]
    for values, barrier_id in expected_batches:
        expected.extend([Record({"id": target(values, batch_format)}), Barrier(barrier_id)])
    assert output_buffer == expected


def test_heterogeneous_columns_are_null_filled(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=2)

    batcher.accumulate_then_flush(Record({"a": 1}))
    batcher.accumulate_then_flush(Record({"b": 2}))

    assert [record.block for record in output_buffer] == [{"a": [1, None], "b": [None, 2]}]


def test_force_flush_emits_trailing_partial_batch(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=2)
    for value in (1, 2, 3):
        batcher.accumulate_then_flush(Record({"id": value}))

    batcher.force_flush()

    assert [record.block for record in output_buffer] == [{"id": [1, 2]}, {"id": [3]}]


def test_force_flush_on_empty_batch_is_noop(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=2)

    batcher.force_flush()

    assert output_buffer == []


def test_data_handler_can_be_redirected(output_buffer: list[Record]) -> None:
    redirected: list[Record] = []
    batcher = make_batcher(output_buffer, batch_size=1)
    batcher.accumulate_then_flush(Record({"id": 1}))

    batcher.data_handler = redirected.append
    batcher.accumulate_then_flush(Record({"id": 2}))

    assert [record.block for record in output_buffer] == [{"id": [1]}]
    assert [record.block for record in redirected] == [{"id": [2]}]


def columnar(values: list[int]) -> Record:
    return Record({"id": values}, num_rows=len(values))


def non_barrier_blocks(records: list[Record]) -> list[dict]:
    return [record.block for record in records if not isinstance(record, Barrier)]


def test_columnar_input_is_resliced_with_a_carried_tail(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=4)

    batcher.accumulate_then_flush(columnar([1, 2, 3]))
    assert output_buffer == []
    batcher.accumulate_then_flush(columnar([4, 5, 6]))
    assert non_barrier_blocks(output_buffer) == [{"id": [1, 2, 3, 4]}]
    assert output_buffer[0].num_rows == 4

    batcher.accumulate_then_flush(Barrier(1))
    assert non_barrier_blocks(output_buffer) == [{"id": [1, 2, 3, 4]}, {"id": [5, 6]}]


def test_barrier_force_drains_partial_columnar_input(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=10)

    batcher.accumulate_then_flush(columnar([1, 2, 3]))
    batcher.accumulate_then_flush(Barrier(7))

    assert output_buffer[0].block == {"id": [1, 2, 3]}
    assert output_buffer[0].num_rows == 3
    assert isinstance(output_buffer[1], Barrier)


def test_columnar_input_is_exploded_for_unbatched_downstream(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=None)

    batcher.accumulate_then_flush(columnar([10, 20, 30]))

    assert [record.block for record in output_buffer] == [{"id": 10}, {"id": 20}, {"id": 30}]
    assert all(record.num_rows is None for record in output_buffer)


def test_exact_multiple_leaves_no_carried_rows(output_buffer: list[Record]) -> None:
    batcher = make_batcher(output_buffer, batch_size=2)

    batcher.accumulate_then_flush(columnar([1, 2, 3, 4]))
    batcher.accumulate_then_flush(Barrier(1))

    assert non_barrier_blocks(output_buffer) == [{"id": [1, 2]}, {"id": [3, 4]}]
