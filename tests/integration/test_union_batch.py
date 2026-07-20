# SPDX-License-Identifier: Apache-2.0
import pytest

from tests.support.assertions import assert_rows_equal
from tests.support.terminal import execute_terminal


@pytest.fixture()
def csv_streams(context, test_data_dir):
    return tuple(context.data.read_csv(str(test_data_dir / f"csv_data{index}.csv")) for index in range(1, 4))


@pytest.fixture()
def expected_rows():
    return [
        {"id": f"cd{dataset}_{row}", "name": f"cd{dataset}_{name}", "age": age}
        for dataset in range(1, 4)
        for row, name, age in [(1, "tom", 18), (2, "tony", 22), (3, "jerry", 21)]
    ]


def test_binary_union(csv_streams, expected_rows) -> None:
    first, second, _ = csv_streams

    rows = execute_terminal(first.union(second).take_all(), job_name="binary-union")

    assert_rows_equal(rows, expected_rows[:6], order_sensitive=False)


def test_chained_and_variadic_union_are_equivalent(csv_streams, expected_rows) -> None:
    first, second, third = csv_streams

    chained = execute_terminal(first.union(second).union(third).take_all(), job_name="chained-union")
    variadic = execute_terminal(first.union(second, third).take_all(), job_name="variadic-union")

    assert_rows_equal(chained, expected_rows, order_sensitive=False)
    assert_rows_equal(variadic, expected_rows, order_sensitive=False)


def test_union_composes_with_different_parallelism(csv_streams, expected_rows) -> None:
    first, second, third = csv_streams
    mapped = first.map(lambda row: row, concurrency=3)
    filtered = mapped.union(second).filter(lambda row: True, concurrency=2)

    sink = third.union(filtered).flat_map(lambda row: [row], concurrency=1).take_all()
    rows = execute_terminal(sink, job_name="union-parallelism")

    assert_rows_equal(rows, expected_rows, order_sensitive=False)
