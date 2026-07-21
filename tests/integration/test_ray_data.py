# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.klein_context import KleinContext
from tests.support.terminal import execute_terminal


def test_source_transform_consumer_and_multi_input() -> None:
    context = KleinContext()

    left = context.data.range(3)
    right = context.data.range(2)
    rows = execute_terminal(
        left.data.union(right).data.sort("id").data.take_all(),
        job_name="ray-data-union",
    )

    assert rows == [{"id": 0}, {"id": 0}, {"id": 1}, {"id": 1}, {"id": 2}]

    grouped = context.data.from_items(
        [
            {"group": "a", "value": 1},
            {"group": "a", "value": 2},
            {"group": "b", "value": 3},
        ]
    ).data.transform(lambda dataset: dataset.groupby("group").count())
    grouped_rows = execute_terminal(grouped.data.take_all(), job_name="ray-data-groupby")
    assert sorted(grouped_rows, key=lambda row: row["group"]) == [
        {"group": "a", "count()": 2},
        {"group": "b", "count()": 1},
    ]
