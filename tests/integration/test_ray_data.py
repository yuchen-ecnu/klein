# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.klein_context import KleinContext


def test_source_transform_consumer_and_multi_input() -> None:
    context = KleinContext()
    context.enable_interactive_mode()

    left = context.data.range(3)
    right = context.data.range(2)
    rows = left.data.union(right).data.sort("id").data.take_all()

    assert rows == [{"id": 0}, {"id": 0}, {"id": 1}, {"id": 1}, {"id": 2}]

    grouped = context.data.from_items(
        [
            {"group": "a", "value": 1},
            {"group": "a", "value": 2},
            {"group": "b", "value": 3},
        ]
    ).data.transform(lambda dataset: dataset.groupby("group").count())
    assert sorted(grouped.data.take_all(), key=lambda row: row["group"]) == [
        {"group": "a", "count()": 2},
        {"group": "b", "count()": 1},
    ]
