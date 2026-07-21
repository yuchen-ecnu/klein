# SPDX-License-Identifier: Apache-2.0

import pytest

from ray.klein.api.klein_context import KleinContext


@pytest.fixture()
def interactive_context(configuration):
    context = KleinContext(configuration)
    with pytest.warns(DeprecationWarning, match="deprecated"):
        returned = context.enable_interactive_mode()
    assert returned is context
    return context


class _AtLeast:
    def __init__(self, threshold: int, *, inclusive: bool = True) -> None:
        self._threshold = threshold
        self._inclusive = inclusive

    def __call__(self, row: dict[str, int]) -> bool:
        if self._inclusive:
            return row["id"] >= self._threshold
        return row["id"] > self._threshold


def test_each_interactive_consumer_executes_the_selected_graph(interactive_context, test_data_dir) -> None:
    stream = interactive_context.data.read_csv(str(test_data_dir / "test_data.csv"))
    filtered = stream.filter(lambda row: row["id"] >= 2)
    mapped = filtered.map(lambda row: {"id": row["id"] * 2})

    assert stream.take_all() == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert filtered.take_all() == [{"id": 2}, {"id": 3}]
    assert mapped.take_all() == [{"id": 4}, {"id": 6}]
    assert mapped.filter(lambda row: row["id"] >= 5).take_all() == [{"id": 6}]


def test_batch_filter_forwards_callable_class_constructor(context) -> None:
    sink = (
        context.data.from_items([{"id": 1}, {"id": 2}, {"id": 3}])
        .filter(
            _AtLeast,
            fn_constructor_args=[2],
            fn_constructor_kwargs={"inclusive": False},
            concurrency=1,
        )
        .take_all()
    )
    actual = context.execute("callable-filter-constructor", sinks=(sink,)).get()

    assert actual == [{"id": 3}]
