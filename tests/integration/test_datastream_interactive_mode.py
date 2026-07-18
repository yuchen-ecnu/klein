# SPDX-License-Identifier: Apache-2.0


def test_each_interactive_consumer_executes_the_selected_graph(interactive_context, test_data_dir) -> None:
    stream = interactive_context.data.read_csv(str(test_data_dir / "test_data.csv"))
    filtered = stream.filter(lambda row: row["id"] >= 2)
    mapped = filtered.map(lambda row: {"id": row["id"] * 2})

    assert stream.take_all() == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert filtered.take_all() == [{"id": 2}, {"id": 3}]
    assert mapped.take_all() == [{"id": 4}, {"id": 6}]
    assert mapped.filter(lambda row: row["id"] >= 5).take_all() == [{"id": 6}]
