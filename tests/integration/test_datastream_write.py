# SPDX-License-Identifier: Apache-2.0
import pandas as pd


def test_write_json_round_trip_from_items(context, interactive_context, tmp_path) -> None:
    output = tmp_path / "items-json"
    context.from_items([{"one": [1, 2, 3]}, {"two": ["a", "b", "c"]}]).data.write_json(str(output))

    context.execute("write-json-items").wait()

    assert interactive_context.data.read_json(str(output)).take_all() == [
        {"one": [1, 2, 3]},
        {"two": ["a", "b", "c"]},
    ]


def test_write_json_round_trip_from_pandas(context, interactive_context, tmp_path) -> None:
    output = tmp_path / "pandas-json"
    context.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})).data.write_json(str(output))

    context.execute("write-json-pandas").wait()

    assert interactive_context.data.read_json(str(output)).take_all() == [
        {"a": 1, "b": 4},
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
    ]
