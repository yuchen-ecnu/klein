# SPDX-License-Identifier: Apache-2.0
import pandas as pd

from ray.klein.api.klein_context import KleinContext
from tests.support.terminal import execute_terminal


def test_write_json_round_trip_from_items(context, tmp_path) -> None:
    output = tmp_path / "items-json"
    write_sink = context.from_items([{"one": [1, 2, 3]}, {"two": ["a", "b", "c"]}]).data.write_json(str(output))

    context.execute("write-json-items", sinks=(write_sink,)).wait()

    reader = KleinContext(context.config)
    rows = execute_terminal(reader.data.read_json(str(output)).take_all(), job_name="read-json-items")
    assert rows == [
        {"one": [1, 2, 3]},
        {"two": ["a", "b", "c"]},
    ]


def test_write_json_round_trip_from_pandas(context, tmp_path) -> None:
    output = tmp_path / "pandas-json"
    write_sink = context.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})).data.write_json(str(output))

    context.execute("write-json-pandas", sinks=(write_sink,)).wait()

    reader = KleinContext(context.config)
    rows = execute_terminal(reader.data.read_json(str(output)).take_all(), job_name="read-json-pandas")
    assert rows == [
        {"a": 1, "b": 4},
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
    ]
