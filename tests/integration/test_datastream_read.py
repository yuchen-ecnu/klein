# SPDX-License-Identifier: Apache-2.0
import json

import pytest


@pytest.fixture()
def tabular_files(tmp_path):
    rows = [
        {"sepal_length": 5.1, "sepal_width": 3.5, "variety": "Setosa"},
        {"sepal_length": 7.0, "sepal_width": 3.2, "variety": "Versicolor"},
    ]
    csv_path = tmp_path / "iris.csv"
    csv_path.write_text(
        "sepal_length,sepal_width,variety\n5.1,3.5,Setosa\n7.0,3.2,Versicolor\n",
        encoding="utf-8",
    )
    json_path = tmp_path / "iris.json"
    json_path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    return {"csv": csv_path, "json": json_path, "rows": rows}


def test_read_text_returns_each_line(interactive_context, tabular_files) -> None:
    rows = interactive_context.data.read_text(str(tabular_files["csv"])).take_all()

    assert rows == [
        {"text": "sepal_length,sepal_width,variety"},
        {"text": "5.1,3.5,Setosa"},
        {"text": "7.0,3.2,Versicolor"},
    ]


@pytest.mark.parametrize("format_name", ["csv", "json"])
def test_read_tabular_formats(interactive_context, tabular_files, format_name) -> None:
    rows = getattr(interactive_context.data, f"read_{format_name}")(str(tabular_files[format_name])).take_all()

    assert rows == tabular_files["rows"]
