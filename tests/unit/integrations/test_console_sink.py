# SPDX-License-Identifier: Apache-2.0
import json
from types import SimpleNamespace

from ray.klein.integrations.console.console_sink import ConsoleSinkFunction


def test_console_sink_writes_machine_readable_records_only_to_stdout(capsys) -> None:
    sink = ConsoleSinkFunction(limit=2)
    sink.open(SimpleNamespace(task_index=4))

    sink.write({"id": 1})
    sink.write({"id": 2})
    sink.write({"id": 3})
    sink.flush()

    captured = capsys.readouterr()
    records = [json.loads(line) for line in captured.out.splitlines()]
    assert not captured.err
    assert records == [
        {"sink": "console", "subtask_index": 4, "sequence": 1, "value": {"id": 1}},
        {"sink": "console", "subtask_index": 4, "sequence": 2, "value": {"id": 2}},
    ]
