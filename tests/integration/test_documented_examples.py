# SPDX-License-Identifier: Apache-2.0
import runpy


def _load_run(project_root, filename):
    namespace = runpy.run_path(str(project_root / "examples" / filename))
    return namespace["run"]


def test_sql_batch_example(project_root) -> None:
    run = _load_run(project_root, "sql_batch.py")

    assert run() == [
        {"customer": "Ada", "total": 11},
        {"customer": "Grace", "total": 3},
    ]


def test_stateful_streaming_example(project_root) -> None:
    run = _load_run(project_root, "stateful_streaming.py")

    assert run() == [
        {"customer": "Ada", "total": 4},
        {"customer": "Ada", "total": 11},
        {"customer": "Grace", "total": 3},
    ]
