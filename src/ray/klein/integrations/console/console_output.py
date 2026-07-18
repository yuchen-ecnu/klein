# SPDX-License-Identifier: Apache-2.0
"""The sole stdout boundary for Klein's console integration."""

import json
import sys
import threading
from typing import Any

_OUTPUT_LOCK = threading.Lock()


def write_console_record(*, subtask_index: int, sequence: int, value: Any) -> None:
    """Write one parseable JSON Lines record to stdout.

    Operational logs use stderr. Keeping sink data on stdout lets shell users
    pipe records safely and prevents values from being mistaken for log events.
    """

    record = {
        "sink": "console",
        "subtask_index": subtask_index,
        "sequence": sequence,
        "value": value,
    }
    from ray.klein.api.changelog_row import ChangelogRow

    if isinstance(value, ChangelogRow):
        record["row_kind"] = value.row_kind.value
    line = json.dumps(record, ensure_ascii=False, default=str, separators=(",", ":"))
    with _OUTPUT_LOCK:
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def flush_console_output() -> None:
    with _OUTPUT_LOCK:
        sys.stdout.flush()
