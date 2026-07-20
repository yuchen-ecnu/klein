# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.integrations.console.console_output import flush_console_output, write_console_record


class ConsoleSinkFunction(SinkFunction):
    # Actor-preserving rescale may open a pending task-local sink before the
    # fenced runtime is closed. Console sinks own no exclusive external
    # resource, so that overlap is safe.
    supports_concurrent_rescale = True

    def __init__(self, limit: int = -1) -> None:
        self._limit = limit
        self._current_seq = 0
        self._subtask_index = -1

    def open(self, runtime_context: RuntimeContext) -> None:
        self._subtask_index = runtime_context.task_index

    def flush(self) -> None:
        flush_console_output()

    def write(self, value: dict[str, Any]) -> None:
        if self._limit == -1 or self._current_seq < self._limit:
            self._current_seq += 1
            write_console_record(
                subtask_index=self._subtask_index,
                sequence=self._current_seq,
                value=value,
            )
