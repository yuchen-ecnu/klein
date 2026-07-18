# SPDX-License-Identifier: Apache-2.0
"""In-memory source used by Klein's local collection backend."""

from collections.abc import Iterable
from typing import Any

from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction


class CollectionSource(SourceFunction):
    """Emit a finite collection once and then complete."""

    def __init__(self, values: Iterable[Any]) -> None:
        self._values = list(values)
        self._next_index = 0

    def run(self, context: SourceContext) -> None:
        while self._next_index < len(self._values):
            value = self._values[self._next_index]
            self._next_index += 1
            context.collect(value)

    def cancel(self) -> None:
        self._next_index = len(self._values)

    def snapshot_state(self, checkpoint_id: int) -> int:
        return self._next_index

    def restore_state(self, state: Any) -> None:
        if isinstance(state, bool) or not isinstance(state, int) or state < 0:
            raise ValueError("collection source state must be a non-negative integer")
        self._next_index = min(state, len(self._values))
