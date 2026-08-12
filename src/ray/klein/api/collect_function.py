# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from typing import Any

from ray.util.queue import Queue

from ray.klein.api.sink_function import SinkFunction


@dataclass(frozen=True, slots=True)
class _CollectLimitExceeded:
    """Internal queue marker for a successfully drained safety-limit breach."""

    limit: int


def _validate_collect_limit(limit: int | None) -> int | None:
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int)):
        raise TypeError("limit must be an integer or None")
    if limit is not None and limit < 0:
        raise ValueError("limit must be greater than or equal to zero")
    return limit


class CollectFunction(SinkFunction):
    def __init__(
        self,
        output_queue: Queue,
        limit: int | None = None,
        *,
        truncate: bool = False,
    ) -> None:
        super().__init__()
        self.output_queue: Queue = output_queue
        self.limit = _validate_collect_limit(limit)
        self.truncate = truncate

    def write(self, value: Any) -> None:
        self.output_queue.put(value)

    def flush(self) -> None:
        return None
