# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.util.queue import Queue

from ray.klein.api.sink_function import SinkFunction


class CollectFunction(SinkFunction):
    def __init__(self, output_queue: Queue, limit: int | None = None) -> None:
        super().__init__()
        self.output_queue: Queue = output_queue
        self.limit: int | None = limit

    def write(self, value: Any) -> None:
        self.output_queue.put(value)

    def flush(self) -> None:
        return None
