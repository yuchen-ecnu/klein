# SPDX-License-Identifier: Apache-2.0
"""Backend sentinel for a sink implemented through Ray Data only."""

from typing import Any

from ray.klein.api.sink_function import SinkFunction


class BatchOnlySink(SinkFunction):
    """Reject a Ray Data consumer if a graph selects streaming execution."""

    def __init__(self) -> None:
        raise NotImplementedError("stream.data consumers are available in batch mode only")

    def write(self, value: dict[str, Any]) -> None:
        raise AssertionError("BatchOnlySink cannot run")

    def flush(self) -> None:
        raise AssertionError("BatchOnlySink cannot run")
