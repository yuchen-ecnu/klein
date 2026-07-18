# SPDX-License-Identifier: Apache-2.0
"""Backend sentinel for a source implemented through Ray Data only."""

from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction


class BatchOnlySource(SourceFunction):
    """Reject a Ray Data source if a graph selects streaming execution."""

    def __init__(self, operation: str) -> None:
        raise NotImplementedError(
            f"{operation!r} is available in batch mode only. Use a streaming integration "
            "or set execution.runtime.mode=batch."
        )

    def run(self, context: SourceContext) -> None:
        raise AssertionError("BatchOnlySource cannot run")

    def cancel(self) -> None:
        raise AssertionError("BatchOnlySource cannot run")

    def snapshot_state(self, checkpoint_id: int) -> None:
        return None

    def restore_state(self, state) -> None:
        return None
