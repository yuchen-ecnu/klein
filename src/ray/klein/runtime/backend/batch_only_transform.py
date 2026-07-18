# SPDX-License-Identifier: Apache-2.0
"""Backend sentinel for a transform implemented through Ray Data only."""

from typing import Any


class BatchOnlyTransform:
    """Reject a Ray Data transform if a graph selects streaming execution."""

    def __call__(self, value: Any) -> Any:
        raise NotImplementedError(
            "stream.data operations are available in batch mode only. Use Klein's "
            "native map(), filter(), or flat_map() for an unbounded pipeline."
        )
