# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.runtime_context import RuntimeContext


class Function:
    """Lifecycle hooks shared by user-defined sources and sinks."""

    def open(self, runtime_context: RuntimeContext) -> None:
        """Initialize the function before processing starts."""

    def close(self) -> None:
        """Release resources after processing stops."""
