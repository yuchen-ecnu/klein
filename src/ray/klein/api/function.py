# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.runtime_context import RuntimeContext


class Function:
    """Lifecycle hooks shared by user-defined sources and sinks."""

    # Retained actors prepare the replacement runtime while the old runtime is
    # fenced but still open, preserving an exact rollback path. Lifecycle UDFs
    # that hold exclusive external resources must therefore opt in explicitly
    # only when two task-local instances may overlap during this handoff.
    supports_concurrent_rescale = False

    def open(self, runtime_context: RuntimeContext) -> None:
        """Initialize the function before processing starts."""

    def close(self) -> None:
        """Release resources after processing stops."""
