# SPDX-License-Identifier: Apache-2.0


class StateConflictError(RuntimeError):
    """Raised when a state update loses an optimistic-version race."""
