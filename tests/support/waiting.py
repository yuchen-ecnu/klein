# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def wait_until(
    predicate: Callable[[], T],
    *,
    timeout: float = 10.0,
    interval: float = 0.05,
    description: str = "condition",
) -> T:
    """Poll until a truthy result is returned, with a bounded deadline."""

    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except Exception as exc:  # transient failures are reported on timeout
            last_error = exc
        time.sleep(interval)

    message = f"Timed out after {timeout:.2f}s waiting for {description}"
    if last_error is not None:
        raise TimeoutError(message) from last_error
    raise TimeoutError(message)
