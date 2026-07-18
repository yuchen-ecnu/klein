# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterator


class BarrierIdGenerator:
    """Generates monotonically increasing checkpoint barrier ids.

    Per-coordinator-instance (not a process global): a rebuilt coordinator gets
    a fresh generator and ``reseed``s it above the last persisted high-water
    mark, so barrier ids never wrap back through values a downstream aligner
    might still be tracking from before the restart.
    """

    # Jump this far past the persisted high-water on reseed, so barriers that
    # were allocated after the last snapshot (hence not persisted) but may still
    # be in flight downstream also fall below the new range.
    RESEED_STRIDE = 1_000_000

    def __init__(self) -> None:
        self._current = 0

    def reseed(self, high_water: int) -> None:
        """Advance the counter past a restored high-water mark (idempotent)."""
        if high_water > 0:
            self._current = max(self._current, high_water + self.RESEED_STRIDE)

    @property
    def current(self) -> int:
        """Largest id allocated so far (the high-water mark to persist)."""
        return self._current

    def __iter__(self) -> Iterator[int]:
        return self

    def __next__(self) -> int:
        self._current += 1
        return self._current
