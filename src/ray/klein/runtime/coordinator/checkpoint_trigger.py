# SPDX-License-Identifier: Apache-2.0
import time
from collections.abc import Callable

from ray.klein._internal.logging import get_logger

logger = get_logger(__name__)


class CheckpointTrigger:
    """Decides when a source should emit a checkpoint barrier.

    Two thresholds, whichever fires first:

    * **records** — every ``interval_records`` emitted records (0 = disabled).
      This is the high-throughput path: a busy source checkpoints by volume, so
      recovery only ever replays a bounded number of records.
    * **seconds** — every ``interval_seconds`` wall-clock seconds since the last
      barrier (0 = disabled). This is the backstop for bursty / low-traffic
      streams: a burst of fewer than ``interval_records`` rows followed by
      silence would otherwise leave those rows' offsets uncommitted until the
      next burst. The time branch is checked on BOTH the data path and the
      idle path (a connector calls ``should_trigger(record_emitted=False)`` when
      a poll returns nothing), so it fires even while the source is quiet.

    Whichever branch fires resets BOTH the counter and the timer, so a
    record-triggered barrier also restarts the clock (and vice versa) — the two
    never double-fire back-to-back.
    """

    def __init__(
        self,
        interval_records: int,
        interval_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if isinstance(interval_records, bool) or not isinstance(interval_records, int):
            raise TypeError("interval_records must be an integer")
        if interval_records < 0:
            raise ValueError("interval_records must be non-negative")
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float)):
            raise TypeError("interval_seconds must be a number")
        if interval_seconds < 0:
            raise ValueError("interval_seconds must be non-negative")
        self._interval_records = interval_records
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._counter: int = 0
        self._last_trigger: float | None = clock()
        if interval_records <= 0 and interval_seconds <= 0:
            logger.warning(
                "Both checkpoint trigger thresholds are disabled "
                "(interval_records=%s, interval_seconds=%s) — no checkpoint "
                "barriers will ever be emitted from this source.",
                interval_records,
                interval_seconds,
            )

    def should_trigger(self, record_emitted: bool) -> bool:
        """Return whether a barrier should be emitted now.

        ``record_emitted`` True on the data path (a record was just emitted, so
        the record counter advances), False on the idle path (only the time
        branch is consulted).
        """
        now = self._clock()
        if record_emitted and self._interval_records > 0:
            self._counter += 1
            if self._counter >= self._interval_records:
                self._reset(now)
                return True
        if self._interval_seconds > 0 and now - (self._last_trigger or 0) >= (self._interval_seconds):
            self._reset(now)
            return True
        return False

    def _reset(self, now: float) -> None:
        self._counter = 0
        self._last_trigger = now
