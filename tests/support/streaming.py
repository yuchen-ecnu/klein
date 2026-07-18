# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction

logger = get_logger(__name__)


class LoopSourceFunction(SourceFunction):
    """Controllable finite/infinite source shared by graph and runtime tests."""

    def __init__(self, sleep_interval: float = 0.05, record_num: int = -1, restore_validator=None):
        self.idx = 0
        self._interrupted = False
        self._sleep_interval = sleep_interval
        self._record_num = record_num
        self._restore_validator = restore_validator

    def run(self, context: SourceContext) -> None:
        while not self._interrupted:
            self.idx += 1
            context.collect({"idx": self.idx})
            time.sleep(self._sleep_interval)
            if 0 < self._record_num <= self.idx:
                self.cancel()

    def snapshot_state(self, checkpoint_id: int) -> int:
        return self.idx

    def restore_state(self, state: int) -> None:
        if self._restore_validator is not None:
            self._restore_validator(state)
        self.idx = state

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        logger.info("Completed source checkpoint %s", checkpoint_id)

    def cancel(self) -> None:
        self._interrupted = True


def flat_map_identity(data: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield data
