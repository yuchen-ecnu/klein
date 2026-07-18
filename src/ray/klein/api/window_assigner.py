# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod

from ray.klein.api.time_window import TimeWindow


class WindowAssigner(ABC):
    @abstractmethod
    def assign_windows(self, timestamp: int) -> tuple[TimeWindow, ...]:
        """Return the windows that contain ``timestamp``."""

    @property
    def is_merging(self) -> bool:
        return False
