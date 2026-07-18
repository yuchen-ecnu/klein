# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.coordinator.checkpoint_strategy import CheckpointStrategy


class StreamRuntimeContext(RuntimeContext, ABC):
    """RuntimeContext for streaming execution, adding checkpoint capabilities."""

    @property
    @abstractmethod
    def checkpoint_strategy(self) -> CheckpointStrategy:
        """The strategy used to align and persist checkpoints."""
