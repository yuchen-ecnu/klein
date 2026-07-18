# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Any

from ray.klein.api.function import Function
from ray.klein.api.source_context import SourceContext


class SourceFunction(Function, ABC):
    @abstractmethod
    def run(self, context: SourceContext) -> None:
        """
        The main loop for fetching data from DataSource. It will be triggered with a separate thread.
        """

    @abstractmethod
    def cancel(self) -> None:
        """Ask a running source loop to return without releasing resources.

        Implementations should set an event or flag observed by :meth:`run`.
        Resource cleanup belongs in ``Function.close`` and is invoked once by
        the task lifecycle after the source loop has stopped.
        """

    @abstractmethod
    def snapshot_state(self, checkpoint_id: int) -> Any:
        """Return this source subtask's opaque state at ``checkpoint_id``.

        The value must be pickleable. Sources should normally capture the
        position of the next record to read and advance that position before
        calling ``SourceContext.collect`` so a barrier emitted by that call
        observes the matching state.
        """

    @abstractmethod
    def restore_state(self, state: Any) -> None:
        """Restore state previously returned by :meth:`snapshot_state`."""

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        """Notify the source that a checkpoint is durably complete.

        Delivery is at least once across coordinator recovery, so implementations
        must use ``checkpoint_id`` as an idempotency key.
        """
