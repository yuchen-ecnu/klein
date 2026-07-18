# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import abstractmethod

from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.api.sink_function import SinkFunction


class TwoPhaseCommitSinkFunction(SinkFunction):
    """Sink writer that prepares serializable transactions at checkpoints.

    Records remain private to the writer until :meth:`prepare_commit` returns a
    committable. The checkpoint coordinator persists that value and performs
    the second-phase commit only after the global checkpoint is durable.
    """

    @abstractmethod
    def prepare_commit(self, checkpoint_id: int) -> SinkCommittable | None:
        """Close the current transaction and return its committable.

        ``None`` means the transaction contains no output. The writer must begin
        a fresh transaction before accepting the next record.
        """

    @abstractmethod
    def abort_current_transaction(self) -> None:
        """Discard writer-local data that has not reached ``prepare_commit``."""
