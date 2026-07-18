# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod


class SinkCommittable(ABC):
    """Serializable transaction prepared by a two-phase-commit sink.

    The checkpoint coordinator persists the committable before invoking
    :meth:`commit`. Implementations must make both ``commit`` and ``abort``
    idempotent because coordinator recovery can retry either operation.
    """

    @property
    @abstractmethod
    def transaction_id(self) -> str:
        """Stable idempotency key for this prepared transaction."""

    @abstractmethod
    def commit(self) -> None:
        """Publish the prepared transaction after checkpoint durability."""

    @abstractmethod
    def abort(self) -> None:
        """Discard a transaction whose checkpoint cannot complete."""
