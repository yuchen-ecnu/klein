# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.job_status import JobStatus

logger = get_logger(__name__)


class JobHandle(ABC):
    """The result of :meth:`JobClient.execute` — a job to observe and control.

    A handle is the *running (or finished) job*, never the builder. The two
    concrete forms — :class:`LiveJobHandle` (a submitted streaming job backed by
    a JobManager actor) and :class:`CompletedJobHandle` (an already-finished
    batch / compile-only run with an in-memory result) — are siblings, so each
    only implements what is true for it.
    """

    @abstractmethod
    def wait(self) -> None:
        """Block until the job reaches a terminal state."""

    @abstractmethod
    def get(self) -> Any:
        """Block and return the result of one result-producing terminal.

        Use :meth:`wait` for jobs containing side-effect terminals or multiple
        sinks. A collecting ``take``/``take_all`` terminal must execute alone.
        """

    @property
    @abstractmethod
    def status(self) -> JobStatus:
        """Current job status."""

    @abstractmethod
    def cancel(self, timeout: int = 60) -> bool:
        """Cancel the job."""

    @property
    def namespace(self) -> str | None:
        """Per-job Ray namespace."""
        return None
