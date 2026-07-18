# SPDX-License-Identifier: Apache-2.0

from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.job_status import JobStatus

logger = get_logger(__name__)


class CompletedJobHandle(JobHandle):
    """Handle to an already-finished run (batch result or compile-only graph).

    There is no job to wait on, drain or cancel — the work is done and the
    result is held in memory. Uses composition (no JobManager actor is created),
    so nothing here has to undo a more general superclass's behaviour.
    """

    def __init__(self, result: Any) -> None:
        self._result = result

    def wait(self) -> None:
        return None

    def get(self) -> Any:
        return self._result

    @property
    def status(self) -> JobStatus:
        return JobStatus.FINISHED

    def cancel(self, timeout: int = 60) -> bool:
        del timeout
        return True
