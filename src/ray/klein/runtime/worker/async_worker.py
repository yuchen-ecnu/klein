# SPDX-License-Identifier: Apache-2.0
"""Reusable asyncio background-worker lifecycle."""

import asyncio
from abc import ABC, abstractmethod

from ray.klein._internal.logging import get_logger

logger = get_logger(__name__)


class AsyncWorker(ABC):
    """Run one asynchronous iteration repeatedly in a managed task."""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_cancelled_task: asyncio.Task | None = None
        self._stopping = False

    @abstractmethod
    async def _run(self) -> None:
        """Run one worker-loop iteration."""

    @abstractmethod
    def _get_name(self) -> str:
        """Return the task name used for diagnostics."""

    async def start(self) -> None:
        if self.healthy:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._loop(), name=self._get_name())

    async def stop(self, timeout: float = 30.0) -> None:
        self._stopping = True
        task = self._task
        if task is None or task.done() or asyncio.current_task() is task:
            return
        if self._stop_cancelled_task is not task:
            self._stop_cancelled_task = task
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except asyncio.TimeoutError as error:
            raise TimeoutError(f"Worker {self._get_name()} did not stop within {timeout:g} seconds") from error
        except asyncio.CancelledError:
            if not task.done():
                raise
        finally:
            if task.done():
                self._task = None

    @property
    def healthy(self) -> bool:
        return self._task is not None and not self._task.done()

    def ping(self) -> bool:
        return True

    async def _loop(self) -> None:
        try:
            while not self._stopping:
                await self._run()
        except asyncio.CancelledError:
            return
        except Exception as error:
            logger.exception("Unexpected error in worker %s", self._get_name())
            try:
                self.handle_exception(error)
            except Exception:
                logger.exception("Exception handler failed in worker %s", self._get_name())

    def handle_exception(self, error: Exception) -> None:
        """Handle an uncaught loop error before the worker terminates."""
        return
