# SPDX-License-Identifier: Apache-2.0


class WorkerPoolDispatcher:
    """Backpressure-driven round-robin over a fixed set of downstream tasks.

    The routing rule is intentionally trivial: hand the next batch to the next
    task in the ring. Real load-awareness is now provided by backpressure — a
    downstream whose ``asyncio.Queue`` inbox is full makes the upstream's
    ``put`` time out, which fires ``on_emit_timeout`` and advances the ring to
    the next task. So "send to whoever has free inbox space" emerges from the
    queue semantics instead of polled buffer-size statistics.
    """

    def __init__(self, assigned_tasks: list[int]) -> None:
        if not assigned_tasks:
            raise ValueError("assigned_tasks cannot be empty")
        self._tasks: list[int] = assigned_tasks
        self._cursor: int = 0

    def current(self) -> int:
        return self._tasks[self._cursor]

    def advance(self) -> int:
        self._cursor = (self._cursor + 1) % len(self._tasks)
        return self.current()

    def take(self) -> int:
        """Return the current task and advance the cursor for the next record."""
        current = self.current()
        self.advance()
        return current

    def ring_from(self, task: int) -> tuple[int, ...]:
        """Return every eligible task once, beginning with ``task``."""
        try:
            start = self._tasks.index(task)
        except ValueError as error:
            raise ValueError(f"task {task} is not assigned to this dispatcher") from error
        return tuple(self._tasks[start:] + self._tasks[:start])
