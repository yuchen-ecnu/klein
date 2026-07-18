# SPDX-License-Identifier: Apache-2.0
"""Lightweight, picklable per-operator progress for the CLI view.

A snapshot the JobManager hands to JobClient.wait()'s live renderer. Kept tiny
and free of live handles so it rides a single RPC cheaply.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SubtaskCounts:
    """One subtask's throughput counters, polled by the JobManager.

    Named (vs. a bare tuple) so call sites read by field, not position — the
    counts are summed across subtasks in ``progress_snapshot``."""

    rows_in: int = 0  # records consumed so far (0 for a source)
    rows_out: int = 0  # records emitted so far
    queued: int = 0  # records in the inbox, arrived but not yet processed
    capacity: int = 0  # inbox maxsize; queued/capacity is the backlog ratio
    # Monotonic ns counters for Flink-style time accounting; the view diffs two
    # samples / wall-clock to get a busy% / backpressure% per interval.
    busy_ns: int = 0  # time spent processing records
    backpressure_ns: int = 0  # time spent blocked emitting downstream
    backpressure_events: int = 0
    barriers_in: int = 0
    barriers_out: int = 0
    checkpoint_alignment_ms: float = 0.0
    checkpoint_barrier_latency_ms: float = 0.0
    checkpoint_state_size_bytes: int = 0
    last_checkpoint_id: int | None = None


@dataclass(frozen=True, slots=True)
class SubtaskProgress:
    """One physical task instance shown in the operator drill-down."""

    subtask_index: int
    status: str
    rows_in: int = 0
    rows_out: int = 0
    queued: int = 0
    capacity: int = 0
    busy_ns: int = 0
    backpressure_ns: int = 0
    backpressure_events: int = 0
    barriers_in: int = 0
    barriers_out: int = 0
    checkpoint_alignment_ms: float = 0.0
    checkpoint_barrier_latency_ms: float = 0.0
    checkpoint_state_size_bytes: int = 0
    last_checkpoint_id: int | None = None


@dataclass(frozen=True, slots=True)
class InstanceCounts:
    """Per-operator breakdown of its subtask (instance) states.

    The aggregate ``status`` collapses an operator to one label; this keeps the
    raw counts so the CLI view can show e.g. "3 running / 1 restarting" when an
    operator is mid-recovery."""

    running: int = 0
    pending: int = 0  # created/deployed but not yet running
    restarting: int = 0  # a subtask Ray is rebuilding (FAILED while job runs)
    finished: int = 0
    failed: int = 0


@dataclass(frozen=True, slots=True)
class OperatorProgress:
    """Tracks operator progress."""

    name: str
    op_id: int
    parallelism: int
    status: str
    rows_out: int  # total data records emitted across the operator's subtasks
    rows_in: int = 0  # total data records consumed (0 for sources)
    queued: int = 0  # records sitting in subtask inboxes, not yet processed
    capacity: int = 0  # summed inbox maxsize; queued/capacity = backlog ratio
    # Summed monotonic ns counters across subtasks; the view diffs two samples
    # and normalizes by (wall-clock × parallelism) to a busy% / backpressure%.
    busy_ns: int = 0  # time subtasks spent processing records
    backpressure_ns: int = 0  # time subtasks spent blocked emitting downstream
    # Per-instance state breakdown (running/pending/restarting/...).
    instances: InstanceCounts = field(default_factory=InstanceCounts)
    # Per-instance resource ask (one subtask's reservation).
    cpus: float = 0.0
    gpus: float = 0.0
    # Downstream operator ids — the outbound topology, so the CLI view can draw
    # the graph shape (fan-out, union, multi-sink) as an indented tree.
    downstream: tuple[int, ...] = ()
    backpressure_events: int = 0
    barriers_in: int = 0
    barriers_out: int = 0
    checkpoint_alignment_ms: float = 0.0
    checkpoint_barrier_latency_ms: float = 0.0
    checkpoint_state_size_bytes: int = 0
    last_checkpoint_id: int | None = None
    subtasks: tuple[SubtaskProgress, ...] = ()


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    """Full progress payload for one poll: per-operator rows + failover state."""

    operators: tuple[OperatorProgress, ...] = ()
    restarts: int = 0  # restarts within the sliding window
    max_restarts: int = 0  # window limit before the job is suppressed/failed
    window_seconds: int = 0
