# SPDX-License-Identifier: Apache-2.0
import time
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import (
    CheckpointTriggerOptions,
)
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metric_group import MetricGroup
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.coordinator.checkpoint_registration import CheckpointRegistration
from ray.klein.runtime.coordinator.checkpoint_trigger import (
    CheckpointTrigger,
)
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import Barrier, DeliveryChannel, EndOfData
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry
from ray.klein.state.state_snapshot_reference import StateSnapshotReference

logger = get_logger(__name__)


class CheckpointStrategy(ABC):
    """How a StreamTask participates in aligned checkpointing."""

    def open(self) -> None:
        """Initialize the strategy before the task starts."""
        return

    @abstractmethod
    def on_barrier_received(
        self,
        barrier: Barrier,
        on_barrier_aligned: Callable | None = None,
        sender_vertex_id: ExecutionVertexId | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> bool:
        """Called when a barrier arrives. Return whether to emit it downstream."""

    @abstractmethod
    def on_eof_received(self, barrier: EndOfData) -> bool:
        """Called when an eof arrives. Return whether all source eofs are in."""

    @abstractmethod
    def restore_source_state(self) -> SourceCheckpointEntry | None:
        """Obtain state owned by this source subtask."""

    async def restore_source_state_async(self) -> SourceCheckpointEntry | None:
        """Loop-safe source-state restore."""

        return self.restore_source_state()

    @abstractmethod
    def should_trigger(self, record_emitted: bool, record_count: int = 1) -> bool:
        """Source-only: whether a barrier should be emitted now.

        ``record_emitted`` True on the data path (a record was just emitted),
        False on the idle path (connector poll returned nothing) — the latter
        only consults the time threshold. ``record_count`` accounts for an
        atomic columnar source batch without generating adjacent barriers."""

    @abstractmethod
    def generate_next_barrier(
        self,
        is_eof: bool,
        *,
        force: bool = False,
        checkpoint_id: int | None = None,
    ) -> Barrier | None:
        """Source-only: ask the coordinator to allocate the next barrier."""

    def reset_trigger(self) -> None:
        """Restart source-local thresholds after consuming a shared epoch."""

        return

    @abstractmethod
    def register_operator_state(self, barrier_id: int, reference: StateSnapshotReference) -> bool:
        """Register managed operator state captured for an aligned barrier."""

    @abstractmethod
    async def restore_operator_states_async(self) -> tuple[StateSnapshotReference, ...]:
        """Return the latest state fragments needed by this task."""

    @abstractmethod
    async def restore_durable_operator_states_async(self) -> tuple[StateSnapshotReference, ...]:
        """Return durable state fragments needed by this task."""

    async def restore_rescale_operator_states_async(
        self,
        operation_id: str,
    ) -> tuple[StateSnapshotReference, ...]:
        del operation_id
        return ()

    def register_sink_committable(self, barrier_id: int, committable: SinkCommittable) -> bool:
        """Register a prepared sink transaction before acknowledging a barrier."""

        del barrier_id, committable
        return False

    def register_operator_metrics(self, barrier_id: int, metrics: dict[str, int | float]) -> bool:
        """Publish task-local checkpoint timings for dashboard drill-down."""

        del barrier_id, metrics
        return False

    @property
    def last_alignment_duration_ms(self) -> float:
        return 0.0

    def reconfigure_barrier_split(
        self,
        barrier_splits: dict[ExecutionVertexId, int],
        input_vertex_ids: tuple[ExecutionVertexId, ...] | None = None,
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        """Replace physical-input alignment counts at a quiescent rescale cut."""

        del barrier_splits, input_vertex_ids, input_channels
        raise NotImplementedError(f"{type(self).__name__} does not support runtime topology changes")

    def validate_barrier_reconfiguration(self) -> None:
        """Fail before a topology transaction if old barriers are still aligned."""

        return

    def reconfigure_barrier_inputs(
        self,
        input_vertex_ids: Iterable[ExecutionVertexId],
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        """Replace direct physical inputs at a quiescent topology cut."""

        del input_vertex_ids, input_channels
        raise NotImplementedError(f"{type(self).__name__} does not support runtime topology changes")

    def reset_inflight_before(self, cutoff_barrier_id: int) -> int:
        """Drop alignment bookkeeping inherited from an older coordinator epoch."""

        del cutoff_barrier_id
        return 0

    def discard_checkpoint(self, barrier_id: int) -> int:
        """Drop alignment bookkeeping for one explicitly aborted epoch."""

        del barrier_id
        return 0

    def abort_checkpoint(self, barrier_id: int) -> bool:
        """Discard one partial local alignment. Idempotent."""

        return bool(self.discard_checkpoint(barrier_id))

    def barrier_to_forward(self, barrier: Barrier) -> Barrier:
        """Return the normalized barrier produced by the latest alignment."""

        return barrier

    @property
    def last_alignment_is_terminal(self) -> bool:
        """Whether the latest aligned cut permanently closed every input."""

        return False


class _BarrierAligner:
    """Chandy-Lamport alignment counter for one operator.

    Owns the in-flight counts and the per-source split table. The split table
    says how many upstream subtasks of a given source feed this operator; a
    barrier is *aligned* once that many copies (same barrier id) have arrived.

    The lookup is **total**: a source absent from the table aligns on its first
    barrier (count 1). A missing entry means "no fan-out recorded for this
    source", which can only legitimately mean a single upstream — so treating it
    as 1 is correct and avoids the KeyError that a raw ``dict[source_id]`` threw
    when broadcast routing and the split BFS disagreed on a Union branch.
    """

    def __init__(
        self,
        barrier_splits: dict[ExecutionVertexId, int],
        input_vertex_ids: Iterable[ExecutionVertexId] | None = None,
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        inputs = None if input_vertex_ids is None else tuple(input_vertex_ids)
        self._split = dict(barrier_splits)
        self._inflight: dict[int, int] = {}
        self._input_vertex_ids = Counter(inputs or ())
        self._coordinated_inflight: dict[int, Counter[ExecutionVertexId]] = {}
        self._coordinated_channels: dict[int, set[object]] = {}
        self._last_coordinated_barrier_id = -1
        self._direct_input_vertex_ids = None if inputs is None else Counter(inputs)
        self._direct_inflight: dict[int, dict[object, tuple[ExecutionVertexId, bool]]] = {}
        self._direct_expected: dict[int, Counter[ExecutionVertexId]] = {}
        self._direct_resolved_through = -1
        self._terminal_inputs: Counter[ExecutionVertexId] = Counter()
        self._terminal_input_channels: set[object] = set()
        # Removed terminal lanes remain tombstoned so a delayed old-topology RPC
        # is ignored instead of failing the task or satisfying a new epoch.
        self._retired_terminal_input_channels: set[object] = set()
        self._retired_input_channels: set[DeliveryChannel] = set()
        self._configured_input_channels: frozenset[DeliveryChannel] | None = None
        self._last_aligned_id: int | None = None
        self._last_alignment_is_terminal = False
        self._eof_from_src: dict[ExecutionVertexId, bool] = dict.fromkeys(barrier_splits, False)
        if input_channels is not None:
            if self._direct_input_vertex_ids is None:
                raise ValueError("checkpoint input channels require input vertex ids")
            self._reconfigure_terminal_inputs(self._direct_input_vertex_ids, input_channels)

    def _expected(self, source_id: ExecutionVertexId) -> int:
        return self._split.get(source_id, 1)

    def coordinated_barrier_finalized(self, barrier_id: int) -> bool:
        return barrier_id <= self._last_coordinated_barrier_id

    def receive(
        self,
        barrier: Barrier,
        sender_vertex_id: ExecutionVertexId | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> bool:
        """Count one barrier; return True once its source is fully aligned."""
        if barrier.coordinated:
            return self._receive_coordinated(barrier, sender_vertex_id, delivery_channel)
        if sender_vertex_id is not None:
            return self._receive_direct(barrier, sender_vertex_id, delivery_channel)
        count = self._inflight.pop(barrier.id, 0) + 1
        if count >= self._expected(barrier.source_id):
            return True
        self._inflight[barrier.id] = count
        return False

    def _receive_coordinated(
        self,
        barrier: Barrier,
        sender_vertex_id: ExecutionVertexId | None,
        delivery_channel: DeliveryChannel | None,
    ) -> bool:
        """Align one shared epoch once across every direct physical input."""

        if barrier.id <= self._last_coordinated_barrier_id:
            return False
        if not self._input_vertex_ids:
            self._last_coordinated_barrier_id = barrier.id
            return True
        if sender_vertex_id is None:
            raise ValueError("a coordinated checkpoint barrier requires its direct sender identity")
        channel = delivery_channel or sender_vertex_id
        if delivery_channel is not None and delivery_channel.sender_vertex_id != sender_vertex_id:
            raise ValueError("checkpoint delivery_channel does not match its sender")
        if channel in self._retired_input_channels:
            return False
        if self._configured_input_channels is not None and channel not in self._configured_input_channels:
            raise RuntimeError(f"unexpected checkpoint delivery channel {channel}")
        if sender_vertex_id not in self._input_vertex_ids:
            raise ValueError(f"unexpected coordinated checkpoint sender {sender_vertex_id}")
        channels = self._coordinated_channels.setdefault(barrier.id, set())
        if channel in channels:
            return False
        channels.add(channel)
        seen = self._coordinated_inflight.setdefault(barrier.id, Counter())
        seen[sender_vertex_id] += 1
        if any(seen[sender] < count for sender, count in self._input_vertex_ids.items()):
            return False
        self._coordinated_inflight.pop(barrier.id, None)
        self._coordinated_channels.pop(barrier.id, None)
        self._last_coordinated_barrier_id = barrier.id
        return True

    def _receive_direct(
        self,
        barrier: Barrier,
        sender_vertex_id: ExecutionVertexId,
        delivery_channel: DeliveryChannel | None,
    ) -> bool:
        inputs = self._direct_input_vertex_ids
        if barrier.id <= self._direct_resolved_through:
            return False

        channel = self._direct_channel(barrier, sender_vertex_id, delivery_channel, inputs)
        if (
            channel in self._terminal_input_channels
            or channel in self._retired_terminal_input_channels
            or channel in self._retired_input_channels
        ):
            return False
        if self._configured_input_channels is not None and channel not in self._configured_input_channels:
            raise RuntimeError(f"unexpected checkpoint delivery channel {channel}")
        if inputs is not None and sender_vertex_id not in inputs:
            raise RuntimeError(f"unexpected checkpoint barrier sender {sender_vertex_id}")
        self._retire_replaced_terminal_channel(channel)
        if self._direct_inflight and barrier.id not in self._direct_inflight:
            current_barrier_id = min(self._direct_inflight)
            raise RuntimeError(f"checkpoint barrier {barrier.id} arrived before epoch {current_barrier_id} aligned")

        seen = self._direct_inflight.setdefault(barrier.id, {})
        terminal = isinstance(barrier, EndOfData)
        previous = seen.get(channel)
        incoming = (sender_vertex_id, terminal)
        if previous is not None:
            if previous != incoming:
                raise RuntimeError(
                    f"checkpoint barrier {barrier.id} on channel {channel} changed sender or terminal kind"
                )
            return False
        seen[channel] = incoming

        if not self._direct_alignment_complete(barrier, seen, inputs):
            return False

        self._complete_direct_alignment(barrier.id, seen, inputs)
        return True

    def _direct_channel(
        self,
        barrier: Barrier,
        sender_vertex_id: ExecutionVertexId,
        delivery_channel: DeliveryChannel | None,
        inputs: Counter[ExecutionVertexId] | None,
    ) -> object:
        expected_multiplicity = self._expected(barrier.source_id) if inputs is None else inputs[sender_vertex_id]
        if delivery_channel is None:
            if expected_multiplicity > 1:
                raise ValueError(
                    f"checkpoint sender {sender_vertex_id} has {expected_multiplicity} input lanes; "
                    "delivery_channel is required"
                )
            return sender_vertex_id
        if delivery_channel.sender_vertex_id != sender_vertex_id:
            raise ValueError("checkpoint delivery_channel does not match its sender")
        return delivery_channel

    def _direct_alignment_complete(
        self,
        barrier: Barrier,
        seen: dict[object, tuple[ExecutionVertexId, bool]],
        inputs: Counter[ExecutionVertexId] | None,
    ) -> bool:
        if inputs is None:
            return len(seen) >= self._expected(barrier.source_id)
        expected = self._direct_expected.setdefault(
            barrier.id,
            inputs - self._terminal_inputs,
        )
        observed = Counter(sender for sender, _terminal in seen.values())
        return all(observed[sender] >= count for sender, count in expected.items())

    def _complete_direct_alignment(
        self,
        barrier_id: int,
        seen: dict[object, tuple[ExecutionVertexId, bool]],
        inputs: Counter[ExecutionVertexId] | None,
    ) -> None:
        self._direct_inflight.pop(barrier_id, None)
        self._direct_expected.pop(barrier_id, None)
        self._direct_resolved_through = barrier_id
        for input_channel, (_sender, is_terminal) in seen.items():
            if not is_terminal or input_channel in self._terminal_input_channels:
                continue
            self._terminal_input_channels.add(input_channel)
            self._retired_terminal_input_channels.discard(input_channel)
        self._rebuild_terminal_inputs()
        self._last_aligned_id = barrier_id
        self._last_alignment_is_terminal = inputs is not None and all(
            self._terminal_inputs[sender] >= count for sender, count in inputs.items()
        )

    @staticmethod
    def _terminal_channel_sender(channel: object) -> ExecutionVertexId | None:
        if isinstance(channel, DeliveryChannel):
            sender = channel.sender_vertex_id
            return sender if isinstance(sender, ExecutionVertexId) else None
        return channel if isinstance(channel, ExecutionVertexId) else None

    @staticmethod
    def _delivery_lane_coordinates(channel: DeliveryChannel) -> tuple[object, str, int, int]:
        """Identify one route independently of its topology incarnation."""

        return (
            channel.sender_vertex_id,
            channel.sender_task_name,
            channel.edge_index,
            channel.target_index,
        )

    def _retire_replaced_terminal_channel(self, channel: object) -> None:
        """Prevent a new topology incarnation from inheriting an old EOF.

        ``DeliveryChannel`` equality includes ``topology_epoch``.  An exact
        channel therefore denotes the same physical lane and remains terminal;
        the same route coordinates with a different epoch denote a new lane.
        """

        if not isinstance(channel, DeliveryChannel):
            return
        coordinates = self._delivery_lane_coordinates(channel)
        replaced = {
            terminal
            for terminal in self._terminal_input_channels
            if isinstance(terminal, DeliveryChannel)
            and self._delivery_lane_coordinates(terminal) == coordinates
            and terminal.topology_epoch != channel.topology_epoch
        }
        if not replaced:
            return
        self._terminal_input_channels.difference_update(replaced)
        self._retired_terminal_input_channels.update(replaced)
        self._rebuild_terminal_inputs()

    def _rebuild_terminal_inputs(self) -> None:
        """Derive the sender Counter exclusively from current terminal lanes."""

        self._terminal_inputs = Counter(
            sender
            for channel in self._terminal_input_channels
            if (sender := self._terminal_channel_sender(channel)) is not None
        )

    def _reconfigure_terminal_inputs(
        self,
        inputs: Counter[ExecutionVertexId],
        input_channels: Iterable[DeliveryChannel] | None,
    ) -> None:
        """Retain EOF only for physical lanes present in the new topology.

        Callers that know the complete channel inventory should provide it.  An
        exact ``DeliveryChannel`` (including ``topology_epoch``) preserves EOF;
        a removed channel is tombstoned and a newly-added channel starts active.
        The vertex-only compatibility path retains at most the new multiplicity
        for each still-present sender and never transfers EOF to a new sender.
        """

        candidates = set(self._terminal_input_channels)
        configured: frozenset[DeliveryChannel] | None = None
        if input_channels is not None:
            channels = tuple(input_channels)
            if len(channels) != len(set(channels)):
                raise ValueError("checkpoint input channels cannot contain duplicates")
            channel_inputs: Counter[ExecutionVertexId] = Counter()
            for channel in channels:
                if not isinstance(channel, DeliveryChannel):
                    raise TypeError("checkpoint input channels must contain DeliveryChannel values")
                sender = self._terminal_channel_sender(channel)
                if sender is None:
                    raise TypeError("checkpoint delivery channels require an ExecutionVertexId sender")
                channel_inputs[sender] += 1
            if channel_inputs != inputs:
                raise ValueError("checkpoint input channel inventory does not match input vertex multiplicity")
            configured = frozenset(channels)
            if self._configured_input_channels is not None:
                self._retired_input_channels.update(self._configured_input_channels.difference(configured))
            self._retired_input_channels.difference_update(configured)
            # An exact lane explicitly restored after an earlier topology can
            # retain its terminal state; a new topology epoch is a new identity.
            candidates.update(self._retired_terminal_input_channels.intersection(configured))
            retained = candidates.intersection(configured)
        else:
            self._retired_input_channels.clear()
            remaining = inputs.copy()
            retained = set()
            for channel in sorted(candidates, key=repr):
                sender = self._terminal_channel_sender(channel)
                if sender is None or remaining[sender] <= 0:
                    continue
                retained.add(channel)
                remaining[sender] -= 1

        removed = candidates.difference(retained)
        self._retired_terminal_input_channels.update(removed)
        self._retired_terminal_input_channels.difference_update(retained)
        self._terminal_input_channels = retained
        self._configured_input_channels = configured
        self._rebuild_terminal_inputs()
        self._last_alignment_is_terminal = bool(inputs) and all(
            self._terminal_inputs[sender] >= count for sender, count in inputs.items()
        )

    def receive_eof(self, barrier: EndOfData) -> bool:
        """Mark a source's eof; return True once every source has sent eof."""
        if self._direct_input_vertex_ids is not None and self._last_aligned_id == barrier.id:
            return self._last_alignment_is_terminal
        self._eof_from_src[barrier.source_id] = True
        return all(self._eof_from_src.values())

    def barrier_to_forward(self, barrier: Barrier) -> Barrier:
        """Collapse mixed direct-input barriers to one downstream barrier."""
        if self._direct_input_vertex_ids is None or self._last_aligned_id != barrier.id:
            return barrier
        if self._last_alignment_is_terminal:
            if isinstance(barrier, EndOfData):
                return barrier
            forwarded = EndOfData(barrier.id, source_id=barrier.source_id)
        else:
            if not isinstance(barrier, EndOfData):
                return barrier
            forwarded = Barrier(barrier.id, source_id=barrier.source_id)
        forwarded.timestamp = barrier.timestamp
        return forwarded

    @property
    def last_alignment_is_terminal(self) -> bool:
        return self._last_alignment_is_terminal

    def is_resolved(self, barrier_id: int) -> bool:
        return barrier_id <= self._direct_resolved_through

    def reset_inflight_before(self, cutoff_barrier_id: int) -> int:
        """Drop partial alignment counts for barriers from a previous epoch.

        After a Tier-1 coordinator rebuild, barriers allocated in the previous epoch
        (id <= cutoff) will never be re-broadcast to completion, so their
        partial counts here would linger forever — a slow leak that accumulates
        one entry per orphan across every rebuild. Mirror the source-side
        source-state reclaim: the rebuilt coordinator reseeds ids strictly above the
        epoch floor, so every orphan count has a key <= cutoff. Returns the
        number reclaimed. Idempotent.
        """
        stale = [barrier_id for barrier_id in self._inflight if barrier_id <= cutoff_barrier_id]
        for barrier_id in stale:
            self._inflight.pop(barrier_id, None)
        coordinated = [barrier_id for barrier_id in self._coordinated_inflight if barrier_id <= cutoff_barrier_id]
        for barrier_id in coordinated:
            self._coordinated_inflight.pop(barrier_id, None)
            self._coordinated_channels.pop(barrier_id, None)
        self._last_coordinated_barrier_id = max(self._last_coordinated_barrier_id, cutoff_barrier_id)
        direct = [barrier_id for barrier_id in self._direct_inflight if barrier_id <= cutoff_barrier_id]
        for barrier_id in direct:
            self._direct_inflight.pop(barrier_id, None)
            self._direct_expected.pop(barrier_id, None)
        self._direct_resolved_through = max(self._direct_resolved_through, cutoff_barrier_id)
        return len(stale) + len(coordinated) + len(direct)

    def discard(self, barrier_id: int) -> int:
        """Drop one failed barrier without disturbing concurrent checkpoints."""

        removed = int(self._inflight.pop(barrier_id, None) is not None)
        removed += int(self._coordinated_inflight.pop(barrier_id, None) is not None)
        self._coordinated_channels.pop(barrier_id, None)
        self._last_coordinated_barrier_id = max(self._last_coordinated_barrier_id, barrier_id)
        removed += int(self._direct_inflight.pop(barrier_id, None) is not None)
        self._direct_expected.pop(barrier_id, None)
        self._direct_resolved_through = max(self._direct_resolved_through, barrier_id)
        return removed

    def abort(self, barrier_id: int) -> bool:
        """Discard one partial alignment and ignore late copies."""

        return bool(self.discard(barrier_id))

    def reconfigure(
        self,
        barrier_splits: dict[ExecutionVertexId, int],
        input_vertex_ids: Iterable[ExecutionVertexId] | None = None,
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        """Install a new topology after every old-topology barrier completed."""

        self.validate_reconfiguration()
        previous_eof = self._eof_from_src
        self._split = dict(barrier_splits)
        if input_vertex_ids is not None:
            inputs = tuple(input_vertex_ids)
            self._input_vertex_ids = Counter(inputs)
            self._direct_input_vertex_ids = Counter(inputs)
            self._reconfigure_terminal_inputs(self._direct_input_vertex_ids, input_channels)
        elif input_channels is not None:
            raise ValueError("checkpoint input channels require input vertex ids")
        self._eof_from_src = {source_id: previous_eof.get(source_id, False) for source_id in barrier_splits}

    def reconfigure_inputs(
        self,
        input_vertex_ids: Iterable[ExecutionVertexId],
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        self.validate_reconfiguration()
        inputs = tuple(input_vertex_ids)
        self._input_vertex_ids = Counter(inputs)
        direct_inputs = Counter(inputs)
        self._direct_input_vertex_ids = direct_inputs
        self._reconfigure_terminal_inputs(direct_inputs, input_channels)

    def validate_reconfiguration(self) -> None:
        if self._inflight or self._coordinated_inflight or self._direct_inflight:
            raise RuntimeError("cannot reconfigure barrier alignment while checkpoints are in flight")


class _CoordinatorClient:
    """Synchronous wrapper over the CheckpointCoordinator RPCs the strategy needs.

    Keeps blocking ``klein.get`` I/O out of the alignment/counting logic so each
    concern reads top-to-bottom.

    ``notify_complete`` has two modes. Synchronous (default): block on the ack,
    one coordinator round-trip per barrier on the alignment hot path. Async
    fire-and-reap (``async_notify``): fire the RPC, return immediately, and reap
    the in-flight refs on the NEXT notify — re-firing any that haven't completed.
    The coordinator's per-committer ack is idempotent, so a re-fired notify is
    counted at most once; reaping bounds the in-flight set so refs can't leak.
    """

    def __init__(self, coordinator: KleinActorHandle, vertex_id: Any, async_notify: bool = False) -> None:
        self._coordinator = coordinator
        self._vertex_id = vertex_id
        self._async_notify = async_notify
        self._pending_notifies: dict[int, Any] = {}

    def register_barrier(self, *, force: bool) -> CheckpointRegistration:
        return klein.get(self._coordinator.register_checkpoint(self._vertex_id, force=force))

    def source_checkpoint_started(self, barrier_id: int, *, is_eof: bool) -> bool:
        return klein.get(
            self._coordinator.source_checkpoint_started(
                barrier_id,
                self._vertex_id,
                is_eof=is_eof,
            )
        )

    def notify_complete(self, barrier_id: int) -> None:
        if not self._async_notify:
            klein.get(self._coordinator.notify_checkpoint_aligned(barrier_id, self._vertex_id))
            return
        self._reap_pending()
        self._pending_notifies[barrier_id] = self._coordinator.notify_checkpoint_aligned(barrier_id, self._vertex_id)

    def _reap_pending(self) -> None:
        if not self._pending_notifies:
            return
        for barrier_id in list(self._pending_notifies):
            ref = self._pending_notifies[barrier_id]
            try:
                klein.get(ref, timeout=0)
                self._pending_notifies.pop(barrier_id, None)
            except Exception:
                self._pending_notifies[barrier_id] = self._coordinator.notify_checkpoint_aligned(
                    barrier_id, self._vertex_id
                )

    def flush_pending(self) -> None:
        while self._pending_notifies:
            for barrier_id in list(self._pending_notifies):
                ref = self._pending_notifies.pop(barrier_id)
                try:
                    klein.get(ref)
                except Exception:
                    self._pending_notifies[barrier_id] = self._coordinator.notify_checkpoint_aligned(
                        barrier_id, self._vertex_id
                    )

    def source_state(self) -> SourceCheckpointEntry | None:
        return klein.get(self._coordinator.source_state(self._vertex_id))

    def register_operator_state(
        self,
        barrier_id: int,
        reference: StateSnapshotReference,
    ) -> bool:
        return klein.get(
            self._coordinator.register_operator_state(
                barrier_id,
                self._vertex_id,
                reference,
            )
        )

    def register_sink_committable(self, barrier_id: int, committable: SinkCommittable) -> bool:
        return klein.get(
            self._coordinator.register_sink_committable(
                barrier_id,
                self._vertex_id,
                committable,
            )
        )

    def register_operator_metrics(self, barrier_id: int, metrics: dict[str, int | float]) -> bool:
        return klein.get(
            self._coordinator.register_operator_checkpoint_metrics(
                barrier_id,
                self._vertex_id,
                metrics,
            )
        )

    async def latest_operator_states_async(self) -> tuple[StateSnapshotReference, ...]:
        return tuple(await klein.aget(self._coordinator.latest_operator_states(self._vertex_id)))

    async def durable_operator_states_async(self) -> tuple[StateSnapshotReference, ...]:
        return tuple(await klein.aget(self._coordinator.durable_operator_states(self._vertex_id)))

    async def rescale_operator_states_async(self, operation_id: str) -> tuple[StateSnapshotReference, ...]:
        return tuple(
            await klein.aget(
                self._coordinator.restore_operator_rescale_states(
                    operation_id,
                    self._vertex_id,
                )
            )
        )

    async def source_state_async(self) -> SourceCheckpointEntry | None:
        return await klein.aget(self._coordinator.source_state(self._vertex_id))


class AlignedCheckpointStrategy(CheckpointStrategy):
    """Checkpoint strategy composed of an aligner, a coordinator client,
    a trigger, and source-progress tracking — each a single responsibility.
    """

    def __init__(
        self,
        coordinator: KleinActorHandle,
        barrier_splits: dict[ExecutionVertexId, int],
        vertex_id: Any,
        operator_type: OperatorType,
        config: Configuration | None = None,
        is_committer: bool = False,
        synchronous_notify: bool = False,
        metric_group: MetricGroup | None = None,
        input_vertex_ids: Iterable[ExecutionVertexId] | None = None,
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        self._vertex_id = vertex_id
        self._operator_type = operator_type
        self._is_committer = is_committer
        self._aligner = _BarrierAligner(barrier_splits, input_vertex_ids, input_channels)
        config = config if config is not None else Configuration()
        async_notify = config.get(CheckpointOptions.ASYNC_NOTIFY) and not synchronous_notify
        self._coordinator = _CoordinatorClient(coordinator, vertex_id, async_notify=async_notify)
        self._trigger = self._resolve_trigger(operator_type, config)
        self._alignment_started_at: dict[tuple[int, Any], float] = {}
        self._alignment_duration = (
            metric_group.builtin_histogram(KleinMetrics.CHECKPOINT_ALIGNMENT_DURATION_MS)
            if metric_group is not None
            else None
        )
        self._last_alignment_duration_ms = 0.0

    def on_barrier_received(
        self,
        barrier: Barrier,
        on_barrier_aligned: Callable | None = None,
        sender_vertex_id: ExecutionVertexId | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> bool:
        alignment_key = (
            barrier.id,
            None if barrier.coordinated or sender_vertex_id is not None else barrier.source_id,
        )
        if not barrier.coordinated and sender_vertex_id is not None and self._aligner.is_resolved(barrier.id):
            return False
        self._alignment_started_at.setdefault(alignment_key, time.monotonic())
        if not self._aligner.receive(barrier, sender_vertex_id, delivery_channel):
            if barrier.coordinated and self._aligner.coordinated_barrier_finalized(barrier.id):
                self._alignment_started_at.pop(alignment_key, None)
            return False
        started_at = self._alignment_started_at.pop(alignment_key)
        self._last_alignment_duration_ms = max(0.0, (time.monotonic() - started_at) * 1_000)
        if self._alignment_duration is not None:
            self._alignment_duration.observe(self._last_alignment_duration_ms)
        if on_barrier_aligned:
            on_barrier_aligned()
        if self._is_committer:
            self._coordinator.notify_complete(barrier.id)
            if self._aligner.last_alignment_is_terminal or isinstance(barrier, EndOfData):
                self._coordinator.flush_pending()
        return True

    def on_eof_received(self, barrier: EndOfData) -> bool:
        return self._aligner.receive_eof(barrier)

    def reset_inflight_before(self, cutoff_barrier_id: int) -> int:
        removed = self._aligner.reset_inflight_before(cutoff_barrier_id)
        for key in [key for key in self._alignment_started_at if key[0] <= cutoff_barrier_id]:
            self._alignment_started_at.pop(key, None)
        return removed

    def discard_checkpoint(self, barrier_id: int) -> int:
        removed = self._aligner.discard(barrier_id)
        for key in [key for key in self._alignment_started_at if key[0] == barrier_id]:
            self._alignment_started_at.pop(key, None)
        return removed

    def abort_checkpoint(self, barrier_id: int) -> bool:
        return bool(self.discard_checkpoint(barrier_id))

    def reconfigure_barrier_split(
        self,
        barrier_splits: dict[ExecutionVertexId, int],
        input_vertex_ids: Iterable[ExecutionVertexId] | None = None,
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        self._aligner.reconfigure(barrier_splits, input_vertex_ids, input_channels)

    def reconfigure_barrier_inputs(
        self,
        input_vertex_ids: Iterable[ExecutionVertexId],
        input_channels: Iterable[DeliveryChannel] | None = None,
    ) -> None:
        self._aligner.reconfigure_inputs(input_vertex_ids, input_channels)

    def validate_barrier_reconfiguration(self) -> None:
        self._aligner.validate_reconfiguration()

    def barrier_to_forward(self, barrier: Barrier) -> Barrier:
        return self._aligner.barrier_to_forward(barrier)

    @property
    def last_alignment_is_terminal(self) -> bool:
        return self._aligner.last_alignment_is_terminal

    def restore_source_state(self) -> SourceCheckpointEntry | None:
        return self._coordinator.source_state()

    async def restore_source_state_async(self) -> SourceCheckpointEntry | None:
        return await self._coordinator.source_state_async()

    def register_operator_state(
        self,
        barrier_id: int,
        reference: StateSnapshotReference,
    ) -> bool:
        return self._coordinator.register_operator_state(barrier_id, reference)

    def register_sink_committable(self, barrier_id: int, committable: SinkCommittable) -> bool:
        return self._coordinator.register_sink_committable(barrier_id, committable)

    def register_operator_metrics(self, barrier_id: int, metrics: dict[str, int | float]) -> bool:
        return self._coordinator.register_operator_metrics(barrier_id, metrics)

    @property
    def last_alignment_duration_ms(self) -> float:
        return self._last_alignment_duration_ms

    async def restore_operator_states_async(self) -> tuple[StateSnapshotReference, ...]:
        """Return every previous subtask fragment for keyed rescaling."""

        return await self._coordinator.latest_operator_states_async()

    async def restore_durable_operator_states_async(self) -> tuple[StateSnapshotReference, ...]:
        return await self._coordinator.durable_operator_states_async()

    async def restore_rescale_operator_states_async(
        self,
        operation_id: str,
    ) -> tuple[StateSnapshotReference, ...]:
        return await self._coordinator.rescale_operator_states_async(operation_id)

    def should_trigger(self, record_emitted: bool, record_count: int = 1) -> bool:
        if self._operator_type != OperatorType.SOURCE:
            return False
        return self._trigger.should_trigger(record_emitted, record_count)

    def generate_next_barrier(
        self,
        is_eof: bool = False,
        *,
        force: bool = False,
        checkpoint_id: int | None = None,
    ) -> Barrier | EndOfData | None:
        registration: CheckpointRegistration | None = None
        barrier_id = checkpoint_id
        if barrier_id is None:
            registration = self._coordinator.register_barrier(force=is_eof or force)
            if registration.barrier_id is None:
                logger.debug("Checkpoint not triggered: %s", registration.reason)
                return None
            barrier_id = registration.barrier_id
        if not self._coordinator.source_checkpoint_started(barrier_id, is_eof=is_eof):
            logger.debug("Checkpoint %s was canceled before this source started it", barrier_id)
            return None
        coordinated = registration.coordinated if registration is not None else False
        # A source can reach EOF while a post-rescale epoch is being armed.
        # Keep that shared cut homogeneous; after it becomes durable the source
        # emits its ordinary, independently identified EndOfData barrier.
        if is_eof and not coordinated:
            return EndOfData(
                barrier_id,
                source_id=self._vertex_id,
                coordinated=False,
            )
        return Barrier(
            barrier_id,
            source_id=self._vertex_id,
            coordinated=coordinated,
        )

    def reset_trigger(self) -> None:
        if self._trigger is not None:
            self._trigger.reset()

    @staticmethod
    def _resolve_trigger(operator_type: OperatorType, config: Configuration) -> CheckpointTrigger | None:
        if operator_type != OperatorType.SOURCE:
            return None
        records = config.get(CheckpointTriggerOptions.INTERVAL_RECORDS)
        seconds = config.get(CheckpointTriggerOptions.INTERVAL_DURATION).total_seconds()
        return CheckpointTrigger(interval_records=records, interval_seconds=seconds)
