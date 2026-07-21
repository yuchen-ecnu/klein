# SPDX-License-Identifier: Apache-2.0
"""Composition of every downstream edge owned by one task."""

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from ray.klein._internal.logging import get_logger
from ray.klein._internal.memory import estimate_retained_size
from ray.klein.api.collector import Collector
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.collector.delivery_command import DeliveryCommand, EdgeCommand
from ray.klein.runtime.collector.edge_output import EdgeOutput
from ray.klein.runtime.message import Barrier, Record, StreamControl

logger = get_logger(__name__)


@dataclass(slots=True)
class _EdgeSwap:
    operation_id: str
    previous_edges: tuple[EdgeOutput, ...]
    replacement_edges: tuple[EdgeOutput, ...]
    changed_indices: tuple[int, ...]
    active: bool = False


class TaskOutput(Collector):
    """The complete output boundary of one task, composed from independent edges."""

    def __init__(self, edges: Sequence[EdgeOutput]) -> None:
        super().__init__()
        if not edges:
            raise ValueError("TaskOutput requires at least one edge")
        self._edges = tuple(edges)
        self._records_out = 0
        self._bytes_out = 0
        self._records_out_metric: Counter | None = None
        self._bytes_out_metric: Counter | None = None
        self._edge_swap: _EdgeSwap | None = None

    def _on_open(self, runtime_context: RuntimeContext) -> None:
        if runtime_context.metric_group is not None:
            self._records_out_metric = runtime_context.metric_group.builtin_counter(KleinMetrics.RECORDS_OUT)
            self._bytes_out_metric = runtime_context.metric_group.builtin_counter(KleinMetrics.BYTES_OUT)
        opened: list[EdgeOutput] = []
        try:
            for edge in self._edges:
                edge.open(runtime_context)
                opened.append(edge)
        except BaseException:
            for edge in reversed(opened):
                try:
                    edge.close()
                except Exception:
                    logger.exception("Failed to roll back output edge after open failed")
            self._records_out_metric = None
            self._bytes_out_metric = None
            raise

    def collect(self, record: Record) -> None:
        self._ensure_open()
        if not isinstance(record, Barrier | StreamControl):
            rows = EdgeOutput._record_rows(record)
            size_bytes = estimate_retained_size(record)
            self._records_out += rows
            self._bytes_out += size_bytes
            if self._records_out_metric is not None:
                self._records_out_metric.inc(rows)
            if self._bytes_out_metric is not None:
                self._bytes_out_metric.inc(size_bytes)
        for edge_index, edge in enumerate(self._edges):
            routed_record = record if edge_index == 0 or isinstance(record, Barrier | StreamControl) else record.fork()
            edge.accept(routed_record)

    def collect_to_edges(self, record: StreamControl, edge_indices: Sequence[int]) -> None:
        """Emit an internal control fence on selected logical output edges."""

        self._ensure_open()
        selected = tuple(edge_indices)
        if not selected:
            raise ValueError("at least one output edge must be selected")
        if len(set(selected)) != len(selected):
            raise ValueError("output edge indices cannot contain duplicates")
        for edge_index in selected:
            if isinstance(edge_index, bool) or not isinstance(edge_index, int):
                raise TypeError("output edge indices must be integers")
            if edge_index < 0 or edge_index >= len(self._edges):
                raise IndexError(f"output edge index {edge_index} is out of range")
            self._edges[edge_index].accept(record)

    def flush(self, force: bool = False) -> None:
        self._ensure_open()
        for edge in self._edges:
            edge.flush(force=force)

    def _on_close(self) -> None:
        first_error: Exception | None = None
        edges = list(self._edges)
        if self._edge_swap is not None:
            edges.extend(self._edge_swap.previous_edges)
            edges.extend(self._edge_swap.replacement_edges)
            self._edge_swap = None
        unique_edges = tuple(dict.fromkeys(edges))
        for edge in unique_edges:
            try:
                edge.close()
            except Exception as error:
                if first_error is None:
                    first_error = error
                else:
                    logger.exception("Failed to close output edge")
        self._records_out_metric = None
        self._bytes_out_metric = None
        if first_error is not None:
            raise first_error.with_traceback(first_error.__traceback__)

    def abort_delivery(self) -> None:
        """Fence every live or staged output edge before a force kill."""

        edges = list(self._edges)
        if self._edge_swap is not None:
            edges.extend(self._edge_swap.previous_edges)
            edges.extend(self._edge_swap.replacement_edges)
        for edge in dict.fromkeys(edges):
            edge.abort_delivery()

    @property
    def records_out(self) -> int:
        return self._records_out

    @property
    def bytes_out(self) -> int:
        return self._bytes_out

    def take_pending_commands(self) -> list[DeliveryCommand]:
        commands: list[DeliveryCommand] = []
        for edge_index, edge in enumerate(self._edges):
            commands.extend(DeliveryCommand(edge_index, command) for command in edge.take_pending_commands())
        return commands

    async def send_commands(self, commands: Sequence[DeliveryCommand]) -> None:
        per_edge: list[list[EdgeCommand]] = [[] for _ in self._edges]
        for command in commands:
            if not isinstance(command, DeliveryCommand):
                raise TypeError(f"TaskOutput cannot send {type(command).__name__}")
            per_edge[command.edge_index].append(command.command)
        await asyncio.gather(
            *(
                edge.send_commands(edge_commands)
                for edge, edge_commands in zip(self._edges, per_edge, strict=True)
                if edge_commands
            )
        )

    def replay_commands_for(self, downstream_name: str) -> list[DeliveryCommand]:
        commands: list[DeliveryCommand] = []
        for edge_index, edge in enumerate(self._edges):
            commands.extend(
                DeliveryCommand(edge_index, command) for command in edge.replay_commands_for(downstream_name)
            )
        return commands

    def refresh_downstream(self, downstream_name: str) -> None:
        for edge in self._edges:
            edge.refresh_downstream(downstream_name)

    def replace_edges(self, edges: Sequence[EdgeOutput | None]) -> None:
        """Immediately replace selected routing edges at a quiescent cut.

        ``None`` retains the existing edge, including its delivery journal and
        sequence epoch. This matters for fan-out operators where only the edge
        leading to the resized target changed.
        """

        operation_id = "task-output-immediate-edge-swap"
        self.prepare_edge_swap(operation_id, edges)
        self.activate_edge_swap(operation_id)
        self.commit_edge_swap(operation_id)

    def prepare_edge_swap(
        self,
        operation_id: str,
        edges: Sequence[EdgeOutput | None],
    ) -> None:
        """Open replacement edges while retaining the exact old routing state."""

        context = self._ensure_open()
        if self._edge_swap is not None:
            if self._edge_swap.operation_id == operation_id:
                return
            raise RuntimeError(f"edge swap {self._edge_swap.operation_id} is already prepared")
        requested = tuple(edges)
        if len(requested) != len(self._edges):
            raise ValueError("runtime rescale cannot change an operator's logical output edge count")
        replacements = tuple(
            current if replacement is None else replacement
            for current, replacement in zip(self._edges, requested, strict=True)
        )
        changed = [
            (index, current, replacement)
            for index, (current, replacement) in enumerate(zip(self._edges, requested, strict=True))
            if replacement is not None
        ]
        for _index, edge, _replacement in changed:
            edge.ensure_quiescent()
        opened: list[EdgeOutput] = []
        try:
            for _index, _current, edge in changed:
                edge.open(context)
                opened.append(edge)
        except BaseException:
            for edge in reversed(opened):
                try:
                    edge.close()
                except Exception:
                    logger.exception("Failed to close a prepared replacement edge")
            raise
        self._edge_swap = _EdgeSwap(
            operation_id,
            self._edges,
            replacements,
            tuple(index for index, _current, _replacement in changed),
        )

    def activate_edge_swap(self, operation_id: str) -> None:
        """Expose prepared routes without discarding the rollback journal."""

        swap = self._require_edge_swap(operation_id)
        if swap.active:
            return
        self._edges = swap.replacement_edges
        swap.active = True

    def rollback_edge_swap(self, operation_id: str) -> bool:
        """Restore the original edge objects, journals, epochs, and sequences."""

        swap = self._edge_swap
        if swap is None:
            return False
        if swap.operation_id != operation_id:
            raise RuntimeError(f"edge swap {swap.operation_id} does not belong to {operation_id}")
        if swap.active:
            self._edges = swap.previous_edges
        self._edge_swap = None
        for index in swap.changed_indices:
            try:
                swap.replacement_edges[index].close()
            except Exception:
                logger.exception("Failed to close a rolled-back replacement edge")
        return True

    def commit_edge_swap(self, operation_id: str) -> bool:
        """Discard retained old routes after the topology commit point."""

        swap = self._edge_swap
        if swap is None:
            return False
        if swap.operation_id != operation_id:
            raise RuntimeError(f"edge swap {swap.operation_id} does not belong to {operation_id}")
        if not swap.active:
            raise RuntimeError(f"edge swap {operation_id} has not been activated")
        # Clearing the transaction is the irreversible point. Closing an old
        # edge is cleanup only and must never turn a committed route into a
        # rollback attempt.
        self._edge_swap = None
        for index in swap.changed_indices:
            try:
                swap.previous_edges[index].close()
            except Exception:
                logger.exception("Failed to close a committed old edge")
        return True

    def _require_edge_swap(self, operation_id: str) -> _EdgeSwap:
        swap = self._edge_swap
        if swap is None:
            raise RuntimeError(f"edge swap {operation_id} has not been prepared")
        if swap.operation_id != operation_id:
            raise RuntimeError(f"edge swap {swap.operation_id} does not belong to {operation_id}")
        return swap

    def configure_replay(
        self,
        enabled: bool,
        sender_vertex_id=None,
        max_bytes: int = 0,
        *,
        sender_task_name: str | None = None,
        topology_epochs: Sequence[str | None] | None = None,
    ) -> None:
        epochs = tuple(topology_epochs or (None,) * len(self._edges))
        if len(epochs) != len(self._edges):
            raise ValueError("topology epoch count must match output edge count")
        for edge_index, edge in enumerate(self._edges):
            edge.configure_replay(
                enabled,
                sender_vertex_id,
                max_bytes,
                sender_task_name=sender_task_name,
                edge_index=edge_index,
                topology_epoch=epochs[edge_index],
            )

    def acknowledge_delivery(self, edge_index: int, target_index: int, forwarded_sequence: int) -> None:
        self._edges[edge_index].acknowledge(target_index, forwarded_sequence)

    def attach_runtime_metrics(
        self,
        replay_size_observer: Callable[[int], None],
        replay_bytes_observer: Callable[[int], None],
        backpressure_events: Counter,
        backpressure_duration_ms: Histogram,
        *,
        transport_requests: Counter | None = None,
        transport_batch_rows: Histogram | None = None,
        transport_batch_bytes: Histogram | None = None,
        transport_send_duration_ms: Histogram | None = None,
        transport_inflight_observer: Callable[[int], None] | None = None,
    ) -> None:
        def publish_total(_value: int) -> None:
            replay_size_observer(self.replay_buffered_records)

        def publish_total_bytes(_value: int) -> None:
            replay_bytes_observer(self.replay_buffered_bytes)

        def publish_total_inflight(_value: int) -> None:
            if transport_inflight_observer is not None:
                transport_inflight_observer(sum(edge.inflight_requests for edge in self._edges))

        for edge in self._edges:
            edge.attach_runtime_metrics(
                publish_total,
                publish_total_bytes,
                backpressure_events,
                backpressure_duration_ms,
                transport_requests=transport_requests,
                transport_batch_rows=transport_batch_rows,
                transport_batch_bytes=transport_batch_bytes,
                transport_send_duration_ms=transport_send_duration_ms,
                transport_inflight_observer=publish_total_inflight,
            )

    @property
    def replay_buffered_records(self) -> int:
        return sum(edge.replay_buffered_records for edge in self._edges)

    @property
    def replay_buffered_bytes(self) -> int:
        return sum(edge.replay_buffered_bytes for edge in self._edges)

    @property
    def backpressure_events(self) -> int:
        return sum(edge.backpressure_events for edge in self._edges)

    @property
    def backpressure_duration_ns(self) -> int:
        return sum(edge.backpressure_duration_ns for edge in self._edges)
