# SPDX-License-Identifier: Apache-2.0
"""Composition of every downstream edge owned by one task."""

import asyncio
from collections.abc import Callable, Sequence

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

    def flush(self, force: bool = False) -> None:
        self._ensure_open()
        for edge in self._edges:
            edge.flush(force=force)

    def _on_close(self) -> None:
        first_error: Exception | None = None
        for edge in self._edges:
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

    def configure_replay(
        self,
        enabled: bool,
        sender_vertex_id=None,
        max_bytes: int = 0,
        *,
        sender_task_name: str | None = None,
    ) -> None:
        for edge_index, edge in enumerate(self._edges):
            edge.configure_replay(
                enabled,
                sender_vertex_id,
                max_bytes,
                sender_task_name=sender_task_name,
                edge_index=edge_index,
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
