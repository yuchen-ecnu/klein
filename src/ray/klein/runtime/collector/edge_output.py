# SPDX-License-Identifier: Apache-2.0
"""Routing, batching and delivery for one logical output edge."""

import asyncio
from collections.abc import Callable, Sequence
from enum import Enum

import ray
import ray.klein as klein
from ray.klein._internal.memory import estimate_retained_size
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.collector.delivery_command import (
    BarrierCommand,
    ControlCommand,
    DataCommand,
    EdgeCommand,
    ReplayCommand,
)
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.collector.downstream_batcher import DownstreamBatcher
from ray.klein.runtime.collector.downstream_sender import DownstreamSender
from ray.klein.runtime.collector.record_router import RecordRouter
from ray.klein.runtime.message import Barrier, Record, StreamControl
from ray.klein.runtime.partitioning.partitioner import Partitioner


class DeliveryMode(Enum):
    """Where downstream commands are executed."""

    INLINE = "inline"
    PIPELINED = "pipelined"


class EdgeOutput:
    """Route, batch and deliver records for exactly one logical output edge."""

    def __init__(
        self,
        target_tasks: list[KleinActorHandle],
        partitioner: Partitioner,
        *,
        control_targets: tuple[int, ...],
        output_buffer_max_rows: int,
        target_task_names: list[str],
        put_timeout: float,
        namespace: str,
        delivery_mode: DeliveryMode,
    ) -> None:
        if output_buffer_max_rows <= 0:
            raise ValueError("output buffer max rows must be greater than zero")
        if len(target_tasks) != len(target_task_names):
            raise ValueError(f"target task/name counts must match: {len(target_tasks)} != {len(target_task_names)}")
        if not namespace:
            raise ValueError("output namespace cannot be empty")
        self._target_task_names = tuple(target_task_names)
        self._buffer_limit = output_buffer_max_rows
        self._buffer_byte_limit = 0
        self._object_store_threshold_bytes = 0
        self._delivery_mode = delivery_mode
        self._router = RecordRouter(partitioner, len(target_tasks), control_targets)
        self._journal = DeliveryJournal(len(target_tasks))
        self._sender = DownstreamSender(
            target_tasks,
            target_task_names,
            control_targets,
            self._journal,
            put_timeout,
            namespace,
        )
        self._batcher: DownstreamBatcher | None = None
        self._pending: list[EdgeCommand] = []
        self._buffered_rows = 0
        self._buffered_bytes = 0
        self._task_name = "<unopened>"

    def open(self, runtime_context: RuntimeContext) -> None:
        if self._batcher is not None:
            raise RuntimeError("EdgeOutput is already open")
        self._router.open(runtime_context)
        batch_size = runtime_context.config.get(PipelineOptions.INTERNAL_BATCH_SIZE)
        batch_max_rows = runtime_context.config.get(PipelineOptions.INTERNAL_BATCH_MAX_ROWS)
        batch_max_bytes = runtime_context.config.get(PipelineOptions.INTERNAL_BATCH_MAX_BYTES)
        self._buffer_byte_limit = runtime_context.config.get(PipelineOptions.OUTPUT_BUFFER_MAX_BYTES)
        if self._buffer_byte_limit <= 0:
            raise ValueError("pipeline.output-buffer.max-bytes must be greater than zero")
        self._object_store_threshold_bytes = runtime_context.config.get(
            PipelineOptions.TRANSPORT_OBJECT_STORE_THRESHOLD_BYTES
        )
        if self._object_store_threshold_bytes < 0:
            raise ValueError("pipeline.transport.object-store-threshold-bytes cannot be negative")
        batch_timeout = runtime_context.runtime_info.batch_timeout
        idle_timeout = float(batch_timeout) if batch_timeout else 3.0
        self._batcher = DownstreamBatcher(
            len(self._target_task_names),
            batch_size,
            idle_timeout,
            max_rows=batch_max_rows,
            max_bytes=batch_max_bytes,
        )
        self._task_name = runtime_context.task_name

    def accept(self, record: Record) -> None:
        batcher = self._require_batcher()
        if isinstance(record, Barrier):
            self._drain_batcher(force=True)
            self._dispatch_or_buffer(BarrierCommand(record))
            return
        if isinstance(record, StreamControl):
            self._drain_batcher(force=True)
            self._dispatch_or_buffer(ControlCommand(record))
            return
        ready: list[DataCommand] = []
        for target, routed in self._router.route(record):
            self._reserve(routed)
            batcher.append(target, routed)
            records = batcher.take_full(target)
            if records:
                ready.append(self._data_command(target, records))
        self._dispatch_data_commands(ready)

    def flush(self, force: bool = False) -> None:
        self._require_batcher()
        self._drain_batcher(force=force)

    def close(self) -> None:
        batcher = self._batcher
        if batcher is None:
            return
        # The async emit worker has already drained before task teardown. Any
        # final partial batch is sent inline, after those earlier commands.
        pending = self.take_pending_commands()
        for target, records in batcher.drain(force=True):
            pending.append(self._data_command(target, records))
            self._release(records)
        try:
            for command in pending:
                self._send_sync(command)
        finally:
            self._batcher = None

    def take_pending_commands(self) -> list[EdgeCommand]:
        """Transfer executor-owned commands after the executor becomes idle."""
        commands = self._pending
        self._pending = []
        for command in commands:
            if isinstance(command, DataCommand):
                self._release(command.records)
        return commands

    async def send_commands(self, commands: Sequence[EdgeCommand]) -> None:
        """Send independent target lanes concurrently with ordered control fences.

        Data/replay commands retain FIFO order per initial target. A barrier or
        stream-control command waits for every earlier target lane, is broadcast,
        and only then allows later data to start. DownstreamSender additionally
        serializes each *actual* retry target so rerouting cannot race sequences.
        """
        lanes: dict[int, list[EdgeCommand]] = {}
        for command in commands:
            if isinstance(command, DataCommand | ReplayCommand):
                lanes.setdefault(command.target, []).append(command)
                continue
            await self._send_lanes(lanes)
            lanes = {}
            await self._send_async(command)
        await self._send_lanes(lanes)

    async def _send_lanes(self, lanes: dict[int, list[EdgeCommand]]) -> None:
        shared_payloads = self._shared_wire_payloads(lanes)

        async def send_lane(commands: Sequence[EdgeCommand]) -> None:
            for command in commands:
                wire_records = shared_payloads.get(id(command))
                if wire_records is None:
                    await self._send_async(command)
                else:
                    await self._send_async_shared(command, wire_records)

        if lanes:
            await asyncio.gather(*(send_lane(commands) for commands in lanes.values()))

    def _shared_wire_payloads(self, lanes: dict[int, list[EdgeCommand]]) -> dict[int, object]:
        """Put duplicated large broadcast batches into the Object Store once."""
        if not ray.is_initialized() or klein.is_debug_mode():
            return {}
        by_identity: dict[tuple[int, ...], list[DataCommand | ReplayCommand]] = {}
        for commands in lanes.values():
            for command in commands:
                if isinstance(command, DataCommand | ReplayCommand):
                    by_identity.setdefault(tuple(map(id, command.records)), []).append(command)
        shared: dict[int, object] = {}
        for commands in by_identity.values():
            records = commands[0].records
            if len(commands) < 2 or estimate_retained_size(records) < self._object_store_threshold_bytes:
                continue
            payload_ref = ray.put(records)
            shared.update((id(command), payload_ref) for command in commands)
        return shared

    async def _send_async_shared(self, command: EdgeCommand, wire_records: object) -> None:
        if isinstance(command, DataCommand):
            await self._sender.send_async(
                command.target,
                command.records,
                command.retry_ring,
                wire_records=wire_records,
            )
        elif isinstance(command, ReplayCommand):
            await self._sender.send_async(
                command.target,
                command.records,
                (command.target,),
                is_replay=True,
                replay_sequence=command.sequence,
                wire_records=wire_records,
            )
        else:
            raise TypeError(f"only data commands can share a wire payload, got {type(command).__name__}")

    def replay_commands_for(self, downstream_name: str) -> list[EdgeCommand]:
        try:
            target = self._target_task_names.index(downstream_name)
        except ValueError:
            return []
        return [ReplayCommand(target, sequence, records) for sequence, records in self._journal.pending_for(target)]

    def refresh_downstream(self, downstream_name: str) -> None:
        self._sender.refresh_by_name(downstream_name)

    def ensure_quiescent(self) -> None:
        """Reject a topology swap while this edge still owns unsent data."""

        if self._pending or self._buffered_rows or self._buffered_bytes or self._sender.inflight_requests:
            raise RuntimeError("cannot replace an output edge while delivery is in flight")

    def configure_replay(
        self,
        enabled: bool,
        sender_vertex_id=None,
        max_bytes: int = 0,
        *,
        sender_task_name: str | None = None,
        edge_index: int | None = None,
        topology_epoch: str | None = None,
    ) -> None:
        self._journal.configure(
            enabled,
            sender_vertex_id,
            max_bytes,
            sender_task_name=sender_task_name,
            edge_index=edge_index,
            topology_epoch=topology_epoch,
        )

    def acknowledge(self, target_index: int, forwarded_sequence: int) -> None:
        self._journal.acknowledge(target_index, forwarded_sequence)

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
        self._journal.attach_observers(replay_size_observer, replay_bytes_observer)
        self._sender.attach_backpressure_metrics(backpressure_events, backpressure_duration_ms)
        transport_metrics = (
            transport_requests,
            transport_batch_rows,
            transport_batch_bytes,
            transport_send_duration_ms,
            transport_inflight_observer,
        )
        if all(metric is not None for metric in transport_metrics):
            self._sender.attach_transport_metrics(*transport_metrics)

    @property
    def replay_buffered_records(self) -> int:
        return self._journal.buffered_record_count

    @property
    def replay_buffered_bytes(self) -> int:
        return self._journal.buffered_bytes

    @property
    def backpressure_events(self) -> int:
        return self._sender.backpressure_events

    @property
    def backpressure_duration_ns(self) -> int:
        return self._sender.backpressure_duration_ns

    @property
    def inflight_requests(self) -> int:
        return self._sender.inflight_requests

    def _drain_batcher(self, force: bool) -> None:
        self._dispatch_data_commands(
            [self._data_command(target, records) for target, records in self._require_batcher().drain(force=force)]
        )

    def _data_command(self, target: int, records: tuple[Record, ...]) -> DataCommand:
        return DataCommand(target, self._router.retry_ring(target), records)

    def _dispatch_data_commands(self, commands: Sequence[DataCommand]) -> None:
        if self._delivery_mode is DeliveryMode.PIPELINED:
            self._pending.extend(commands)
            return
        lanes = {command.target: [command] for command in commands}
        shared_payloads = self._shared_wire_payloads(lanes)
        for command in commands:
            wire_records = shared_payloads.get(id(command))
            if wire_records is None:
                self._send_sync(command)
            else:
                self._sender.send_sync(
                    command.target,
                    command.records,
                    command.retry_ring,
                    wire_records=wire_records,
                )
            self._release(command.records)

    def _dispatch_or_buffer(self, command: EdgeCommand) -> None:
        if self._delivery_mode is DeliveryMode.PIPELINED:
            self._pending.append(command)
            return
        self._send_sync(command)
        if isinstance(command, DataCommand):
            self._release(command.records)

    def _send_sync(self, command: EdgeCommand) -> None:
        if isinstance(command, DataCommand):
            self._sender.send_sync(command.target, command.records, command.retry_ring)
        elif isinstance(command, ReplayCommand):
            self._sender.send_sync(
                command.target,
                command.records,
                (command.target,),
                is_replay=True,
                replay_sequence=command.sequence,
            )
        elif isinstance(command, BarrierCommand):
            self._sender.send_barrier_sync(command.barrier)
        elif isinstance(command, ControlCommand):
            self._sender.send_control_sync(command.control)
        else:
            raise TypeError(f"EdgeOutput cannot send {type(command).__name__}")

    async def _send_async(self, command: EdgeCommand) -> None:
        if isinstance(command, DataCommand):
            await self._sender.send_async(command.target, command.records, command.retry_ring)
        elif isinstance(command, ReplayCommand):
            await self._sender.send_async(
                command.target,
                command.records,
                (command.target,),
                is_replay=True,
                replay_sequence=command.sequence,
            )
        elif isinstance(command, BarrierCommand):
            await self._sender.send_barrier_async(command.barrier)
        elif isinstance(command, ControlCommand):
            await self._sender.send_control_async(command.control)
        else:
            raise TypeError(f"EdgeOutput cannot send {type(command).__name__}")

    def _reserve(self, record: Record) -> None:
        rows = self._record_rows(record)
        size_bytes = self._record_bytes(record)
        next_rows = self._buffered_rows + rows
        next_bytes = self._buffered_bytes + size_bytes
        # One oversized columnar block is exclusive so the pipeline can still
        # make progress. No other buffered output may accompany it.
        has_buffered_output = self._buffered_rows > 0 or self._buffered_bytes > 0
        if has_buffered_output and next_rows > self._buffer_limit:
            raise BufferError(
                f"{self._task_name} output edge would retain {next_rows} rows, above configured "
                f"pipeline.output-buffer.max-rows={self._buffer_limit}"
            )
        if has_buffered_output and next_bytes > self._buffer_byte_limit:
            raise BufferError(
                f"{self._task_name} output edge would retain {next_bytes} bytes, above configured "
                f"pipeline.output-buffer.max-bytes={self._buffer_byte_limit}"
            )
        self._buffered_rows = next_rows
        self._buffered_bytes = next_bytes

    def _release(self, records: Sequence[Record]) -> None:
        self._buffered_rows -= sum(self._record_rows(record) for record in records)
        self._buffered_bytes -= sum(self._record_bytes(record) for record in records)
        if self._buffered_rows < 0:
            raise AssertionError("output edge row accounting became negative")
        if self._buffered_bytes < 0:
            raise AssertionError("output edge byte accounting became negative")

    @staticmethod
    def _record_rows(record: Record) -> int:
        rows = 1 if record.num_rows is None else record.num_rows
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError(f"record has invalid row count: {rows!r}")
        return rows

    @staticmethod
    def _record_bytes(record: Record) -> int:
        return estimate_retained_size(record)

    def _require_batcher(self) -> DownstreamBatcher:
        if self._batcher is None:
            raise RuntimeError("EdgeOutput is not open")
        return self._batcher
