# SPDX-License-Identifier: Apache-2.0
import time
from collections.abc import Iterable
from copy import copy
from dataclasses import dataclass
from typing import Any

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId

Block = dict[str, Any] | None
MAX_WATERMARK = (1 << 63) - 1


class StreamControl:
    """Base type for ordered event-time control messages."""


@dataclass(frozen=True, slots=True)
class Watermark(StreamControl):
    """Monotonic event-time progress from one physical upstream input."""

    timestamp: int

    def __post_init__(self) -> None:
        if isinstance(self.timestamp, bool) or not isinstance(self.timestamp, int):
            raise TypeError("watermark timestamp must be an integer")
        if self.timestamp < 0:
            raise ValueError("watermark timestamp must be non-negative")


@dataclass(frozen=True, slots=True)
class InputIdle(StreamControl):
    """The sending input currently has no data and must not hold back time."""


@dataclass(frozen=True, slots=True)
class InputActive(StreamControl):
    """An idle input resumed; optional watermark establishes its progress."""

    resume_watermark: int | None = None

    def __post_init__(self) -> None:
        value = self.resume_watermark
        if value is None:
            return
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("resume_watermark must be an integer or None")
        if value < 0:
            raise ValueError("resume_watermark must be non-negative")


@dataclass(frozen=True, slots=True)
class RescaleBarrier(StreamControl):
    """A local topology fence used while one operator is replaced."""

    operation_id: str
    target_operator_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.operation_id, str) or not self.operation_id.strip():
            raise ValueError("rescale operation_id cannot be empty")
        if isinstance(self.target_operator_id, bool) or not isinstance(self.target_operator_id, int):
            raise TypeError("rescale target_operator_id must be an integer")


@dataclass(frozen=True, slots=True)
class DeliveryChannel:
    """Stable identity for one upstream edge's actual delivery target."""

    sender_vertex_id: object
    sender_task_name: str
    edge_index: int
    target_index: int
    topology_epoch: str | None = None


@dataclass(frozen=True, slots=True)
class PutAck:
    """Reply from a downstream ``StreamTask.put``.

    ``forwarded_sequence`` is the replay watermark: the largest ``batch_sequence`` from
    *this* sender whose derived output the downstream has already forwarded onward (or persisted, at a sink).
    The upstream may drop replay-buffer entries with ``batch_sequence <=
    forwarded_sequence`` — those records now exist on a second node, so a single
    downstream crash can't lose them. ``-1`` means "nothing acked yet".

    ``buffer_size`` is the downstream inbox's queued logical-row weight, not
    the number of transport envelopes.
    """

    accepted: bool
    buffer_size: int
    forwarded_sequence: int = -1


class Record:
    """Data record in data stream.

    ``num_rows`` distinguishes wire shapes for the columnar-passthrough path:
    ``None`` (default) is a row-shaped record whose block values are scalars. A
    non-None value marks a
    *columnar batch*: the block holds equal-length column arrays spanning
    ``num_rows`` rows, shipped as one record instead of being exploded into
    per-row dicts. The downstream InputBatchAccumulator uses this tag to decide whether
    to re-slice (columnar) or accumulate row-by-row.
    """

    def __init__(self, block: Block, num_rows: int | None = None) -> None:
        self.block: Block = block
        self.num_rows: int | None = num_rows
        self.sender: ExecutionVertexId | None = None
        self.input_tag: int | None = None
        self.timestamp: int | None = None

    @property
    def is_columnar(self) -> bool:
        return self.num_rows is not None

    def fork(self) -> "Record":
        """Copy a data record for fan-out without sharing its mutable block."""

        if self.block is None:
            raise ValueError("control records cannot be forked as data")
        forked = copy(self)
        forked.block = dict(self.block)
        return forked

    def __repr__(self) -> str:
        return f"Record({self.block})"

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        if self.block is None or other.block is None:
            return self.block is other.block
        if self.block.keys() != other.block.keys():
            return False
        return all(_values_equal(value, other.block[key]) for key, value in self.block.items())

    __hash__ = None


class KeyRecord(Record):
    """Data record in a keyed data stream"""

    def __init__(self, key: Any, block: dict[str, Any] | None) -> None:
        super().__init__(block)
        self.key: Any = key

    def __repr__(self) -> str:
        return f"KeyRecord(key={self.key}, block={self.block})"

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return self.key == other.key and Record.__eq__(self, other)
        return False

    __hash__ = None


class Barrier(Record):
    """
    Barrier record.
    """

    def __init__(
        self,
        _id: int,
        source_id: ExecutionVertexId | None = None,
        *,
        coordinated: bool = False,
    ) -> None:
        super().__init__(None)
        self.id = _id
        self.source_id = source_id
        self.coordinated = coordinated
        self.timestamp = int(time.time() * 1000)

    def __repr__(self) -> str:
        return (
            f"Barrier(id:{self.id}, source:{self.source_id}, coordinated:{self.coordinated}, "
            f"timestamp:{self.timestamp})"
        )

    def __eq__(self, other: object) -> bool:
        if type(self) is type(other):
            return self.id == other.id
        return False

    __hash__ = None


class EndOfData(Barrier):
    """
    End of data record.
    """

    def __repr__(self) -> str:
        return f"EndOfData(id:{self.id}, source:{self.source_id}, timestamp:{self.timestamp})"


def _values_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    if hasattr(left, "shape") and left.shape != right.shape:
        return False
    try:
        comparison = left == right
    except (TypeError, ValueError):
        return False
    try:
        return all(comparison) if isinstance(comparison, Iterable) else bool(comparison)
    except (TypeError, ValueError):
        return False
