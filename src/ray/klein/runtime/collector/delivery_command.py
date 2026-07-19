# SPDX-License-Identifier: Apache-2.0
"""Immutable commands crossing from the operator executor to the actor loop."""

from dataclasses import dataclass

from ray.klein.runtime.message import Barrier, Record, StreamControl


class EdgeCommand:
    """One command owned by a single physical output edge."""


@dataclass(frozen=True, slots=True)
class DataCommand(EdgeCommand):
    target: int
    retry_ring: tuple[int, ...]
    records: tuple[Record, ...]


@dataclass(frozen=True, slots=True)
class BarrierCommand(EdgeCommand):
    barrier: Barrier


@dataclass(frozen=True, slots=True)
class ControlCommand(EdgeCommand):
    control: StreamControl


@dataclass(frozen=True, slots=True)
class ReplayCommand(EdgeCommand):
    target: int
    sequence: int
    records: tuple[Record, ...]


@dataclass(frozen=True, slots=True)
class DeliveryCommand:
    """An edge command tagged with the task output edge that owns it."""

    edge_index: int
    command: EdgeCommand
