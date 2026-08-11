# SPDX-License-Identifier: Apache-2.0
"""Typed public-status model for an admitted operator-rescale operation."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any


class RescaleOperationStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    STABILIZING = "STABILIZING"
    COMPLETED = "COMPLETED"
    NOOP = "NOOP"
    REJECTED = "REJECTED"
    FAILED = "FAILED"

    @property
    def terminal(self) -> bool:
        return self in {
            RescaleOperationStatus.COMPLETED,
            RescaleOperationStatus.NOOP,
            RescaleOperationStatus.REJECTED,
            RescaleOperationStatus.FAILED,
        }


class RescaleOperationPhase(IntEnum):
    QUEUED = 0
    COORDINATING = 1
    STABILIZING = 2
    COMPLETED = 3


_ALLOWED_STATUS_TRANSITIONS = {
    RescaleOperationStatus.ACCEPTED: {
        RescaleOperationStatus.RUNNING,
        RescaleOperationStatus.FAILED,
    },
    RescaleOperationStatus.RUNNING: {
        RescaleOperationStatus.STABILIZING,
        RescaleOperationStatus.NOOP,
        RescaleOperationStatus.REJECTED,
        RescaleOperationStatus.FAILED,
    },
    RescaleOperationStatus.STABILIZING: {
        RescaleOperationStatus.COMPLETED,
        RescaleOperationStatus.FAILED,
    },
}
TERMINAL_RESCALE_STATUSES = frozenset(status.value for status in RescaleOperationStatus if status.terminal)
_FIELDS = (
    "operation_id",
    "job_id",
    "operator_id",
    "operator_name",
    "previous_parallelism",
    "parallelism",
    "target_parallelism",
    "status",
    "phase",
    "accepted_at_ms",
    "started_at_ms",
    "updated_at_ms",
    "ended_at_ms",
    "error",
)
_ERROR_UNSET = object()


@dataclass(slots=True)
class RescaleOperation(Mapping[str, object]):
    operation_id: str
    job_id: str
    operator_id: int | str
    operator_name: str | None
    previous_parallelism: int | None
    target_parallelism: int
    status: RescaleOperationStatus
    phase: RescaleOperationPhase
    accepted_at_ms: int
    started_at_ms: int | None
    updated_at_ms: int
    ended_at_ms: int | None
    error: str | None

    @classmethod
    def accepted(
        cls,
        *,
        operation_id: str,
        job_id: str,
        operator_id: int | str,
        operator_name: str,
        previous_parallelism: int,
        target_parallelism: int,
        now_ms: int,
    ) -> RescaleOperation:
        return cls(
            operation_id=operation_id,
            job_id=job_id,
            operator_id=operator_id,
            operator_name=operator_name,
            previous_parallelism=previous_parallelism,
            target_parallelism=target_parallelism,
            status=RescaleOperationStatus.ACCEPTED,
            phase=RescaleOperationPhase.QUEUED,
            accepted_at_ms=now_ms,
            started_at_ms=None,
            updated_at_ms=now_ms,
            ended_at_ms=None,
            error=None,
        )

    @classmethod
    def terminal(
        cls,
        *,
        operation_id: str,
        job_id: str,
        operator_id: int | str,
        operator_name: str | None,
        previous_parallelism: int | None,
        target_parallelism: int,
        status: str,
        error: str | None,
        now_ms: int,
    ) -> RescaleOperation:
        parsed_status = RescaleOperationStatus(status)
        if not parsed_status.terminal:
            raise ValueError(f"terminal rescale operation cannot use status {status}")
        return cls(
            operation_id=operation_id,
            job_id=job_id,
            operator_id=operator_id,
            operator_name=operator_name,
            previous_parallelism=previous_parallelism,
            target_parallelism=target_parallelism,
            status=parsed_status,
            phase=RescaleOperationPhase.COMPLETED,
            accepted_at_ms=now_ms,
            started_at_ms=now_ms,
            updated_at_ms=now_ms,
            ended_at_ms=now_ms,
            error=error,
        )

    def transition(
        self,
        *,
        status: str,
        phase: str,
        now_ms: int,
        started_at_ms: int | None = None,
        error: str | None | object = _ERROR_UNSET,
    ) -> None:
        next_status = RescaleOperationStatus(status)
        next_phase = RescaleOperationPhase[phase]
        if next_status not in _ALLOWED_STATUS_TRANSITIONS.get(self.status, set()):
            raise RuntimeError(f"invalid rescale status transition {self.status.value} -> {next_status.value}")
        if next_phase < self.phase:
            raise RuntimeError(f"rescale phase cannot move backward from {self.phase.name} to {next_phase.name}")
        if next_status.terminal and next_phase is not RescaleOperationPhase.COMPLETED:
            raise RuntimeError("a terminal rescale status requires the COMPLETED phase")
        self.status = next_status
        self.phase = next_phase
        self.updated_at_ms = now_ms
        if started_at_ms is not None:
            self.started_at_ms = started_at_ms
        if error is not _ERROR_UNSET:
            self.error = error if isinstance(error, str) or error is None else str(error)
        if next_status.terminal:
            self.ended_at_ms = now_ms

    def merge_result(self, result: Mapping[str, Any], *, now_ms: int) -> None:
        if "operator_id" in result:
            self.operator_id = result["operator_id"]
        if "operator_name" in result:
            self.operator_name = result["operator_name"]
        if "previous_parallelism" in result:
            self.previous_parallelism = result["previous_parallelism"]
        if "target_parallelism" in result:
            self.target_parallelism = result["target_parallelism"]
        elif "parallelism" in result:
            self.target_parallelism = result["parallelism"]
        if "error" in result:
            error = result["error"]
            self.error = error if isinstance(error, str) or error is None else str(error)
        self.updated_at_ms = now_ms

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "job_id": self.job_id,
            "operator_id": self.operator_id,
            "operator_name": self.operator_name,
            "previous_parallelism": self.previous_parallelism,
            "parallelism": self.target_parallelism,
            "target_parallelism": self.target_parallelism,
            "status": self.status.value,
            "phase": self.phase.name,
            "accepted_at_ms": self.accepted_at_ms,
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "ended_at_ms": self.ended_at_ms,
            "error": self.error,
        }

    def __getitem__(self, key: str) -> object:
        try:
            return self.to_dict()[key]
        except KeyError:
            raise KeyError(key) from None

    def __iter__(self) -> Iterator[str]:
        return iter(_FIELDS)

    def __len__(self) -> int:
        return len(_FIELDS)
