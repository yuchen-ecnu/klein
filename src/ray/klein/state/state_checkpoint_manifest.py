# SPDX-License-Identifier: Apache-2.0
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ray.klein.state.checkpoint_file_scope import CheckpointFileScope
from ray.klein.state.state_checkpoint_entry import StateCheckpointEntry
from ray.klein.state.state_partition import StatePartition


@dataclass(frozen=True, slots=True)
class StateCheckpointManifest:
    """Durable, ObjectRef-free description of a completed checkpoint."""

    job_id: str
    checkpoint_id: int
    epoch: int
    entries: tuple[StateCheckpointEntry, ...]
    source_positions: tuple[tuple[str, str], ...] = ()
    sink_transactions: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("job_id must not be empty")
        if self.checkpoint_id < 0:
            raise ValueError("checkpoint_id must be non-negative")
        if self.epoch < 0:
            raise ValueError("epoch must be non-negative")
        if any(entry.partition.job_id != self.job_id for entry in self.entries):
            raise ValueError("all entries must belong to manifest.job_id")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation without ObjectRefs."""

        return {
            "job_id": self.job_id,
            "checkpoint_id": self.checkpoint_id,
            "epoch": self.epoch,
            "entries": [
                {
                    "job_id": entry.partition.job_id,
                    "operator_id": entry.partition.operator_id,
                    "key_group": entry.partition.key_group,
                    "version": entry.version,
                    "input_sequence": entry.input_sequence,
                    "uri": entry.uri,
                    "checksum": entry.checksum,
                    "size_bytes": entry.size_bytes,
                    "scope": entry.scope.value,
                }
                for entry in self.entries
            ],
            "source_positions": dict(self.source_positions),
            "sink_transactions": dict(self.sink_transactions),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "StateCheckpointManifest":
        """Build a manifest from its JSON-decoded representation."""

        entries = tuple(
            StateCheckpointEntry(
                partition=StatePartition(
                    job_id=item["job_id"],
                    operator_id=item["operator_id"],
                    key_group=item["key_group"],
                ),
                version=item["version"],
                input_sequence=item["input_sequence"],
                uri=item["uri"],
                checksum=item["checksum"],
                size_bytes=item["size_bytes"],
                scope=CheckpointFileScope(item["scope"]),
            )
            for item in value["entries"]
        )
        return cls(
            job_id=value["job_id"],
            checkpoint_id=value["checkpoint_id"],
            epoch=value["epoch"],
            entries=entries,
            source_positions=tuple(sorted(value.get("source_positions", {}).items())),
            sink_transactions=tuple(sorted(value.get("sink_transactions", {}).items())),
        )
