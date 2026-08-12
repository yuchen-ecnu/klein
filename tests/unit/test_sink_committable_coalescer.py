# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pytest

from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.runtime.coordinator.sink_committable_coalescer import (
    coalesce_sink_committables,
)
from ray.klein.state.sink_committable_checkpoint_entry import (
    SinkCommittableCheckpointEntry,
)


@dataclass(frozen=True)
class _CombinedCommittable(SinkCommittable):
    _transaction_id: str
    writer_ids: tuple[str, ...]

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        return None

    def abort(self) -> None:
        return None


@dataclass(frozen=True)
class _WriterCommittable(SinkCommittable):
    _transaction_id: str
    namespace: str = "test"

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    @property
    def global_commit_namespace(self) -> str:
        return self.namespace

    def combine_committables(
        self,
        committables: tuple[SinkCommittable, ...],
        *,
        transaction_id: str,
    ) -> SinkCommittable:
        return _CombinedCommittable(
            transaction_id,
            tuple(sorted(item.transaction_id for item in committables)),
        )

    def commit(self) -> None:
        return None

    def abort(self) -> None:
        return None


def _entry(task_key: str, committable: SinkCommittable) -> SinkCommittableCheckpointEntry:
    return SinkCommittableCheckpointEntry(task_key, 7, committable)


def test_coalesces_an_opt_in_writer_group_without_connector_dependencies() -> None:
    entries = {
        "4:1": _entry("4:1", _WriterCommittable("writer-b")),
        "4:0": _entry("4:0", _WriterCommittable("writer-a")),
    }

    coalesced = coalesce_sink_committables(entries, checkpoint_id=7, job_id="job")

    digest = hashlib.sha256(b"writer-a\0writer-b").hexdigest()
    assert tuple(coalesced) == ("4:global",)
    assert coalesced["4:global"].transaction_id == f"klein:test:job:4:7:{digest}"
    assert coalesced["4:global"].committable.writer_ids == ("writer-a", "writer-b")


def test_keeps_non_combining_and_single_writer_transactions_unchanged() -> None:
    plain = _CombinedCommittable("plain", ())
    writer = _WriterCommittable("writer")
    entries = {"1:0": _entry("1:0", plain), "2:0": _entry("2:0", writer)}

    assert coalesce_sink_committables(entries, checkpoint_id=7, job_id="job") == entries


def test_rejects_mixed_or_invalid_combiner_namespaces() -> None:
    with pytest.raises(ValueError, match="mixes incompatible"):
        coalesce_sink_committables(
            {
                "3:0": _entry("3:0", _WriterCommittable("a", "first")),
                "3:1": _entry("3:1", _WriterCommittable("b", "second")),
            },
            checkpoint_id=7,
            job_id="job",
        )

    with pytest.raises(ValueError, match="colon-free"):
        coalesce_sink_committables(
            {
                "3:0": _entry("3:0", _WriterCommittable("a", "bad:value")),
                "3:1": _entry("3:1", _WriterCommittable("b", "bad:value")),
            },
            checkpoint_id=7,
            job_id="job",
        )
