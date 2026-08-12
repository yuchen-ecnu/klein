# SPDX-License-Identifier: Apache-2.0
"""Connector-neutral coalescing of parallel transactional sink writers."""

from __future__ import annotations

import hashlib

from ray.klein.api.sink_committable_combiner import SinkCommittableCombiner
from ray.klein.state.sink_committable_checkpoint_entry import (
    SinkCommittableCheckpointEntry,
)


def coalesce_sink_committables(
    entries: dict[str, SinkCommittableCheckpointEntry],
    *,
    checkpoint_id: int,
    job_id: str,
) -> dict[str, SinkCommittableCheckpointEntry]:
    """Collapse opt-in parallel writers into one logical-sink transaction."""

    if len(entries) < 2:
        return entries

    result: dict[str, SinkCommittableCheckpointEntry] = {}
    combinable_groups: dict[
        str,
        list[tuple[SinkCommittableCheckpointEntry, SinkCommittableCombiner]],
    ] = {}
    for task_key, entry in entries.items():
        committable = entry.committable
        if isinstance(committable, SinkCommittableCombiner):
            logical_sink_id = task_key.partition(":")[0]
            combinable_groups.setdefault(logical_sink_id, []).append((entry, committable))
        else:
            result[task_key] = entry

    for logical_sink_id, group in combinable_groups.items():
        if len(group) == 1:
            entry, _combiner = group[0]
            result[entry.task_key] = entry
            continue

        combiners = tuple(combiner for _entry, combiner in group)
        namespaces = {combiner.global_commit_namespace for combiner in combiners}
        if len(namespaces) != 1:
            raise ValueError(f"logical sink {logical_sink_id!r} mixes incompatible committable combiners")
        namespace = namespaces.pop()
        if not namespace or ":" in namespace:
            raise ValueError("global commit namespace must be a non-empty colon-free string")

        writer_transaction_ids = sorted(entry.transaction_id for entry, _combiner in group)
        transaction_digest = hashlib.sha256("\0".join(writer_transaction_ids).encode()).hexdigest()
        global_transaction_id = f"klein:{namespace}:{job_id}:{logical_sink_id}:{checkpoint_id}:{transaction_digest}"
        committable = combiners[0].combine_committables(
            tuple(entry.committable for entry, _combiner in group),
            transaction_id=global_transaction_id,
        )
        global_task_key = f"{logical_sink_id}:global"
        result[global_task_key] = SinkCommittableCheckpointEntry(
            global_task_key,
            checkpoint_id,
            committable,
        )
    return result
