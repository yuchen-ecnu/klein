# SPDX-License-Identifier: Apache-2.0
"""Repeatable microbenchmarks for Klein's transport hot paths.

The baseline functions model the replaced implementation. The optimized side
calls the current production helpers. No Ray cluster or external service is
required, so this is suitable for local regression checks.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import gc
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import ray.cloudpickle as cloudpickle

from ray.klein._internal.block import slice_block_rows
from ray.klein.runtime.collector.delivery_command import DataCommand
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.collector.edge_output import EdgeOutput
from ray.klein.runtime.message import Record


@dataclass(frozen=True, slots=True)
class Result:
    name: str
    baseline_ms: float
    optimized_ms: float
    detail: str = ""

    @property
    def speedup(self) -> float:
        return self.baseline_ms / self.optimized_ms if self.optimized_ms else float("inf")


def _median_ms(function: Callable[[], object], repeats: int) -> float:
    samples = []
    gc_enabled = gc.isenabled()
    gc.disable()
    try:
        for _ in range(repeats):
            started_at = time.perf_counter_ns()
            function()
            samples.append((time.perf_counter_ns() - started_at) / 1_000_000)
    finally:
        if gc_enabled:
            gc.enable()
    return statistics.median(samples)


def _actor_copy(repeats: int) -> Result:
    record = Record({"values": np.arange(256 * 1024, dtype=np.int64)})
    args = ((record,),)
    baseline = _median_ms(lambda: copy.deepcopy(args), repeats)
    optimized = _median_ms(lambda: args, repeats)
    return Result("real-Ray pre-RPC copy", baseline, optimized, "2 MiB NumPy payload")


def _columnar_slice(repeats: int) -> Result:
    values = np.arange(1_000_000, dtype=np.int64)
    block = {"values": values}
    baseline = _median_ms(lambda: {"values": values[list(range(250_000, 750_000))]}, repeats)
    optimized = _median_ms(lambda: slice_block_rows(block, slice(250_000, 750_000)), repeats)
    view = slice_block_rows(block, slice(250_000, 750_000))["values"]
    return Result(
        "contiguous columnar slice",
        baseline,
        optimized,
        f"shares_memory={np.shares_memory(values, view)}",
    )


def _whole_block_validation(repeats: int) -> Result:
    rows = 100_000
    baseline = _median_ms(lambda: set(range(rows)) == set(range(rows)), repeats)
    optimized = _median_ms(lambda: None, repeats)
    return Result("whole-block route coverage", baseline, optimized, f"{rows} rows")


def _replay_accounting(entries: int, repeats: int) -> Result:
    records = tuple(Record({"id": index}) for index in range(entries))

    def legacy() -> None:
        buffered: list[tuple[Record, ...]] = []
        for record in records:
            buffered.append((record,))
            sum(1 if item.num_rows is None else item.num_rows for batch in buffered for item in batch)

    def optimized() -> None:
        journal = DeliveryJournal(1)
        journal.configure(True, "sender", 1 << 30)
        journal.attach_observers(lambda _value: None)
        for sequence, record in enumerate(records, 1):
            journal.record_delivery(0, (record,), sequence)

    return Result(
        "replay buffered-row accounting",
        _median_ms(legacy, repeats),
        _median_ms(optimized, repeats),
        f"{entries} commits",
    )


def _source_batch_serialization(rows: int, repeats: int) -> Result:
    records = [
        {
            "topic": "events",
            "partition": index % 8,
            "offset": index,
            "key": b"key",
            "value": b"x" * 128,
        }
        for index in range(rows)
    ]
    row_records = tuple(Record(row) for row in records)
    columns = {key: [row[key] for row in records] for key in records[0]}
    columnar = Record(columns, num_rows=rows)
    baseline = _median_ms(lambda: tuple(cloudpickle.dumps(record) for record in row_records), repeats)
    optimized = _median_ms(lambda: cloudpickle.dumps(columnar), repeats)
    return Result("Kafka poll serialization", baseline, optimized, f"{rows}:1 transport calls")


class _SleepingEdge(EdgeOutput):
    def __init__(self, latency: float) -> None:
        self._latency = latency

    async def _send_async(self, _command) -> None:
        await asyncio.sleep(self._latency)


async def _lane_trial(latency: float, concurrent: bool) -> float:
    edge = _SleepingEdge(latency)
    commands = [DataCommand(index, (index,), (Record({"id": index}),)) for index in range(4)]
    started_at = time.perf_counter_ns()
    if concurrent:
        await edge.send_commands(commands)
    else:
        for command in commands:
            await edge._send_async(command)
    return (time.perf_counter_ns() - started_at) / 1_000_000


def _target_lanes(repeats: int) -> Result:
    async def run() -> tuple[float, float]:
        baseline = [await _lane_trial(0.005, False) for _ in range(repeats)]
        optimized = [await _lane_trial(0.005, True) for _ in range(repeats)]
        return statistics.median(baseline), statistics.median(optimized)

    baseline, optimized = asyncio.run(run())
    return Result("four independent target lanes", baseline, optimized, "5 ms/target")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="Use fewer repeats for a fast smoke benchmark.")
    args = parser.parse_args()
    repeats = 5 if args.quick else 25
    results = [
        _actor_copy(repeats),
        _columnar_slice(repeats),
        _whole_block_validation(repeats),
        _replay_accounting(2_000, 2 if args.quick else 5),
        _source_batch_serialization(1_000, 2 if args.quick else 10),
        _target_lanes(2 if args.quick else 5),
    ]
    print(f"{'benchmark':36} {'baseline ms':>12} {'optimized ms':>13} {'speedup':>9}  detail")
    for result in results:
        print(
            f"{result.name:36} {result.baseline_ms:12.4f} "
            f"{result.optimized_ms:13.4f} {result.speedup:8.2f}x  {result.detail}"
        )


if __name__ == "__main__":
    main()
