# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from queue import Empty

from ray.util.queue import Queue
from ray.util.state import list_actors

import ray
from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.restart_strategy_options import RestartStrategyOptions
from ray.klein.runtime.coordinator import checkpoint_io
from tests.support.streaming import LoopSourceFunction
from tests.support.waiting import wait_until


class _QueueSink(SinkFunction):
    def __init__(self, output_queue: Queue) -> None:
        self._output_queue = output_queue

    def open(self, runtime_context: RuntimeContext) -> None:
        self.task_index = runtime_context.task_index

    def write(self, value) -> None:
        self._output_queue.put(value)

    def flush(self) -> None:
        return None


def _task_actors(address: str, namespace: str):
    return [
        actor
        for actor in list_actors(address=address, filters=[("ray_namespace", "=", namespace)], detail=True)
        if actor.name not in {None, "JobManager", "CheckpointCoordinator"}
    ]


def _drain_indices(output_queue: Queue) -> list[int]:
    indices = []
    # Snapshot and cap the available count so an infinite producer cannot keep
    # this diagnostic drain alive indefinitely while it is being consumed.
    for _ in range(min(output_queue.qsize(), 256)):
        try:
            row = output_queue.get(block=False)
        except Empty:
            break
        indices.append(row["idx"])
    return indices


def test_real_ray_recreates_a_killed_stream_task_from_checkpoint(tmp_path: Path, ray_cluster) -> None:
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 50)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(seconds=1))
    config.set(JobManagerOptions.HEALTH_CHECK_INTERVAL, 1)
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    config.set(RestartStrategyOptions.DELAY, timedelta(0))

    context = KleinContext(config)
    output_queue = Queue()
    stream = context.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"sleep_interval": 0.01},
        bounded=False,
        num_cpus=0.1,
        name="RecoverySource",
    ).map(lambda row: row, num_cpus=0.1, name="RecoveryMap")
    stream.write(
        _QueueSink,
        fn_constructor_args=[output_queue],
        num_cpus=0.1,
        name="RecoverySink",
    )
    handle = context.execute("runtime-task-recovery")

    try:
        checkpoint = wait_until(
            lambda: checkpoint_io.latest_checkpoint(
                (tmp_path / "checkpoints").as_uri(),
                handle.namespace,
            ),
            timeout=30,
            interval=0.2,
            description="a durable checkpoint before fault injection",
        )
        assert checkpoint

        original = wait_until(
            lambda: next(
                (
                    actor
                    for actor in _task_actors(ray_cluster.address_info["address"], handle.namespace)
                    if actor.state == "ALIVE" and actor.name.startswith("RecoveryMap")
                ),
                None,
            ),
            timeout=20,
            interval=0.2,
            description="a live stream task",
        )
        original_handle = ray.get_actor(original.name, namespace=handle.namespace)

        delivered_before_failure: set[int] = set()

        def contiguous_prefix_before_failure() -> int | None:
            delivered_before_failure.update(_drain_indices(output_queue))
            if not delivered_before_failure:
                return None
            boundary = max(delivered_before_failure)
            if boundary < 10 or not set(range(1, boundary + 1)).issubset(delivered_before_failure):
                return None
            return boundary

        failure_boundary = wait_until(
            contiguous_prefix_before_failure,
            timeout=20,
            interval=0.05,
            description="a contiguous output prefix before fault injection",
        )
        ray.kill(original_handle, no_restart=True)

        replacement = wait_until(
            lambda: next(
                (
                    actor
                    for actor in _task_actors(ray_cluster.address_info["address"], handle.namespace)
                    if actor.name == original.name and actor.state == "ALIVE" and actor.actor_id != original.actor_id
                ),
                None,
            ),
            timeout=60,
            interval=0.5,
            description=f"replacement actor for {original.name}",
        )
        assert replacement.actor_id != original.actor_id

        expected_after_failure = set(range(failure_boundary + 1, failure_boundary + 26))
        delivered_after_failure: list[int] = []

        def recovered_without_loss() -> bool:
            delivered_after_failure.extend(_drain_indices(output_queue))
            return expected_after_failure.issubset(delivered_after_failure)

        wait_until(
            recovered_without_loss,
            timeout=30,
            interval=0.05,
            description=f"every source id from {min(expected_after_failure)} through {max(expected_after_failure)}",
        )
        assert expected_after_failure.issubset(delivered_after_failure)
        assert handle.status == JobStatus.RUNNING
    finally:
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)
        output_queue.shutdown()

    assert handle.status == JobStatus.CANCELLED
