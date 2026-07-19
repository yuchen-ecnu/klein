# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import timedelta
from pathlib import Path

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
from ray.klein.config.restart_strategy_options import RestartStrategyOptions
from ray.klein.runtime.coordinator import checkpoint_io
from tests.support.streaming import LoopSourceFunction
from tests.support.waiting import wait_until


class _DiscardSink(SinkFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.task_index = runtime_context.task_index

    def write(self, value) -> None:
        return None

    def flush(self) -> None:
        return None


def _task_actors(address: str, namespace: str):
    return [
        actor
        for actor in list_actors(address=address, filters=[("ray_namespace", "=", namespace)], detail=True)
        if actor.name not in {None, "JobManager", "CheckpointCoordinator"}
    ]


def test_real_ray_recreates_a_killed_stream_task_from_checkpoint(tmp_path: Path, ray_cluster) -> None:
    config = Configuration()
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 50)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(seconds=1))
    config.set(JobManagerOptions.HEALTH_CHECK_INTERVAL, 1)
    config.set(RestartStrategyOptions.DELAY, timedelta(0))

    context = KleinContext(config)
    stream = context.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"sleep_interval": 0.01},
        bounded=False,
        num_cpus=0.1,
        name="RecoverySource",
    ).map(lambda row: row, num_cpus=0.1, name="RecoveryMap")
    stream.write(_DiscardSink, num_cpus=0.1, name="RecoverySink")
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
                    if actor.state == "ALIVE"
                ),
                None,
            ),
            timeout=20,
            interval=0.2,
            description="a live stream task",
        )
        original_handle = ray.get_actor(original.name, namespace=handle.namespace)
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
        assert handle.status == JobStatus.RUNNING
    finally:
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)

    assert handle.status == JobStatus.CANCELLED
