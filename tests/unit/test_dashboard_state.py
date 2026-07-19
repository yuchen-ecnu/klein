# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
from copy import deepcopy

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.observability.dashboard.serialization import safe_configuration
from ray.klein.observability.dashboard.state_actor import _KleinStateActor, _with_interval_metrics


class _RemoteMethod:
    def __init__(self, function):
        self._function = function

    def remote(self, *args, **kwargs):
        return self._function(*args, **kwargs)


class _FakeJobManager:
    def __init__(self, snapshot, *, rescale_result=None, rescale_delay: float = 0):
        self.snapshot = snapshot
        self.cancelled_with = None
        self.rescaled_with = None
        self.rescale_result = rescale_result
        self.rescale_delay = rescale_delay
        self.dashboard_snapshot = _RemoteMethod(self._dashboard_snapshot)
        self.cancel = _RemoteMethod(self._cancel)
        self.rescale_operator = _RemoteMethod(self._rescale_operator)

    async def _dashboard_snapshot(self):
        if isinstance(self.snapshot, BaseException):
            raise self.snapshot
        return deepcopy(self.snapshot)

    async def _cancel(self, timeout):
        self.cancelled_with = timeout

    async def _rescale_operator(self, operator_id, parallelism):
        self.rescaled_with = (operator_id, parallelism)
        if self.rescale_delay:
            await asyncio.sleep(self.rescale_delay)
        return deepcopy(self.rescale_result)


def test_dashboard_configuration_redacts_credential_like_options() -> None:
    config = Configuration(
        {
            "connector.kafka.bootstrap.servers": "broker:9092",
            "connector.kafka.api-key": "sensitive",
            "connector.redis.password": "also-sensitive",
            "execution.checkpointing.storage-options": {
                "endpoint": "https://objects.example",
                "access_key": "AKIA-EXAMPLE",
                "nested": {"client_secret": "deep-secret"},
            },
        }
    )

    safe = safe_configuration(config)

    assert safe["connector.kafka.bootstrap.servers"] == "broker:9092"
    assert safe["connector.kafka.api-key"] == "<redacted>"
    assert safe["connector.redis.password"] == "<redacted>"
    assert safe["execution.checkpointing.storage-options"] == {
        "endpoint": "https://objects.example",
        "access_key": "<redacted>",
        "nested": {"client_secret": "<redacted>"},
    }


def test_interval_metrics_are_derived_from_monotonic_counters() -> None:
    previous = {
        "operators": [
            {
                "op_id": 1,
                "rows_in": 100,
                "rows_out": 50,
                "bytes_in": 1_000,
                "bytes_out": 500,
                "busy_ns": 1_000_000_000,
                "backpressure_ns": 0,
                "subtasks": [
                    {
                        "subtask_index": 0,
                        "rows_in": 50,
                        "rows_out": 25,
                        "bytes_in": 500,
                        "bytes_out": 250,
                        "busy_ns": 500_000_000,
                        "backpressure_ns": 0,
                    }
                ],
            }
        ]
    }
    current = {
        "operators": [
            {
                "op_id": 1,
                "parallelism": 2,
                "rows_in": 140,
                "rows_out": 70,
                "bytes_in": 1_400,
                "bytes_out": 700,
                "busy_ns": 2_000_000_000,
                "backpressure_ns": 500_000_000,
                "subtasks": [
                    {
                        "subtask_index": 0,
                        "rows_in": 70,
                        "rows_out": 35,
                        "bytes_in": 700,
                        "bytes_out": 350,
                        "busy_ns": 1_000_000_000,
                        "backpressure_ns": 250_000_000,
                    }
                ],
            }
        ]
    }

    operator = _with_interval_metrics(current, previous, elapsed_ms=1_000)["operators"][0]

    assert operator["rows_in_per_second"] == 40
    assert operator["rows_out_per_second"] == 20
    assert operator["bytes_in_per_second"] == 400
    assert operator["bytes_out_per_second"] == 200
    assert operator["busy_percent"] == 50
    assert operator["backpressure_percent"] == 25
    assert operator["max_busy_percent"] == 50
    assert operator["max_backpressure_percent"] == 25
    assert operator["subtasks"][0]["rows_in_per_second"] == 20
    assert operator["subtasks"][0]["rows_out_per_second"] == 10
    assert operator["subtasks"][0]["bytes_in_per_second"] == 200
    assert operator["subtasks"][0]["bytes_out_per_second"] == 100
    assert operator["subtasks"][0]["busy_percent"] == 50
    assert operator["subtasks"][0]["backpressure_percent"] == 25


def test_topology_metrics_surface_the_hottest_subtask() -> None:
    previous = {
        "operators": [
            {
                "op_id": 1,
                "busy_ns": 0,
                "backpressure_ns": 0,
                "subtasks": [
                    {"subtask_index": 0, "busy_ns": 0, "backpressure_ns": 0},
                    {"subtask_index": 1, "busy_ns": 0, "backpressure_ns": 0},
                ],
            }
        ]
    }
    current = {
        "operators": [
            {
                "op_id": 1,
                "parallelism": 2,
                "busy_ns": 1_000_000_000,
                "backpressure_ns": 500_000_000,
                "subtasks": [
                    {"subtask_index": 0, "busy_ns": 200_000_000, "backpressure_ns": 100_000_000},
                    {"subtask_index": 1, "busy_ns": 900_000_000, "backpressure_ns": 600_000_000},
                ],
            }
        ]
    }

    operator = _with_interval_metrics(current, previous, elapsed_ms=1_000)["operators"][0]

    assert operator["busy_percent"] == 50
    assert operator["backpressure_percent"] == 25
    assert operator["max_busy_percent"] == 90
    assert operator["max_backpressure_percent"] == 60


@pytest.mark.asyncio
async def test_state_actor_returns_last_snapshot_when_job_manager_is_unavailable() -> None:
    snapshot = {"status": "RUNNING", "operators": [], "overview": {}, "checkpoints": {}}
    manager = _FakeJobManager(snapshot)
    actor = _KleinStateActor(history_size=2)
    actor.register_job("job-1", manager, {"job_name": "events"})

    fresh = await actor.get_job("job-1")
    manager.snapshot = RuntimeError("actor unavailable")
    stale = await actor.get_job("job-1")

    assert fresh["dashboard_stale"] is False
    assert stale["status"] == "RUNNING"
    assert stale["dashboard_stale"] is True
    assert stale["dashboard_error"] == "RuntimeError: actor unavailable"


@pytest.mark.asyncio
async def test_state_actor_does_not_let_an_older_refresh_overwrite_a_newer_snapshot() -> None:
    old_release = asyncio.Event()
    new_release = asyncio.Event()

    class _RacingJobManager:
        def __init__(self) -> None:
            self.calls = 0
            self.dashboard_snapshot = _RemoteMethod(self._dashboard_snapshot)

        async def _dashboard_snapshot(self):
            self.calls += 1
            if self.calls == 1:
                await old_release.wait()
                return {"status": "RUNNING", "marker": "old", "operators": []}
            await new_release.wait()
            return {"status": "RUNNING", "marker": "new", "operators": []}

    manager = _RacingJobManager()
    actor = _KleinStateActor()
    actor.register_job("job-1", manager, {"job_name": "events"})
    old_refresh = asyncio.create_task(actor.get_job("job-1"))
    await asyncio.sleep(0)
    new_refresh = asyncio.create_task(actor.get_job("job-1"))
    await asyncio.sleep(0)

    new_release.set()
    assert (await new_refresh)["marker"] == "new"
    old_release.set()
    assert (await old_refresh)["marker"] == "old"
    assert actor._entries["job-1"]["snapshot"]["marker"] == "new"


@pytest.mark.asyncio
async def test_state_actor_forwards_cancellation_to_the_job_manager() -> None:
    manager = _FakeJobManager({"status": "CANCELLED", "operators": []})
    actor = _KleinStateActor()
    actor.register_job("job-1", manager, {"job_name": "events"})

    assert await actor.cancel_job("job-1", timeout=15) is True
    assert await actor.cancel_job("missing", timeout=15) is False
    assert manager.cancelled_with == 15


@pytest.mark.asyncio
async def test_state_actor_forwards_rescale_and_normalizes_the_result() -> None:
    manager = _FakeJobManager(
        {"status": "RUNNING", "operators": []},
        rescale_result={
            "job_id": "wrong-job",
            "operator_id": 99,
            "previous_parallelism": 2,
            "target_parallelism": 4,
            "status": "COMPLETED",
        },
    )
    actor = _KleinStateActor()
    actor.register_job("job-1", manager, {"job_name": "events"})

    result = await actor.rescale_operator("job-1", 3, 4, timeout=15)

    assert result == {
        "job_id": "job-1",
        "operator_id": 3,
        "previous_parallelism": 2,
        "target_parallelism": 4,
        "parallelism": 4,
        "status": "COMPLETED",
    }
    assert manager.rescaled_with == (3, 4)
    assert await actor.rescale_operator("missing", 3, 4) is None


@pytest.mark.asyncio
async def test_state_actor_bounds_rescale_wait_time() -> None:
    manager = _FakeJobManager(
        {"status": "RUNNING", "operators": []},
        rescale_result={"status": "COMPLETED"},
        rescale_delay=0.1,
    )
    actor = _KleinStateActor()
    actor.register_job("job-1", manager, {"job_name": "events"})

    with pytest.raises(asyncio.TimeoutError):
        await actor.rescale_operator("job-1", 3, 4, timeout=0.001)


@pytest.mark.asyncio
async def test_state_actor_rejects_non_mapping_rescale_results() -> None:
    manager = _FakeJobManager(
        {"status": "RUNNING", "operators": []},
        rescale_result=True,
    )
    actor = _KleinStateActor()
    actor.register_job("job-1", manager, {"job_name": "events"})

    with pytest.raises(TypeError, match="must return a dict"):
        await actor.rescale_operator("job-1", 3, 4)
