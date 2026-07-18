# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

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
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.cancelled_with = None
        self.dashboard_snapshot = _RemoteMethod(self._dashboard_snapshot)
        self.cancel = _RemoteMethod(self._cancel)

    async def _dashboard_snapshot(self):
        if isinstance(self.snapshot, BaseException):
            raise self.snapshot
        return deepcopy(self.snapshot)

    async def _cancel(self, timeout):
        self.cancelled_with = timeout


def test_dashboard_configuration_redacts_credential_like_options() -> None:
    config = Configuration(
        {
            "connector.kafka.bootstrap.servers": "broker:9092",
            "connector.kafka.api-key": "sensitive",
            "connector.redis.password": "also-sensitive",
        }
    )

    safe = safe_configuration(config)

    assert safe["connector.kafka.bootstrap.servers"] == "broker:9092"
    assert safe["connector.kafka.api-key"] == "<redacted>"
    assert safe["connector.redis.password"] == "<redacted>"


def test_interval_metrics_are_derived_from_monotonic_counters() -> None:
    previous = {
        "operators": [
            {
                "op_id": 1,
                "rows_in": 100,
                "rows_out": 50,
                "busy_ns": 1_000_000_000,
                "backpressure_ns": 0,
                "subtasks": [
                    {
                        "subtask_index": 0,
                        "rows_in": 50,
                        "rows_out": 25,
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
                "busy_ns": 2_000_000_000,
                "backpressure_ns": 500_000_000,
                "subtasks": [
                    {
                        "subtask_index": 0,
                        "rows_in": 70,
                        "rows_out": 35,
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
    assert operator["busy_percent"] == 50
    assert operator["backpressure_percent"] == 25
    assert operator["subtasks"][0]["rows_in_per_second"] == 20
    assert operator["subtasks"][0]["rows_out_per_second"] == 10
    assert operator["subtasks"][0]["busy_percent"] == 50
    assert operator["subtasks"][0]["backpressure_percent"] == 25


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
async def test_state_actor_forwards_cancellation_to_the_job_manager() -> None:
    manager = _FakeJobManager({"status": "CANCELLED", "operators": []})
    actor = _KleinStateActor()
    actor.register_job("job-1", manager, {"job_name": "events"})

    assert await actor.cancel_job("job-1", timeout=15) is True
    assert await actor.cancel_job("missing", timeout=15) is False
    assert manager.cancelled_with == 15
