# SPDX-License-Identifier: Apache-2.0
"""Cluster-wide Klein job-state publication actor.

This follows the same ownership model as Ray Train's state actor: the first
Klein streaming job creates a zero-CPU detached actor on the head node. Job
managers remain the source of truth; this actor only keeps discovery metadata
and the last successful immutable snapshot for terminal history and outages.
"""

from __future__ import annotations

import asyncio
import copy
import threading
import time
from collections import OrderedDict
from typing import Any

from ray.actor import ActorHandle

import ray
from ray.klein._internal.logging import get_logger
from ray.klein.config.configuration import Configuration
from ray.klein.config.observability_options import ObservabilityOptions
from ray.klein.observability.dashboard.serialization import dashboard_value

logger = get_logger(__name__)

KLEIN_STATE_ACTOR_NAME = "klein_state_actor"
KLEIN_STATE_ACTOR_NAMESPACE = "_klein_state_actor"
_DEFAULT_HISTORY_SIZE = 100
_REFRESH_TIMEOUT_SECONDS = 10
_state_actor_lock = threading.RLock()


class _KleinStateActor:
    """In-memory discovery index and last-known snapshot cache."""

    def __init__(self, history_size: int = _DEFAULT_HISTORY_SIZE) -> None:
        if history_size < 1:
            raise ValueError("history_size must be at least 1")
        self._history_size = history_size
        self._entries: OrderedDict[str, dict[str, Any]] = OrderedDict()

    def register_job(
        self,
        job_id: str,
        manager: ActorHandle,
        metadata: dict[str, Any],
    ) -> None:
        if not job_id:
            raise ValueError("job_id cannot be empty")
        now_ms = int(time.time() * 1000)
        existing = self._entries.pop(job_id, None)
        self._entries[job_id] = {
            "manager": manager,
            "metadata": {**(existing or {}).get("metadata", {}), **copy.deepcopy(metadata)},
            "snapshot": (existing or {}).get("snapshot"),
            "registered_at_ms": (existing or {}).get("registered_at_ms", now_ms),
            "last_refresh_ms": (existing or {}).get("last_refresh_ms"),
            "refresh_generation": 0,
        }
        self._trim_history()

    async def get_jobs(self) -> list[dict[str, Any]]:
        job_ids = list(reversed(self._entries))
        snapshots = await asyncio.gather(
            *(self._refresh(job_id) for job_id in job_ids),
            return_exceptions=True,
        )
        jobs = []
        for job_id, result in zip(job_ids, snapshots, strict=True):
            if isinstance(result, BaseException):
                jobs.append(self._stale_snapshot(job_id, result))
            else:
                jobs.append(result)
        return jobs

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        if job_id not in self._entries:
            return None
        try:
            return await self._refresh(job_id)
        except Exception as error:
            return self._stale_snapshot(job_id, error)

    async def cancel_job(self, job_id: str, timeout: int = 60) -> bool:
        entry = self._entries.get(job_id)
        if entry is None:
            return False
        await asyncio.wait_for(entry["manager"].cancel.remote(timeout), timeout=timeout + 5)
        await self._refresh(job_id)
        return True

    async def rescale_operator(
        self,
        job_id: str,
        operator_id: int,
        parallelism: int,
        timeout: float = 60,
    ) -> dict[str, Any] | None:
        """Forward one bounded rescale request to the authoritative JobManager."""

        entry = self._entries.get(job_id)
        if entry is None:
            return None
        raw_result = await asyncio.wait_for(
            entry["manager"].rescale_operator.remote(operator_id, parallelism),
            timeout=timeout,
        )
        if not isinstance(raw_result, dict):
            raise TypeError("JobManager.rescale_operator must return a dict")
        result = dashboard_value(raw_result)
        # Keep request identity stable at the public boundary and expose the
        # concise ``parallelism`` spelling expected by dashboard clients while
        # retaining the explicit runtime result field.
        result["job_id"] = job_id
        result["operator_id"] = operator_id
        result.setdefault("target_parallelism", parallelism)
        result["parallelism"] = result["target_parallelism"]
        return result

    def publish_snapshot(self, job_id: str, snapshot: dict[str, Any]) -> None:
        entry = self._entries.get(job_id)
        if entry is None:
            return
        entry["snapshot"] = copy.deepcopy(snapshot)
        entry["last_refresh_ms"] = int(time.time() * 1000)

    async def _refresh(self, job_id: str) -> dict[str, Any]:
        entry = self._entries[job_id]
        generation = int(entry.get("refresh_generation", 0)) + 1
        entry["refresh_generation"] = generation
        snapshot = await asyncio.wait_for(
            entry["manager"].dashboard_snapshot.remote(),
            timeout=_REFRESH_TIMEOUT_SECONDS,
        )
        now_ms = int(time.time() * 1000)
        merged = {
            **entry["metadata"],
            **snapshot,
            "job_id": job_id,
            "dashboard_stale": False,
        }
        # Multiple dashboard readers may refresh the same job concurrently.
        # A slower, older RPC may return after a newer one; return its response
        # to its own caller, but never let it roll the shared cache backwards.
        if self._entries.get(job_id) is not entry or entry["refresh_generation"] != generation:
            return _with_interval_metrics(
                merged,
                entry.get("snapshot"),
                now_ms - entry["last_refresh_ms"] if entry.get("last_refresh_ms") else None,
            )
        merged = _with_interval_metrics(
            merged,
            entry.get("snapshot"),
            now_ms - entry["last_refresh_ms"] if entry.get("last_refresh_ms") else None,
        )
        entry["snapshot"] = copy.deepcopy(merged)
        entry["last_refresh_ms"] = now_ms
        return merged

    def _stale_snapshot(self, job_id: str, error: BaseException) -> dict[str, Any]:
        entry = self._entries[job_id]
        cached = entry.get("snapshot") or entry["metadata"]
        return {
            **copy.deepcopy(cached),
            "job_id": job_id,
            "dashboard_stale": True,
            "dashboard_error": f"{type(error).__name__}: {error}",
        }

    def _trim_history(self) -> None:
        while len(self._entries) > self._history_size:
            terminal_id = next(
                (
                    job_id
                    for job_id, entry in self._entries.items()
                    if (entry.get("snapshot") or entry["metadata"]).get("status") in {"FINISHED", "FAILED", "CANCELLED"}
                ),
                None,
            )
            self._entries.pop(terminal_id or next(iter(self._entries)))


def get_or_create_state_actor(config: Configuration | None = None) -> ActorHandle:
    """Return the detached, head-node Klein state actor singleton."""

    history_size = (config or Configuration()).get(ObservabilityOptions.DASHBOARD_HISTORY_SIZE)
    if history_size < 1:
        raise ValueError("observability.dashboard.history-size must be at least 1")
    with _state_actor_lock:
        return (
            ray.remote(_KleinStateActor)
            .options(
                num_cpus=0,
                name=KLEIN_STATE_ACTOR_NAME,
                namespace=KLEIN_STATE_ACTOR_NAMESPACE,
                get_if_exists=True,
                lifetime="detached",
                resources={"node:__internal_head__": 0.001},
                scheduling_strategy="DEFAULT",
                max_concurrency=16,
                max_restarts=-1,
                max_task_retries=-1,
            )
            .remote(history_size=history_size)
        )


def get_state_actor() -> ActorHandle | None:
    """Return the state actor when Klein has published at least one job."""

    try:
        return ray.get_actor(
            name=KLEIN_STATE_ACTOR_NAME,
            namespace=KLEIN_STATE_ACTOR_NAMESPACE,
        )
    except ValueError:
        return None


def register_job(
    *,
    job_id: str,
    job_name: str,
    runtime_mode: str,
    namespace: str | None,
    manager: ActorHandle,
    config: Configuration,
) -> None:
    """Register one JobManager without delaying the submission hot path."""

    if not config.get(ObservabilityOptions.DASHBOARD_ENABLED):
        return
    actor = get_or_create_state_actor(config)
    actor.register_job.remote(
        job_id,
        manager,
        {
            "job_id": job_id,
            "job_name": job_name,
            "runtime_mode": runtime_mode,
            "namespace": namespace,
            "status": "RUNNING",
        },
    )


def _with_interval_metrics(
    snapshot: dict[str, Any],
    previous: dict[str, Any] | None,
    elapsed_ms: int | None,
) -> dict[str, Any]:
    """Derive Flink-style rates and busy/backpressure percentages."""

    previous_operators = {operator["op_id"]: operator for operator in (previous or {}).get("operators", [])}
    interval_ns = max(elapsed_ms or 0, 0) * 1_000_000
    for operator in snapshot.get("operators", []):
        prior = previous_operators.get(operator["op_id"])
        _apply_interval_metrics(operator, prior, interval_ns, max(int(operator.get("parallelism", 1)), 1))
        previous_subtasks = {subtask["subtask_index"]: subtask for subtask in (prior or {}).get("subtasks", [])}
        for subtask in operator.get("subtasks", []):
            _apply_interval_metrics(
                subtask,
                previous_subtasks.get(subtask["subtask_index"]),
                interval_ns,
                1,
            )
        subtasks = operator.get("subtasks", [])
        operator["max_busy_percent"] = _maximum_percent(subtasks, operator, "busy_percent")
        operator["max_backpressure_percent"] = _maximum_percent(
            subtasks,
            operator,
            "backpressure_percent",
        )
    return snapshot


def _apply_interval_metrics(
    current: dict[str, Any],
    previous: dict[str, Any] | None,
    interval_ns: int,
    parallelism: int,
) -> None:
    if previous is None or interval_ns <= 0:
        current.update(
            {
                "rows_in_per_second": 0.0,
                "rows_out_per_second": 0.0,
                "bytes_in_per_second": 0.0,
                "bytes_out_per_second": 0.0,
                "busy_percent": 0.0,
                "backpressure_percent": 0.0,
            }
        )
        return
    elapsed_seconds = interval_ns / 1_000_000_000
    available_ns = interval_ns * max(parallelism, 1)
    current.update(
        {
            "rows_in_per_second": _nonnegative_delta(current, previous, "rows_in") / elapsed_seconds,
            "rows_out_per_second": _nonnegative_delta(current, previous, "rows_out") / elapsed_seconds,
            "bytes_in_per_second": _nonnegative_delta(current, previous, "bytes_in") / elapsed_seconds,
            "bytes_out_per_second": _nonnegative_delta(current, previous, "bytes_out") / elapsed_seconds,
            "busy_percent": _percent_delta(current, previous, "busy_ns", available_ns),
            "backpressure_percent": _percent_delta(current, previous, "backpressure_ns", available_ns),
        }
    )


def _nonnegative_delta(current: dict[str, Any], previous: dict[str, Any], key: str) -> float:
    return max(float(current.get(key, 0)) - float(previous.get(key, 0)), 0.0)


def _percent_delta(current: dict[str, Any], previous: dict[str, Any], key: str, available_ns: int) -> float:
    if available_ns <= 0:
        return 0.0
    return min(100.0, 100.0 * _nonnegative_delta(current, previous, key) / available_ns)


def _maximum_percent(
    subtasks: list[dict[str, Any]],
    operator: dict[str, Any],
    key: str,
) -> float:
    """Return the hottest subtask value used by the topology visualization."""

    if not subtasks:
        return float(operator.get(key, 0.0))
    return max(float(subtask.get(key, 0.0)) for subtask in subtasks)
