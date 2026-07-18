# SPDX-License-Identifier: Apache-2.0
"""Stable, JSON-safe cluster state API for operations integrations."""

from __future__ import annotations

from typing import Any

import ray
from ray.klein.observability.dashboard.state_actor import get_state_actor


def list_job_snapshots() -> list[dict[str, Any]]:
    """Return current and retained Klein job snapshots from this Ray cluster."""
    actor = get_state_actor()
    if actor is None:
        return []
    return ray.get(actor.get_jobs.remote())


def get_job_snapshot(job_id: str) -> dict[str, Any] | None:
    """Return one job snapshot, or ``None`` when the job is unknown."""
    if not job_id:
        raise ValueError("job_id cannot be empty")
    actor = get_state_actor()
    if actor is None:
        return None
    return ray.get(actor.get_job.remote(job_id))


def cancel_job(job_id: str, *, timeout: int = 60) -> bool:
    """Cancel one published job by ID and wait for the state actor response."""
    if not job_id:
        raise ValueError("job_id cannot be empty")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    actor = get_state_actor()
    if actor is None:
        return False
    return ray.get(actor.cancel_job.remote(job_id, timeout))
