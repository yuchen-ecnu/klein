# SPDX-License-Identifier: Apache-2.0
"""Stable, JSON-safe cluster state API for operations integrations."""

from __future__ import annotations

import math
from typing import Any, cast

import ray
from ray.klein.observability.dashboard.state_actor import get_state_actor

_CONTROL_RESPONSE_GRACE_SECONDS = 5


def list_job_snapshots() -> list[dict[str, Any]]:
    """Return current and retained Klein job snapshots from this Ray cluster."""
    actor = get_state_actor()
    if actor is None:
        return []
    return cast(list[dict[str, Any]], ray.get(actor.get_jobs.remote()))


def get_job_snapshot(job_id: str) -> dict[str, Any] | None:
    """Return one job snapshot, or ``None`` when the job is unknown."""
    if not job_id:
        raise ValueError("job_id cannot be empty")
    actor = get_state_actor()
    if actor is None:
        return None
    return cast(dict[str, Any] | None, ray.get(actor.get_job.remote(job_id)))


def cancel_job(job_id: str, *, timeout: int = 60) -> bool:
    """Cancel one published job by ID and wait for the state actor response."""
    if not job_id:
        raise ValueError("job_id cannot be empty")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero")
    actor = get_state_actor()
    if actor is None:
        return False
    return cast(bool, ray.get(actor.cancel_job.remote(job_id, timeout)))


def rescale_operator(
    job_id: str,
    operator_id: int,
    parallelism: int,
    *,
    timeout: float = 60,
) -> dict[str, Any] | None:
    """Change one published operator's parallelism.

    ``None`` means that the job is not published in this cluster.  Operator- or
    runtime-level rejection is returned as a JSON-safe operation result with a
    ``REJECTED`` or ``FAILED`` status. A timeout only bounds this client's wait;
    callers should refresh the job snapshot before retrying because the remote
    operation may still reach a terminal result.
    """

    _validate_rescale_request(job_id, operator_id, parallelism, timeout)
    actor = get_state_actor()
    if actor is None:
        return None
    return cast(
        dict[str, Any] | None,
        ray.get(
            actor.rescale_operator.remote(job_id, operator_id, parallelism, timeout),
            timeout=timeout + _CONTROL_RESPONSE_GRACE_SECONDS,
        ),
    )


def submit_operator_rescale(
    job_id: str,
    operator_id: int,
    parallelism: int,
    *,
    timeout: float = 10,
) -> dict[str, Any] | None:
    """Submit an operator rescale and return its admission record.

    An ``ACCEPTED`` result continues in the JobManager after this function
    returns.  Its current and terminal state is available through
    :func:`get_job_snapshot` in ``rescale_operations`` and the matching
    operator's ``rescale_operation`` field.
    """

    _validate_rescale_request(job_id, operator_id, parallelism, timeout)
    actor = get_state_actor()
    if actor is None:
        return None
    return ray.get(
        actor.submit_operator_rescale.remote(job_id, operator_id, parallelism, timeout),
        timeout=timeout + _CONTROL_RESPONSE_GRACE_SECONDS,
    )


def _validate_rescale_request(
    job_id: str,
    operator_id: int,
    parallelism: int,
    timeout: float,
) -> None:
    if not job_id:
        raise ValueError("job_id cannot be empty")
    if isinstance(operator_id, bool) or not isinstance(operator_id, int):
        raise TypeError("operator_id must be an integer")
    if operator_id < 0:
        raise ValueError("operator_id must be non-negative")
    if isinstance(parallelism, bool) or not isinstance(parallelism, int):
        raise TypeError("parallelism must be an integer")
    if parallelism < 1:
        raise ValueError("parallelism must be at least 1")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("timeout must be a number")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and greater than zero")
