# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

import pytest

from ray.klein.observability import state_api


def test_list_job_snapshots_without_state_actor(monkeypatch) -> None:
    monkeypatch.setattr(state_api, "get_state_actor", lambda: None)
    assert state_api.list_job_snapshots() == []


def test_rescale_operator_without_state_actor(monkeypatch) -> None:
    monkeypatch.setattr(state_api, "get_state_actor", lambda: None)
    assert state_api.rescale_operator("j", 2, 4) is None


def test_state_api_resolves_remote_calls(monkeypatch) -> None:
    actor = MagicMock()
    actor.get_jobs.remote.return_value = "jobs-ref"
    actor.get_job.remote.return_value = "job-ref"
    actor.cancel_job.remote.return_value = "cancel-ref"
    actor.rescale_operator.remote.return_value = "rescale-ref"
    monkeypatch.setattr(state_api, "get_state_actor", lambda: actor)
    ray_get = MagicMock(
        side_effect=lambda ref, **_kwargs: {
            "jobs-ref": [],
            "job-ref": {"job_id": "j"},
            "cancel-ref": True,
            "rescale-ref": {"status": "COMPLETED"},
        }[ref]
    )
    monkeypatch.setattr(state_api.ray, "get", ray_get)

    assert state_api.list_job_snapshots() == []
    assert state_api.get_job_snapshot("j") == {"job_id": "j"}
    assert state_api.cancel_job("j", timeout=9)
    assert state_api.rescale_operator("j", 2, 4, timeout=9) == {"status": "COMPLETED"}
    actor.get_job.remote.assert_called_once_with("j")
    actor.cancel_job.remote.assert_called_once_with("j", 9)
    actor.rescale_operator.remote.assert_called_once_with("j", 2, 4, 9)
    ray_get.assert_called_with("rescale-ref", timeout=14)


@pytest.mark.parametrize("function", [state_api.get_job_snapshot, state_api.cancel_job])
def test_state_api_rejects_empty_job_id(function) -> None:
    with pytest.raises(ValueError, match="job_id cannot be empty"):
        function("")


def test_cancel_job_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        state_api.cancel_job("j", timeout=0)


@pytest.mark.parametrize(
    ("args", "error", "message"),
    [
        (("", 1, 2), ValueError, "job_id cannot be empty"),
        (("j", True, 2), TypeError, "operator_id must be an integer"),
        (("j", -1, 2), ValueError, "operator_id must be non-negative"),
        (("j", 1, True), TypeError, "parallelism must be an integer"),
        (("j", 1, 0), ValueError, "parallelism must be at least 1"),
    ],
)
def test_rescale_operator_validates_identity_and_parallelism(args, error, message) -> None:
    with pytest.raises(error, match=message):
        state_api.rescale_operator(*args)


@pytest.mark.parametrize(
    ("timeout", "error", "message"),
    [
        (True, TypeError, "timeout must be a number"),
        ("slow", TypeError, "timeout must be a number"),
        (0, ValueError, "greater than zero"),
        (float("nan"), ValueError, "timeout must be finite"),
        (float("inf"), ValueError, "timeout must be finite"),
    ],
)
def test_rescale_operator_validates_timeout(timeout, error, message) -> None:
    with pytest.raises(error, match=message):
        state_api.rescale_operator("j", 1, 2, timeout=timeout)
