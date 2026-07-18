# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

import pytest

from ray.klein.observability import state_api


def test_list_job_snapshots_without_state_actor(monkeypatch) -> None:
    monkeypatch.setattr(state_api, "get_state_actor", lambda: None)
    assert state_api.list_job_snapshots() == []


def test_state_api_resolves_remote_calls(monkeypatch) -> None:
    actor = MagicMock()
    actor.get_jobs.remote.return_value = "jobs-ref"
    actor.get_job.remote.return_value = "job-ref"
    actor.cancel_job.remote.return_value = "cancel-ref"
    monkeypatch.setattr(state_api, "get_state_actor", lambda: actor)
    monkeypatch.setattr(
        state_api.ray,
        "get",
        lambda ref: {"jobs-ref": [], "job-ref": {"job_id": "j"}, "cancel-ref": True}[ref],
    )

    assert state_api.list_job_snapshots() == []
    assert state_api.get_job_snapshot("j") == {"job_id": "j"}
    assert state_api.cancel_job("j", timeout=9)
    actor.get_job.remote.assert_called_once_with("j")
    actor.cancel_job.remote.assert_called_once_with("j", 9)


@pytest.mark.parametrize("function", [state_api.get_job_snapshot, state_api.cancel_job])
def test_state_api_rejects_empty_job_id(function) -> None:
    with pytest.raises(ValueError, match="job_id cannot be empty"):
        function("")


def test_cancel_job_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        state_api.cancel_job("j", timeout=0)
