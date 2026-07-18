# SPDX-License-Identifier: Apache-2.0
import json
import logging

import pytest

from ray.klein._internal.logging import (
    _ray_log_context,
    configure_logging,
    get_logger,
    log_context,
    log_event,
    reset_logging,
)


@pytest.fixture(autouse=True)
def isolated_klein_logging():
    reset_logging()
    yield
    reset_logging()


def test_json_logs_are_structured_redacted_and_written_to_stderr(capsys) -> None:
    configure_logging(level="DEBUG", log_format="json")

    with log_context(job_id="job-7", api_token="do-not-log"):
        log_event(
            get_logger("tests.logging"),
            logging.INFO,
            "job.status.changed",
            "Job %s is %s",
            "example",
            "RUNNING",
            operator_id=3,
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert not captured.out
    assert payload["event"] == "job.status.changed"
    assert payload["component"] == "tests.logging"
    assert payload["message"] == "Job example is RUNNING"
    assert payload["job_id"] == "job-7"
    assert payload["operator_id"] == 3
    assert payload["api_token"] == "<redacted>"


def test_text_logs_include_stable_event_and_context(capsys) -> None:
    configure_logging(level="INFO", log_format="text")

    with log_context(checkpoint_id=12):
        log_event(get_logger("tests.logging"), logging.INFO, "checkpoint.completed", "Checkpoint completed")

    captured = capsys.readouterr()
    assert not captured.out
    assert "event=checkpoint.completed" in captured.err
    assert "checkpoint_id=12" in captured.err
    assert "Checkpoint completed" in captured.err


def test_invalid_log_level_and_format_are_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown Klein log level"):
        configure_logging(level="LOUD")
    with pytest.raises(ValueError, match="must be 'text' or 'json'"):
        configure_logging(log_format="xml")


def test_reset_restores_library_log_propagation() -> None:
    configure_logging(level="INFO")
    assert get_logger().propagate is False

    reset_logging()

    assert get_logger().propagate is True
    assert len(get_logger().handlers) == 1
    assert isinstance(get_logger().handlers[0], logging.NullHandler)


def test_ray_log_context_uses_the_public_runtime_context_api(monkeypatch) -> None:
    class RuntimeContext:
        namespace = "klein-job-7"

        @staticmethod
        def get_actor_name() -> str:
            return "JobManager"

    monkeypatch.setattr("ray.is_initialized", lambda: True)
    monkeypatch.setattr("ray.get_runtime_context", RuntimeContext)

    assert _ray_log_context() == {"job_id": "klein-job-7", "task_name": "JobManager"}
