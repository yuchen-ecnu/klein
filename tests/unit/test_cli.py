# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import click
import pytest
from click.testing import CliRunner

from ray.klein import cli
from ray.klein.api.job_status import JobStatus


def _job(namespace: str, state: str, *, name: str | None = None, stale: bool = False) -> cli._JobInfo:
    return {
        "namespace": namespace,
        "job_name": name or namespace,
        "job_state": state,
        "actor_state": "STALE" if stale else "PUBLISHED",
        "dashboard_stale": stale,
    }


def test_cli_help_lists_operator_commands() -> None:
    result = CliRunner().invoke(cli.klein_cli_group, ["--help"])

    assert result.exit_code == 0
    assert "Klein streaming job management" in result.output
    for command in ("attach", "cancel", "dashboard", "list", "status", "stop"):
        assert command in result.output


def test_cli_version_uses_distribution_metadata() -> None:
    result = CliRunner().invoke(cli.klein_cli_group, ["--version"])

    assert result.exit_code == 0
    assert result.output.startswith("klein, version ")


def test_cli_list_reports_empty_cluster(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_discover_jobs", list)

    result = CliRunner().invoke(cli.klein_cli_group, ["list"])

    assert result.exit_code == 0
    assert result.output == "No running Klein jobs found.\n"


def test_cli_list_filters_terminal_jobs_by_default(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "_discover_jobs",
        lambda: [
            _job("orders-production", "RUNNING", name="Orders Pipeline"),
            _job("klein-finished-12345678", "FINISHED", name="Finished Pipeline"),
        ],
    )

    result = CliRunner().invoke(cli.klein_cli_group, ["list"])

    assert result.exit_code == 0
    assert "Orders Pipeline" in result.output
    assert "Finished Pipeline" not in result.output


def test_cli_list_all_json_is_machine_readable(monkeypatch) -> None:
    jobs = [
        _job("orders-production", "RUNNING", name="Orders Pipeline"),
        _job("klein-finished-12345678", "FINISHED", name="Finished Pipeline", stale=True),
    ]
    monkeypatch.setattr(cli, "_discover_jobs", lambda: jobs)

    result = CliRunner().invoke(cli.klein_cli_group, ["list", "--all", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == jobs


def test_discovery_keeps_explicit_namespace(monkeypatch) -> None:
    reference = object()
    manager = SimpleNamespace(job_status=lambda: reference)
    monkeypatch.setattr(cli.klein, "get_actor_by_name", lambda *_args, **_kwargs: manager)

    job, status_reference = cli._named_actor_job_info("orders-production")

    assert job is not None
    assert job["namespace"] == "orders-production"
    assert status_reference is reference


def test_actor_discovery_uses_cross_namespace_named_actor_api(monkeypatch) -> None:
    reference = object()
    manager = SimpleNamespace(job_status=lambda: reference)
    monkeypatch.setattr(
        "ray.util.list_named_actors",
        lambda *, all_namespaces: [
            {"name": "unrelated", "namespace": "other"},
            {"name": cli.ComponentName.KLEIN_JOB_MANAGER, "namespace": "orders-production"},
        ],
    )
    monkeypatch.setattr(cli.klein, "get_actor_by_name", lambda *_args, **_kwargs: manager)
    monkeypatch.setattr(cli.ray, "wait", lambda *_args, **_kwargs: ([reference], []))
    monkeypatch.setattr(cli.klein, "get", lambda _reference: JobStatus.RUNNING)

    expected = _job("orders-production", "RUNNING")
    expected["actor_state"] = "ALIVE"
    assert cli._discover_actor_jobs() == [expected]


def test_discovery_prefers_exact_published_metadata(monkeypatch) -> None:
    actor_job = _job("klein-orders-12345678", "RUNNING", name="orders")
    published_job = _job("klein-orders-12345678", "RUNNING", name="Orders ETL 🚀")
    monkeypatch.setattr(cli, "_ensure_ray_init", lambda: None)
    monkeypatch.setattr(cli, "_discover_actor_jobs", lambda: [actor_job])
    monkeypatch.setattr(cli, "_discover_published_jobs", lambda: [published_job])

    assert cli._discover_jobs() == [published_job]


def test_discovery_uses_published_jobs_when_actor_state_api_is_unavailable(monkeypatch) -> None:
    published_job = _job("orders-production", "RUNNING", name="Orders Pipeline")
    monkeypatch.setattr(cli, "_ensure_ray_init", lambda: None)
    monkeypatch.setattr(cli, "_discover_published_jobs", lambda: [published_job])
    monkeypatch.setattr(
        cli,
        "_discover_actor_jobs",
        lambda: (_ for _ in ()).throw(click.ClickException("state API unavailable")),
    )

    assert cli._discover_jobs() == [published_job]


def test_cli_status_renders_operational_details(monkeypatch) -> None:
    snapshot = {
        "job_name": "Orders Pipeline",
        "status": "FAILED",
        "dashboard_stale": True,
        "dashboard_error": "actor unavailable",
        "overview": {"operators": 1, "task_instances": 2, "rows_in": 10, "rows_out": 8, "restarts": 1},
        "operators": [
            {
                "name": "enrich",
                "status": "failed",
                "parallelism": 2,
                "rows_in": 10,
                "rows_out": 8,
                "backpressure_percent": 25.0,
            }
        ],
        "checkpoints": {
            "summary": {"completed": 3, "failed": 1, "in_progress": 0},
            "latest_path": "s3://checkpoints/3",
        },
        "failure": "connector failed",
    }
    monkeypatch.setattr(cli, "_get_job_snapshot", lambda _namespace: snapshot)

    result = CliRunner().invoke(cli.klein_cli_group, ["status", "orders-production"])

    assert result.exit_code == 0
    assert "Orders Pipeline" in result.output
    assert "showing the last known snapshot" in result.output
    assert "rows_in=10" in result.output
    assert "bp=25%" in result.output
    assert "latest checkpoint: s3://checkpoints/3" in result.output
    assert "failure: connector failed" in result.output


def test_cli_status_json_emits_complete_snapshot(monkeypatch) -> None:
    snapshot = {"job_id": "job-1", "status": "RUNNING", "operators": []}
    monkeypatch.setattr(cli, "_get_job_snapshot", lambda _namespace: snapshot)

    result = CliRunner().invoke(cli.klein_cli_group, ["status", "job-1", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.output) == snapshot


def test_multiple_jobs_require_namespace_without_interactive_input(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_discover_jobs", lambda: [_job("job-1", "RUNNING"), _job("job-2", "RUNNING")])
    monkeypatch.setattr(cli, "_stdin_is_tty", lambda: False)

    result = CliRunner().invoke(cli.klein_cli_group, ["status"])

    assert result.exit_code == 2
    assert "NAMESPACE is required" in result.output


def test_interactive_picker_reprompts_for_out_of_range_choice(monkeypatch) -> None:
    jobs = [_job("job-1", "RUNNING"), _job("job-2", "RUNNING")]
    requested = []
    monkeypatch.setattr(cli, "_discover_jobs", lambda: jobs)
    monkeypatch.setattr(cli, "_stdin_is_tty", lambda: True)
    monkeypatch.setattr(
        cli,
        "_get_job_snapshot",
        lambda namespace: requested.append(namespace) or {"job_name": namespace, "status": "RUNNING", "operators": []},
    )

    result = CliRunner().invoke(cli.klein_cli_group, ["status"], input="9\n2\n")

    assert result.exit_code == 0
    assert "9 is not in the range 1<=x<=2" in result.output
    assert requested == ["job-2"]


def test_attach_requires_tty_before_cluster_lookup(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_stdout_is_tty", lambda: False)
    monkeypatch.setattr(
        cli,
        "_connect",
        lambda _namespace: pytest.fail("attach should reject non-TTY output before connecting"),
    )

    result = CliRunner().invoke(cli.klein_cli_group, ["attach", "job-1"])

    assert result.exit_code == 2
    assert "attach requires an interactive terminal" in result.output


def test_attached_poll_returns_when_terminal_reference_is_ready(monkeypatch) -> None:
    terminal_reference = object()
    monkeypatch.setattr(cli.ray, "wait", lambda *_args, **_kwargs: ([terminal_reference], []))
    monkeypatch.setattr(cli.klein, "get", lambda *_args, **_kwargs: JobStatus.FINISHED)

    result = cli._poll_attached_progress(
        SimpleNamespace(),
        terminal_reference,
        SimpleNamespace(),
        threading.Event(),
        threading.Event(),
        {"rows": 0},
    )

    assert result == JobStatus.FINISHED


@pytest.mark.parametrize("command", ["cancel", "stop"])
def test_cancel_and_stop_alias_report_success(command, monkeypatch) -> None:
    manager = SimpleNamespace(cancel=lambda **_kwargs: "cancel-reference")
    monkeypatch.setattr(cli, "_get_published_snapshot", lambda _namespace: {"status": "RUNNING", "job_name": "orders"})
    monkeypatch.setattr(cli, "_connect", lambda _namespace: manager)
    monkeypatch.setattr(cli, "_get_status", lambda _manager: JobStatus.RUNNING)
    monkeypatch.setattr(cli.klein, "get", lambda *_args, **_kwargs: True)

    result = CliRunner().invoke(cli.klein_cli_group, [command, "orders-production", "--force"])

    assert result.exit_code == 0
    assert "Job cancelled" in result.output


def test_cancel_does_not_report_false_success(monkeypatch) -> None:
    manager = SimpleNamespace(cancel=lambda **_kwargs: "cancel-reference")
    statuses = iter((JobStatus.RUNNING, JobStatus.RUNNING))
    monkeypatch.setattr(cli, "_get_published_snapshot", lambda _namespace: {"status": "RUNNING"})
    monkeypatch.setattr(cli, "_connect", lambda _namespace: manager)
    monkeypatch.setattr(cli, "_get_status", lambda _manager: next(statuses))
    monkeypatch.setattr(cli.klein, "get", lambda *_args, **_kwargs: False)

    result = CliRunner().invoke(cli.klein_cli_group, ["cancel", "orders-production", "--force"])

    assert result.exit_code == 1
    assert "did not acknowledge cancellation" in result.output
    assert "Job cancelled" not in result.output


def test_cancel_is_idempotent_for_retained_terminal_job(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_get_published_snapshot", lambda _namespace: {"status": "FINISHED"})
    monkeypatch.setattr(
        cli,
        "_connect",
        lambda _namespace: pytest.fail("a retained terminal job should not require a live JobManager"),
    )

    result = CliRunner().invoke(cli.klein_cli_group, ["cancel", "job-1", "--force"])

    assert result.exit_code == 0
    assert "already FINISHED" in result.output


def test_ray_connection_failure_is_a_friendly_cli_error(monkeypatch) -> None:
    monkeypatch.setattr(cli.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(cli.ray, "init", lambda **_kwargs: (_ for _ in ()).throw(ConnectionError("cluster down")))

    result = CliRunner().invoke(cli.klein_cli_group, ["list"])

    assert result.exit_code == 1
    assert "Cannot connect to a Ray cluster" in result.output
    assert "Traceback" not in result.output
