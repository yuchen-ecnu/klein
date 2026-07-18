# SPDX-License-Identifier: Apache-2.0

from click.testing import CliRunner

from ray.klein.cli import klein_cli_group


def test_cli_help_lists_operator_commands() -> None:
    result = CliRunner().invoke(klein_cli_group, ["--help"])

    assert result.exit_code == 0
    assert "Klein streaming job management" in result.output
    for command in ("attach", "list", "status", "stop"):
        assert command in result.output


def test_cli_version_uses_distribution_metadata() -> None:
    result = CliRunner().invoke(klein_cli_group, ["--version"])

    assert result.exit_code == 0
    assert result.output.startswith("klein, version ")


def test_cli_list_reports_empty_cluster(monkeypatch) -> None:
    monkeypatch.setattr("ray.klein.cli._discover_jobs", list)

    result = CliRunner().invoke(klein_cli_group, ["list"])

    assert result.exit_code == 0
    assert result.output == "No running Klein jobs found.\n"
