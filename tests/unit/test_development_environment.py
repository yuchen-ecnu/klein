# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]
DOCTOR = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_development_environment.py"))
collect_problems = DOCTOR["collect_problems"]
parse_version = DOCTOR["parse_version"]


def test_development_environment_accepts_the_supported_toolchain() -> None:
    assert collect_problems((3, 12, 1), lambda _command: "/bin/tool", "v22.22.3") == []


def test_development_environment_reports_versions_and_missing_commands() -> None:
    available = {"node"}
    problems = collect_problems((3, 13, 0), lambda command: "/bin/tool" if command in available else None, "v20.1.0")

    assert problems[0].startswith("Python 3.13 is unsupported")
    assert "missing commands:" in problems[1]
    assert problems[2].startswith("Node.js 22.22.0 or newer")


def test_development_environment_parses_prefixed_semver() -> None:
    assert parse_version("v22.22.3") == (22, 22, 3)
    assert parse_version("unknown") is None
