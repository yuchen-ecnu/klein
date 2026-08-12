# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
POLICY_MODULE = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_coverage_policy.py"))
component_coverage = POLICY_MODULE["component_coverage"]
file_coverage = POLICY_MODULE["file_coverage"]
COMPONENT_POLICIES = POLICY_MODULE["COMPONENT_POLICIES"]
FILE_POLICIES = POLICY_MODULE["FILE_POLICIES"]


def _report(*, covered_lines: int, statements: int, covered_branches: int, branches: int) -> dict:
    return {
        "files": {
            "src/ray/klein/state/backend.py": {
                "summary": {
                    "covered_lines": covered_lines,
                    "num_statements": statements,
                    "covered_branches": covered_branches,
                    "num_branches": branches,
                }
            }
        }
    }


def test_component_coverage_combines_lines_and_branches() -> None:
    report = _report(covered_lines=8, statements=10, covered_branches=3, branches=5)

    assert component_coverage(report, "src/ray/klein/state/") == pytest.approx(11 / 15 * 100)


def test_component_coverage_requires_matching_source_files() -> None:
    with pytest.raises(ValueError, match="contains no files"):
        component_coverage(_report(covered_lines=1, statements=1, covered_branches=0, branches=0), "missing/")


def test_file_coverage_requires_the_exact_risk_sensitive_module() -> None:
    report = _report(covered_lines=8, statements=10, covered_branches=3, branches=5)

    assert file_coverage(report, "src/ray/klein/state/backend.py") == pytest.approx(11 / 15 * 100)
    with pytest.raises(ValueError, match="contains no file"):
        file_coverage(report, "src/ray/klein/state/missing.py")


def test_every_high_risk_runtime_component_has_an_explicit_floor() -> None:
    assert {
        "api",
        "collector",
        "coordinator",
        "execution-graph",
        "job-manager",
        "scheduler",
        "sql",
        "state",
        "worker",
    } <= COMPONENT_POLICIES.keys()
    assert {"checkpoint-coordinator", "emit-pipeline", "job-master", "stream-task", "streaming-rules"} == set(
        FILE_POLICIES
    )
