# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
POLICY_MODULE = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_coverage_policy.py"))
component_coverage = POLICY_MODULE["component_coverage"]


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
