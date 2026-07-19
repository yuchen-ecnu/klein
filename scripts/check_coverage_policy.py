# SPDX-License-Identifier: Apache-2.0
"""Enforce risk-based line-and-branch coverage floors by source component."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class CoveragePolicy:
    source_prefix: str
    minimum_percent: float


COMPONENT_POLICIES = {
    "configuration": CoveragePolicy("src/ray/klein/config/", 82.0),
    "connectors": CoveragePolicy("src/ray/klein/integrations/", 70.0),
    "coordinator": CoveragePolicy("src/ray/klein/runtime/coordinator/", 65.0),
    "event-time": CoveragePolicy("src/ray/klein/runtime/event_time/", 85.0),
    "observability": CoveragePolicy("src/ray/klein/observability/", 60.0),
    "partitioning": CoveragePolicy("src/ray/klein/runtime/partitioning/", 82.0),
    "state": CoveragePolicy("src/ray/klein/state/", 82.0),
}


def component_coverage(report: dict[str, Any], source_prefix: str) -> float:
    summaries = [details["summary"] for path, details in report["files"].items() if path.startswith(source_prefix)]
    if not summaries:
        raise ValueError(f"coverage report contains no files below {source_prefix}")
    covered = sum(summary["covered_lines"] + summary.get("covered_branches", 0) for summary in summaries)
    statements = sum(summary["num_statements"] + summary.get("num_branches", 0) for summary in summaries)
    return 100.0 if statements == 0 else covered * 100.0 / statements


def check_coverage(report: dict[str, Any]) -> dict[str, float]:
    results = {name: component_coverage(report, policy.source_prefix) for name, policy in COMPONENT_POLICIES.items()}
    failures = [
        f"{name} {results[name]:.2f}% < {policy.minimum_percent:.2f}%"
        for name, policy in COMPONENT_POLICIES.items()
        if results[name] < policy.minimum_percent
    ]
    if failures:
        raise ValueError("component coverage floor failed: " + ", ".join(failures))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", nargs="?", type=Path, default=Path("coverage.json"))
    args = parser.parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    results = check_coverage(report)
    for name, percent in results.items():
        print(f"{name}: {percent:.2f}%")


if __name__ == "__main__":
    main()
