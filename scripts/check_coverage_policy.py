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
    "api": CoveragePolicy("src/ray/klein/api/", 85.0),
    "collector": CoveragePolicy("src/ray/klein/runtime/collector/", 78.0),
    "configuration": CoveragePolicy("src/ray/klein/config/", 90.0),
    "connectors": CoveragePolicy("src/ray/klein/integrations/", 81.0),
    "coordinator": CoveragePolicy("src/ray/klein/runtime/coordinator/", 76.0),
    "event-time": CoveragePolicy("src/ray/klein/runtime/event_time/", 87.0),
    "execution-graph": CoveragePolicy("src/ray/klein/runtime/execution_graph/", 85.0),
    "job-manager": CoveragePolicy("src/ray/klein/runtime/job_manager/", 84.0),
    "observability": CoveragePolicy("src/ray/klein/observability/", 87.0),
    "partitioning": CoveragePolicy("src/ray/klein/runtime/partitioning/", 86.0),
    "scheduler": CoveragePolicy("src/ray/klein/runtime/scheduler/", 75.0),
    "sql": CoveragePolicy("src/ray/klein/_internal/sql/", 80.0),
    "state": CoveragePolicy("src/ray/klein/state/", 84.0),
    "worker": CoveragePolicy("src/ray/klein/runtime/worker/", 72.0),
}

FILE_POLICIES = {
    "checkpoint-coordinator": CoveragePolicy(
        "src/ray/klein/runtime/coordinator/checkpoint_coordinator.py",
        74.0,
    ),
    "emit-pipeline": CoveragePolicy("src/ray/klein/runtime/worker/emit_pipeline.py", 59.0),
    "job-master": CoveragePolicy("src/ray/klein/runtime/scheduler/job_master.py", 57.0),
    "stream-task": CoveragePolicy("src/ray/klein/runtime/worker/stream_task.py", 57.5),
    "streaming-rules": CoveragePolicy("src/ray/klein/runtime/graph/rules.py", 53.0),
}


def component_coverage(report: dict[str, Any], source_prefix: str) -> float:
    summaries = [details["summary"] for path, details in report["files"].items() if path.startswith(source_prefix)]
    if not summaries:
        raise ValueError(f"coverage report contains no files below {source_prefix}")
    covered = sum(summary["covered_lines"] + summary.get("covered_branches", 0) for summary in summaries)
    statements = sum(summary["num_statements"] + summary.get("num_branches", 0) for summary in summaries)
    return 100.0 if statements == 0 else covered * 100.0 / statements


def file_coverage(report: dict[str, Any], source_path: str) -> float:
    try:
        summary = report["files"][source_path]["summary"]
    except KeyError as error:
        raise ValueError(f"coverage report contains no file {source_path}") from error
    covered = summary["covered_lines"] + summary.get("covered_branches", 0)
    statements = summary["num_statements"] + summary.get("num_branches", 0)
    return 100.0 if statements == 0 else covered * 100.0 / statements


def check_coverage(report: dict[str, Any]) -> dict[str, float]:
    results = {name: component_coverage(report, policy.source_prefix) for name, policy in COMPONENT_POLICIES.items()}
    file_results = {name: file_coverage(report, policy.source_prefix) for name, policy in FILE_POLICIES.items()}
    failures = [
        f"{name} {results[name]:.2f}% < {policy.minimum_percent:.2f}%"
        for name, policy in COMPONENT_POLICIES.items()
        if results[name] < policy.minimum_percent
    ]
    failures.extend(
        f"file:{name} {file_results[name]:.2f}% < {policy.minimum_percent:.2f}%"
        for name, policy in FILE_POLICIES.items()
        if file_results[name] < policy.minimum_percent
    )
    if failures:
        raise ValueError("coverage policy failed: " + ", ".join(failures))
    return {**results, **{f"file:{name}": value for name, value in file_results.items()}}


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
