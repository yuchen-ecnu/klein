# SPDX-License-Identifier: Apache-2.0
"""Tests for reviewed dependency-license exceptions."""

from __future__ import annotations

import runpy
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised by the Python 3.10 CI job
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).parents[2]
LICENSE_OVERRIDES = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_dependency_licenses.py"))["LICENSE_OVERRIDES"]
EXPECTED_OVERRIDES = {
    "fsspec": (
        "BSD-3-Clause",
        "https://github.com/fsspec/filesystem_spec/blob/2026.7.0/LICENSE",
    ),
    "rocketmq-client-python": (
        "Apache-2.0",
        "https://github.com/apache/rocketmq-client-python/blob/master/LICENSE",
    ),
}


def test_license_overrides_are_exact_pins_with_public_evidence() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    declared = [*project["dependencies"], *project["optional-dependencies"]["all"]]

    assert set(LICENSE_OVERRIDES) == set(EXPECTED_OVERRIDES)
    for package, override in LICENSE_OVERRIDES.items():
        expected_license, expected_evidence = EXPECTED_OVERRIDES[package]
        assert f"{package}=={override.version}" in declared
        assert override.license_expression == expected_license
        assert override.evidence_url == expected_evidence
