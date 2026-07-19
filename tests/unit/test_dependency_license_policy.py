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


def test_license_overrides_are_exact_pins_with_public_evidence() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    declared = [*project["dependencies"], *project["optional-dependencies"]["all"]]

    for package, override in LICENSE_OVERRIDES.items():
        assert f"{package}=={override.version}" in declared
        assert override.license_expression == "Apache-2.0"
        assert override.evidence_url.startswith("https://github.com/apache/")
