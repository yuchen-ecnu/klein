# SPDX-License-Identifier: Apache-2.0
"""Tests for reviewed dependency-license exceptions."""

from __future__ import annotations

import runpy
from pathlib import Path
from unittest.mock import patch

import pytest
from packaging.requirements import Requirement

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised by the Python 3.10 CI job
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).parents[2]
POLICY = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_dependency_licenses.py"))
LICENSE_OVERRIDES = POLICY["LICENSE_OVERRIDES"]
LICENSE_PARSER_EXCEPTIONS = POLICY["LICENSE_PARSER_EXCEPTIONS"]
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


def test_license_parser_exceptions_are_bound_to_exact_metadata_and_constraints() -> None:
    constraints = (PROJECT_ROOT / "requirements" / "ci-constraints.txt").read_text(encoding="utf-8")

    assert set(LICENSE_PARSER_EXCEPTIONS) == {"pyvips-binary"}
    exception = LICENSE_PARSER_EXCEPTIONS["pyvips-binary"]
    assert exception.metadata_license == "LGPL-3.0-or-later"
    assert exception.ignored_fragment == "GNU LESSER GENERAL PUBLIC LICENSE V3"
    assert exception.evidence_url.endswith("/v8.18.4/pyproject.toml")
    assert f"pyvips-binary=={exception.version}" in constraints


def test_license_audit_requires_uv_instead_of_silently_dropping_extras() -> None:
    with (
        patch.object(POLICY["shutil"], "which", return_value=None),
        pytest.raises(
            SystemExit,
            match="complete `all` extra",
        ),
    ):
        POLICY["_require_uv"]()


def test_uv_is_a_pinned_test_dependency() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    constraints = (PROJECT_ROOT / "requirements" / "ci-constraints.txt").read_text(encoding="utf-8")

    assert any(requirement.startswith("uv>=") for requirement in project["optional-dependencies"]["test"])
    assert "uv==" in constraints


def test_every_ranged_direct_ci_dependency_has_a_constraint() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    constraint_names = {
        Requirement(line).name
        for line in (PROJECT_ROOT / "requirements" / "ci-constraints.txt").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }
    requirements = [
        *project["dependencies"],
        *project["optional-dependencies"]["all"],
        *project["optional-dependencies"]["test"],
        *project["optional-dependencies"]["docs"],
        *project["optional-dependencies"]["dev"],
    ]
    missing = []
    for raw_requirement in requirements:
        requirement = Requirement(raw_requirement)
        if requirement.name == "ray-klein" or any(spec.operator in {"==", "==="} for spec in requirement.specifier):
            continue
        if requirement.name not in constraint_names:
            missing.append(str(requirement))

    assert missing == []
