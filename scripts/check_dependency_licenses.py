# SPDX-License-Identifier: Apache-2.0
"""Run dependency license checks with reviewed, version-exact overrides."""

from __future__ import annotations

import importlib.metadata
import shutil
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LicenseOverride:
    version: str
    license_expression: str
    evidence_url: str


@dataclass(frozen=True, slots=True)
class LicenseParserException:
    version: str
    metadata_license: str
    ignored_fragment: str
    evidence_url: str


# Overrides are permitted only when the published artifact omits machine-readable
# license metadata and an upstream license file has been reviewed.  Keep the
# matching dependency exact-pinned in pyproject.toml and document the evidence in
# PROVENANCE.md.
LICENSE_OVERRIDES = {
    "fsspec": LicenseOverride(
        version="2026.7.0",
        license_expression="BSD-3-Clause",
        evidence_url="https://github.com/fsspec/filesystem_spec/blob/2026.7.0/LICENSE",
    ),
    "rocketmq-client-python": LicenseOverride(
        version="2.0.0",
        license_expression="Apache-2.0",
        evidence_url="https://github.com/apache/rocketmq-client-python/blob/master/LICENSE",
    ),
}

# These artifacts do publish license metadata, but the pinned licensecheck
# parser warns on a fragment of the expression. Suppress only the reviewed
# parser spelling after verifying the installed version and metadata exactly.
LICENSE_PARSER_EXCEPTIONS = {
    "pyvips-binary": LicenseParserException(
        version="8.18.4",
        metadata_license="LGPL-3.0-or-later",
        ignored_fragment="GNU LESSER GENERAL PUBLIC LICENSE V3",
        evidence_url="https://github.com/kleisauke/pyvips-binary/blob/v8.18.4/pyproject.toml",
    ),
}


def _require_uv() -> None:
    """Prevent licensecheck from silently falling back to its incomplete resolver."""

    if shutil.which("uv") is None:
        raise SystemExit(
            "uv is required for dependency-license auditing so the complete `all` "
            "extra and its transitive dependencies are resolved"
        )


def _verify_overrides() -> None:
    for package, override in LICENSE_OVERRIDES.items():
        try:
            installed_version = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError as error:
            raise SystemExit(
                f"license override package {package} is not installed; install the `all` extra before auditing"
            ) from error
        if installed_version != override.version:
            raise SystemExit(
                f"license override for {package} covers {override.version}, "
                f"but {installed_version} is installed; review {override.evidence_url}"
            )
    for package, exception in LICENSE_PARSER_EXCEPTIONS.items():
        try:
            installed = importlib.metadata.distribution(package)
        except importlib.metadata.PackageNotFoundError as error:
            raise SystemExit(
                f"license parser exception package {package} is not installed; install the `all` extra before auditing"
            ) from error
        actual_license = installed.metadata.get("License")
        if installed.version != exception.version or actual_license != exception.metadata_license:
            raise SystemExit(
                f"license parser exception for {package} covers {exception.version} "
                f"with {exception.metadata_license}, but found {installed.version} with {actual_license!r}; "
                f"review {exception.evidence_url}"
            )


def main() -> None:
    _require_uv()
    _verify_overrides()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "licensecheck",
            "--requirements-paths",
            "pyproject.toml",
            "--license",
            "Apache-2.0",
            "--extras",
            "all",
            "--zero",
            "--ignore-packages",
            *sorted(LICENSE_OVERRIDES),
            "--ignore-licenses",
            *(exception.ignored_fragment for exception in LICENSE_PARSER_EXCEPTIONS.values()),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
