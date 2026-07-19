# SPDX-License-Identifier: Apache-2.0
"""Run dependency license checks with reviewed, version-exact overrides."""

from __future__ import annotations

import importlib.metadata
import subprocess
import sys
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LicenseOverride:
    version: str
    license_expression: str
    evidence_url: str


# Overrides are permitted only when the published artifact omits machine-readable
# license metadata and an upstream license file has been reviewed.  Keep the
# matching dependency exact-pinned in pyproject.toml and document the evidence in
# PROVENANCE.md.
LICENSE_OVERRIDES = {
    "rocketmq-client-python": LicenseOverride(
        version="2.0.0",
        license_expression="Apache-2.0",
        evidence_url="https://github.com/apache/rocketmq-client-python/blob/master/LICENSE",
    ),
}


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


def main() -> None:
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
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
