#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run pip-audit with Klein's explicit, temporary upstream Ray allowlist."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence

UPSTREAM_RAY_ADVISORIES = (
    "PYSEC-2026-518",
    "PYSEC-2026-520",
    "PYSEC-2026-2271",
    "PYSEC-2026-2272",
    "PYSEC-2026-2273",
)


def build_command(arguments: Sequence[str]) -> list[str]:
    command = [sys.executable, "-m", "pip_audit", *arguments]
    for advisory in UPSTREAM_RAY_ADVISORIES:
        command.extend(("--ignore-vuln", advisory))
    return command


def main() -> int:
    return subprocess.run(build_command(sys.argv[1:]), check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
