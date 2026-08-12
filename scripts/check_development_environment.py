# SPDX-License-Identifier: Apache-2.0
"""Validate the local toolchain before running Klein development targets."""

import re
import shutil
import subprocess
import sys
from typing import Callable, List, Optional, Sequence, Tuple

MINIMUM_PYTHON = (3, 10)
MAXIMUM_PYTHON_EXCLUSIVE = (3, 13)
MINIMUM_NODE = (22, 22, 0)
REQUIRED_COMMANDS = (
    "mypy",
    "pip-audit",
    "pytest",
    "reuse",
    "ruff",
    "sphinx-build",
    "twine",
    "node",
    "npm",
)


def parse_version(value: str) -> Optional[Tuple[int, int, int]]:
    match = re.search(r"(?:^|\D)(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


def collect_problems(
    python_version: Sequence[int],
    command_finder: Callable[[str], Optional[str]],
    node_version: Optional[str],
) -> List[str]:
    problems = []
    current_python = tuple(python_version[:2])
    if current_python < MINIMUM_PYTHON or current_python >= MAXIMUM_PYTHON_EXCLUSIVE:
        problems.append("Python {}.{} is unsupported; activate Python 3.10, 3.11, or 3.12.".format(*current_python))

    missing = [command for command in REQUIRED_COMMANDS if command_finder(command) is None]
    if missing:
        problems.append("missing commands: {}".format(", ".join(missing)))

    parsed_node = parse_version(node_version or "")
    if command_finder("node") is not None and (parsed_node is None or parsed_node < MINIMUM_NODE):
        problems.append("Node.js 22.22.0 or newer is required; found {!r}.".format(node_version or "unknown"))
    return problems


def _node_version() -> Optional[str]:
    if shutil.which("node") is None:
        return None
    completed = subprocess.run(
        ["node", "--version"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def main() -> int:
    node_version = _node_version()
    problems = collect_problems(sys.version_info, shutil.which, node_version)
    if problems:
        print("Development environment is not ready:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            'Run the setup steps in CONTRIBUTING.md, including `pip install -e ".[dev]"` and `npm ci`.', file=sys.stderr
        )
        return 1

    print(
        "Development environment is ready (Python {}, Node {}).".format(
            ".".join(str(part) for part in sys.version_info[:3]),
            node_version,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
