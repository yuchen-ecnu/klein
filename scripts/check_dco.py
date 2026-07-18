# SPDX-License-Identifier: Apache-2.0
"""Require a Developer Certificate of Origin sign-off on every PR commit."""

from __future__ import annotations

import re
import subprocess
import sys

_SIGN_OFF = re.compile(r"^Signed-off-by:\s+.+\s+<[^<>\s]+@[^<>\s]+>$", re.MULTILINE)


def _git(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit_ids(base_ref: str) -> list[str]:
    merge_base = _git("merge-base", base_ref, "HEAD")
    return _git("rev-list", "--no-merges", f"{merge_base}..HEAD").splitlines()


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_dco.py <base-ref>", file=sys.stderr)
        return 2

    commit_ids = _commit_ids(sys.argv[1])
    missing = [
        commit_id for commit_id in commit_ids if not _SIGN_OFF.search(_git("show", "-s", "--format=%B", commit_id))
    ]
    if not missing:
        print(f"DCO sign-off present on {len(commit_ids)} commit(s).")
        return 0

    print("Missing Signed-off-by trailer:", file=sys.stderr)
    for commit_id in missing:
        print(f"  {commit_id} {_git('show', '-s', '--format=%s', commit_id)}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
