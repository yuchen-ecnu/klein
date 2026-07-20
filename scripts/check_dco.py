# SPDX-License-Identifier: Apache-2.0
"""Require a Developer Certificate of Origin sign-off on every PR commit."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

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


def _has_sign_off(message: str) -> bool:
    return _SIGN_OFF.search(message) is not None


def _check_commit_message_file(path: str) -> int:
    try:
        message = Path(path).read_text(encoding="utf-8")
    except OSError as error:
        print(f"Unable to read commit message {path}: {error}", file=sys.stderr)
        return 2

    if _has_sign_off(message):
        print("DCO sign-off present in commit message.")
        return 0

    print("Missing Signed-off-by trailer in commit message.", file=sys.stderr)
    print("Commit with `git commit -s` or amend with `git commit --amend -s`.", file=sys.stderr)
    return 1


def _check_commit_range(base_ref: str) -> int:
    commit_ids = _commit_ids(base_ref)
    missing = [commit_id for commit_id in commit_ids if not _has_sign_off(_git("show", "-s", "--format=%B", commit_id))]
    if not missing:
        print(f"DCO sign-off present on {len(commit_ids)} commit(s).")
        return 0

    print("Missing Signed-off-by trailer:", file=sys.stderr)
    for commit_id in missing:
        print(f"  {commit_id} {_git('show', '-s', '--format=%s', commit_id)}", file=sys.stderr)
    return 1


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--commit-msg-file":
        return _check_commit_message_file(sys.argv[2])
    if len(sys.argv) == 2 and not sys.argv[1].startswith("-"):
        return _check_commit_range(sys.argv[1])

    print("usage: check_dco.py <base-ref> | --commit-msg-file <path>", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
