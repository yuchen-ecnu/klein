# SPDX-License-Identifier: Apache-2.0
"""Validate maintainer authorization and successful CI for an exact release commit."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_MAINTAINER = re.compile(r"@([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}))")
_SHA = re.compile(r"^[0-9a-f]{40}$")
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_REQUIRED_CHECKS = (
    "CI / required gate",
    "Analyze / python",
    "Analyze / javascript-typescript",
    "gitleaks",
)


def _maintainers(path: Path) -> set[str]:
    section = False
    maintainers: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() == "### Maintainers":
            section = True
            continue
        if section and line.startswith("### "):
            break
        if section and line.lstrip().startswith("-"):
            match = _MAINTAINER.search(line)
            if match is not None:
                maintainers.add(match.group(1).lower())
    if not maintainers:
        raise ValueError(f"no maintainers found in {path}")
    return maintainers


class GitHubClient:
    def __init__(self, repository: str, token: str, api_url: str = "https://api.github.com") -> None:
        self._repository = repository
        self._token = token
        self._api_url = api_url.rstrip("/")

    def get(self, path: str) -> Any:
        request = urllib.request.Request(
            f"{self._api_url}{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "ray-klein-release-evidence",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read(_MAX_RESPONSE_BYTES + 1)
        except urllib.error.URLError as error:
            raise ValueError(f"GitHub API request failed for {path}: {error}") from error
        if len(payload) > _MAX_RESPONSE_BYTES:
            raise ValueError(f"GitHub API response is too large for {path}")
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"GitHub API returned invalid JSON for {path}") from error
        return value

    def workflow_run(self, run_id: int) -> dict[str, Any]:
        payload = self.get(f"/repos/{self._repository}/actions/runs/{run_id}")
        if not isinstance(payload, dict):
            raise ValueError("GitHub workflow-run response is invalid")
        return payload

    def check_runs(self, commit: str) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self.get(
                f"/repos/{self._repository}/commits/{commit}/check-runs?filter=latest&per_page=100&page={page}"
            )
            if not isinstance(payload, dict):
                raise ValueError("GitHub check-runs response is invalid")
            page_runs = payload.get("check_runs")
            if not isinstance(page_runs, list):
                raise ValueError("GitHub check-runs response is invalid")
            runs.extend(run for run in page_runs if isinstance(run, dict))
            if len(page_runs) < 100:
                return runs
            page += 1


def _validate_clearance(path: Path, maintainers: set[str]) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    if lines.count("**Status: CLEARED**") != 1:
        raise ValueError("IP clearance must explicitly be CLEARED")
    authors = [
        line.removeprefix("**Authorized by:** @").lower() for line in lines if line.startswith("**Authorized by:** @")
    ]
    if len(authors) != 1 or authors[0] not in maintainers:
        raise ValueError("IP clearance must identify one current maintainer's authorization")
    return authors[0]


def _validate_dispatch(
    run: dict[str, Any], *, repository: str, commit: str, maintainers: set[str], default_branch: str
) -> dict[str, Any]:
    if run.get("event") != "workflow_dispatch" or run.get("path") != ".github/workflows/release.yml":
        raise ValueError("release must use the manually dispatched release workflow")
    if run.get("head_sha") != commit or run.get("head_branch") != default_branch:
        raise ValueError("release dispatch must identify the exact default-branch commit")
    if str(run.get("repository", {}).get("full_name", "")).lower() != repository.lower():
        raise ValueError("release dispatch belongs to another repository")
    actors = {field: str(run.get(field, {}).get("login", "")).lower() for field in ("actor", "triggering_actor")}
    if any(login not in maintainers for login in actors.values()):
        raise ValueError("release dispatch and reruns must be initiated by a current maintainer")
    return {"id": run["id"], "url": run.get("html_url"), **actors}


def _validate_checks(runs: list[dict[str, Any]], required: list[str], commit: str) -> list[dict[str, Any]]:
    verified = []
    for name in required:
        matches = [run for run in runs if run.get("name") == name]
        if not matches:
            raise ValueError(f"required check is missing on the release commit: {name}")
        # A new pending rerun must supersede an older successful run.
        latest = max(matches, key=lambda run: int(run.get("id") or 0))
        if latest.get("head_sha") != commit or latest.get("app", {}).get("slug") != "github-actions":
            raise ValueError(f"required check has an unexpected commit or publisher: {name}")
        if latest.get("status") != "completed" or latest.get("conclusion") != "success":
            raise ValueError(f"required check did not succeed on the release commit: {name}")
        verified.append(
            {
                "name": name,
                "id": latest.get("id"),
                "details_url": latest.get("details_url"),
                "completed_at": latest.get("completed_at"),
                "conclusion": "success",
            }
        )
    return verified


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--default-branch", default="main")
    parser.add_argument("--maintainers-file", type=Path, default=Path("COMMUNITY.md"))
    parser.add_argument("--clearance-file", type=Path, default=Path("IP_CLEARANCE.md"))
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if _SHA.fullmatch(arguments.commit) is None:
        raise SystemExit("release commit must be a full 40-character lowercase Git SHA")
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required to validate release evidence")
    try:
        maintainers = _maintainers(arguments.maintainers_file)
        authorized_by = _validate_clearance(arguments.clearance_file, maintainers)
        client = GitHubClient(arguments.repository, token)
        dispatch = _validate_dispatch(
            client.workflow_run(arguments.run_id),
            repository=arguments.repository,
            commit=arguments.commit,
            maintainers=maintainers,
            default_branch=arguments.default_branch,
        )
        checks = _validate_checks(client.check_runs(arguments.commit), list(_REQUIRED_CHECKS), arguments.commit)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    evidence = {
        "schema_version": 2,
        "repository": arguments.repository,
        "tag": arguments.tag,
        "commit": arguments.commit,
        "authorization": {"maintainer": authorized_by, "record": str(arguments.clearance_file)},
        "dispatch": dispatch,
        "required_checks": checks,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Validated release evidence: {arguments.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
