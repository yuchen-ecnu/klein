# SPDX-License-Identifier: Apache-2.0
"""Validate release-vote evidence and required checks for an exact commit."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_GITHUB_ISSUE_PATH = re.compile(r"^/([^/]+)/([^/]+)/issues/([1-9][0-9]*)/?$")
_MAINTAINER = re.compile(r"@([A-Za-z0-9](?:[A-Za-z0-9-]{0,37}))")
_VOTE = re.compile(r"^[ \t]*(\+1|0|-1)(?:[ \t]|\r?\n|$)")
_SHA = re.compile(r"^[0-9a-f]{40}$")
_SHA512 = re.compile(r"^[0-9a-fA-F]{128}$")
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp is missing a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _contains_exact_identifier(
    body: str,
    value: str,
    *,
    alphabet: str,
    ignore_case: bool = False,
) -> bool:
    """Match one release identifier without accepting a longer lookalike."""

    return (
        re.search(
            rf"(?<![{alphabet}]){re.escape(value)}(?![{alphabet}])",
            body,
            re.IGNORECASE if ignore_case else 0,
        )
        is not None
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


def _issue_number(vote_url: str, repository: str) -> int:
    parsed = urlsplit(vote_url)
    match = _GITHUB_ISSUE_PATH.fullmatch(parsed.path)
    if parsed.scheme != "https" or parsed.hostname != "github.com" or match is None:
        raise ValueError("vote URL must be an HTTPS GitHub issue URL")
    owner, name, raw_number = match.groups()
    if f"{owner}/{name}".lower() != repository.lower():
        raise ValueError("vote issue must belong to the release repository")
    if parsed.query or parsed.fragment:
        raise ValueError("vote URL must not contain a query string or fragment")
    return int(raw_number)


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

    def issue(self, number: int) -> dict[str, Any]:
        payload = self.get(f"/repos/{self._repository}/issues/{number}")
        if not isinstance(payload, dict):
            raise ValueError("GitHub issue response is invalid")
        return payload

    def issue_comments(self, number: int) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self.get(f"/repos/{self._repository}/issues/{number}/comments?per_page=100&page={page}")
            page_comments = payload
            if not isinstance(page_comments, list):
                raise ValueError("GitHub issue comments response is invalid")
            comments.extend(comment for comment in page_comments if isinstance(comment, dict))
            if len(page_comments) < 100:
                return comments
            page += 1

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


def _validate_vote_issue(
    issue: dict[str, Any], *, tag: str, commit: str, source_sha512: str
) -> tuple[datetime, datetime]:
    if issue.get("pull_request") is not None:
        raise ValueError("vote URL must identify an issue, not a pull request")
    if issue.get("state") != "closed" or issue.get("locked") is not True:
        raise ValueError("release vote issue must be closed and locked")
    opened_at = _timestamp(str(issue.get("created_at", "")))
    closed_at = _timestamp(str(issue.get("closed_at", "")))
    if closed_at - opened_at < timedelta(hours=72):
        raise ValueError("release vote must remain open for at least 72 hours")
    body = issue.get("body")
    if not isinstance(body, str) or not (
        _contains_exact_identifier(body, tag, alphabet=r"A-Za-z0-9._\-")
        and _contains_exact_identifier(body, commit, alphabet="0-9A-Fa-f", ignore_case=True)
        and _contains_exact_identifier(body, source_sha512, alphabet="0-9A-Fa-f", ignore_case=True)
    ):
        raise ValueError("vote issue must identify the exact tag, commit, and source SHA-512")
    return opened_at, closed_at


def _validate_vote(
    issue: dict[str, Any],
    comments: list[dict[str, Any]],
    *,
    maintainers: set[str],
    tag: str,
    commit: str,
    source_sha512: str,
) -> dict[str, Any]:
    opened_at, closed_at = _validate_vote_issue(issue, tag=tag, commit=commit, source_sha512=source_sha512)

    latest_votes: dict[str, dict[str, Any]] = {}
    latest_vote_order: dict[str, tuple[datetime, datetime, int]] = {}
    for comment in comments:
        user = comment.get("user")
        login = user.get("login", "").lower() if isinstance(user, dict) else ""
        if login not in maintainers:
            continue
        match = _VOTE.match(str(comment.get("body", "")))
        if match is None:
            continue
        created_at = _timestamp(str(comment.get("created_at", "")))
        updated_at = _timestamp(str(comment.get("updated_at", "")))
        if created_at < opened_at or updated_at < created_at:
            raise ValueError(f"maintainer vote from @{login} has an invalid timestamp")
        if created_at > closed_at or updated_at > closed_at:
            raise ValueError(f"maintainer vote from @{login} was created or edited after the vote closed")
        candidate = {
            "maintainer": login,
            "vote": match.group(1),
            "comment_id": comment.get("id"),
            "url": comment.get("html_url"),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        }
        identifier = comment.get("id")
        order = (updated_at, created_at, identifier if isinstance(identifier, int) else -1)
        if login not in latest_votes or order > latest_vote_order[login]:
            latest_votes[login] = candidate
            latest_vote_order[login] = order

    blocking = sorted(login for login, vote in latest_votes.items() if vote["vote"] == "-1")
    approvals = sorted(login for login, vote in latest_votes.items() if vote["vote"] == "+1")
    if blocking:
        raise ValueError(f"release vote has unresolved binding -1 votes: {', '.join(blocking)}")
    if len(approvals) < 3:
        raise ValueError(f"release vote has {len(approvals)} binding +1 vote(s); at least 3 are required")
    return {
        "issue_id": issue.get("id"),
        "issue_url": issue.get("html_url"),
        "opened_at": opened_at.isoformat(),
        "closed_at": closed_at.isoformat(),
        "locked": True,
        "votes": sorted(latest_votes.values(), key=lambda value: value["maintainer"]),
    }


def _validate_checks(runs: list[dict[str, Any]], required: list[str]) -> list[dict[str, Any]]:
    verified = []
    for name in required:
        matches = [run for run in runs if run.get("name") == name]
        if not matches:
            raise ValueError(f"required check is missing on the release commit: {name}")
        latest = max(matches, key=lambda run: (str(run.get("completed_at") or ""), int(run.get("id") or 0)))
        if latest.get("status") != "completed" or latest.get("conclusion") != "success":
            raise ValueError(
                f"required check did not succeed on the release commit: {name} "
                f"({latest.get('status')}/{latest.get('conclusion')})"
            )
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
    parser.add_argument("--vote-url", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--source-sha512", required=True)
    parser.add_argument("--maintainers-file", type=Path, default=Path("COMMUNITY.md"))
    parser.add_argument("--required-check", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    if _SHA.fullmatch(arguments.commit) is None:
        raise SystemExit("release commit must be a full 40-character lowercase Git SHA")
    if _SHA512.fullmatch(arguments.source_sha512) is None:
        raise SystemExit("source digest must be a 128-character SHA-512")
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required to validate release evidence")

    try:
        issue_number = _issue_number(arguments.vote_url, arguments.repository)
        client = GitHubClient(arguments.repository, token)
        vote = _validate_vote(
            client.issue(issue_number),
            client.issue_comments(issue_number),
            maintainers=_maintainers(arguments.maintainers_file),
            tag=arguments.tag,
            commit=arguments.commit,
            source_sha512=arguments.source_sha512,
        )
        checks = _validate_checks(client.check_runs(arguments.commit), arguments.required_check)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    evidence = {
        "schema_version": 1,
        "repository": arguments.repository,
        "tag": arguments.tag,
        "commit": arguments.commit,
        "source_sha512": arguments.source_sha512.lower(),
        "vote_url": arguments.vote_url,
        "vote": vote,
        "required_checks": checks,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Validated release evidence: {arguments.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
