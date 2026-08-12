# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
POLICY = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_release_evidence.py"))


def _issue(*, locked: bool = True, hours: int = 73) -> dict:
    return {
        "id": 7,
        "html_url": "https://github.com/example/project/issues/17",
        "state": "closed",
        "locked": locked,
        "created_at": "2026-07-01T00:00:00Z",
        "closed_at": f"2026-07-{1 + hours // 24:02d}T{hours % 24:02d}:00:00Z",
        "body": f"v1.2.3\n{'a' * 40}\n{'b' * 128}",
    }


def _comment(login: str, vote: str, identifier: int) -> dict:
    return {
        "id": identifier,
        "html_url": f"https://github.com/example/project/issues/17#issuecomment-{identifier}",
        "user": {"login": login},
        "body": f"{vote}\nI verified the candidate.",
        "created_at": "2026-07-02T00:00:00Z",
        "updated_at": "2026-07-02T00:00:00Z",
    }


def _validate_vote(issue: dict, comments: list[dict]) -> dict:
    return POLICY["_validate_vote"](
        issue,
        comments,
        maintainers={"alice", "bob", "carol"},
        tag="v1.2.3",
        commit="a" * 40,
        source_sha512="b" * 128,
    )


def test_vote_evidence_requires_three_current_maintainers() -> None:
    with pytest.raises(ValueError, match=r"2 binding \+1"):
        _validate_vote(_issue(), [_comment("alice", "+1", 1), _comment("bob", "+1", 2)])


def test_vote_evidence_rejects_an_unresolved_binding_veto() -> None:
    comments = [_comment("alice", "+1", 1), _comment("bob", "+1", 2), _comment("carol", "-1", 3)]

    with pytest.raises(ValueError, match="unresolved binding -1"):
        _validate_vote(_issue(), comments)


def test_vote_evidence_accepts_a_locked_72_hour_approved_vote() -> None:
    comments = [_comment("alice", "+1", 1), _comment("bob", "+1", 2), _comment("carol", "+1", 3)]

    evidence = _validate_vote(_issue(), comments)

    assert [vote["maintainer"] for vote in evidence["votes"]] == ["alice", "bob", "carol"]


def test_vote_evidence_uses_the_most_recent_edit_to_resolve_each_vote() -> None:
    older_veto = _comment("alice", "-1", 1)
    older_veto["updated_at"] = "2026-07-04T00:00:00Z"
    newer_approval = _comment("alice", "+1", 2)
    newer_approval["created_at"] = newer_approval["updated_at"] = "2026-07-03T00:00:00Z"
    comments = [
        older_veto,
        newer_approval,
        _comment("bob", "+1", 3),
        _comment("carol", "+1", 4),
    ]

    with pytest.raises(ValueError, match="unresolved binding -1"):
        _validate_vote(_issue(), comments)


@pytest.mark.parametrize(
    "body",
    [
        f"v1.2.30\n{'a' * 40}\n{'b' * 128}",
        f"V1.2.3\n{'a' * 40}\n{'b' * 128}",
        f"v1.2.3\n{'a' * 41}\n{'b' * 128}",
        f"v1.2.3\n{'a' * 40}\n{'b' * 129}",
    ],
)
def test_vote_evidence_rejects_identifiers_embedded_in_longer_lookalikes(body: str) -> None:
    issue = _issue()
    issue["body"] = body

    with pytest.raises(ValueError, match="exact tag, commit, and source SHA-512"):
        _validate_vote(
            issue,
            [_comment("alice", "+1", 1), _comment("bob", "+1", 2), _comment("carol", "+1", 3)],
        )


def test_vote_marker_must_start_the_first_line() -> None:
    comments = [_comment("alice", "+1", 1), _comment("bob", "+1", 2), _comment("carol", "+1", 3)]
    comments[0]["body"] = "context first\n+1"

    with pytest.raises(ValueError, match=r"2 binding \+1"):
        _validate_vote(_issue(), comments)


def test_vote_url_must_be_a_same_repository_github_issue() -> None:
    assert POLICY["_issue_number"]("https://github.com/example/project/issues/17", "example/project") == 17
    with pytest.raises(ValueError, match="release repository"):
        POLICY["_issue_number"]("https://github.com/other/project/issues/17", "example/project")


def test_required_checks_are_bound_to_successful_runs() -> None:
    runs = [
        {"id": 1, "name": "CI / required gate", "status": "completed", "conclusion": "success"},
        {"id": 2, "name": "gitleaks", "status": "completed", "conclusion": "failure"},
    ]

    verified = POLICY["_validate_checks"](runs, ["CI / required gate"])
    assert verified[0]["id"] == 1
    with pytest.raises(ValueError, match="did not succeed"):
        POLICY["_validate_checks"](runs, ["gitleaks"])
