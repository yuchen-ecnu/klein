<!-- SPDX-License-Identifier: Apache-2.0 -->

# Governance

Klein is an independent project. It does not claim acceptance, affiliation, or
endorsement by Ray or the Apache Software Foundation. The project follows open,
merit-based governance inspired by mature Apache projects.

## Principles and roles

Decisions, evidence, and votes are public unless security, privacy, or conduct
requires confidentiality. Authority is earned through sustained contribution:

- **Contributors** report issues, review, document, test, or submit changes.
- **Committers** have write access after demonstrating reliable technical and
  community judgment across multiple contributions.
- **Maintainers** carry release, security, governance, and committer-election
  responsibilities. Their votes are binding.

The contributor ladder, nomination criteria, and current roster are maintained
in [COMMUNITY.md](COMMUNITY.md).

## Decisions and votes

Routine changes use lazy consensus through public pull-request review. Public
API, compatibility, licensing, security, release, and governance changes remain
open for at least 72 hours and require at least two binding approvals. A
maintainer with a conflict of interest must disclose it and abstain.

Votes use `+1` (approve), `0` (abstain), and `-1` (reject with a concrete
reason). A technical `-1` is resolved by evidence and another vote, not simply
outnumbered. When consensus cannot be reached, a documented majority vote of
the non-conflicted maintainers decides, except for release votes.

Every release vote must:

- remain open for at least 72 hours;
- identify the signed source candidate, commit, tag, SHA-512 digest, license
  artifacts, SBOMs, test evidence, and known issues;
- receive at least three binding `+1` votes from distinct maintainers; and
- have no unresolved binding `-1` vote.

Klein currently has fewer than three maintainers, so an official release cannot
pass this policy yet. Candidate artifacts may be evaluated but must not be
presented as approved releases.

## Elections, inactivity, and removal

Any maintainer may nominate a contributor in a public issue with evidence
against the criteria in `COMMUNITY.md`. The vote remains open for seven days and
requires a majority of non-conflicted maintainers with at least two binding
`+1` votes. The result and effective role are recorded publicly.

Inactive committers or maintainers may move to emeritus status after public
notice and 30 days to respond. Immediate suspension is limited to credential,
security, legal, or conduct risk and must be reviewed and documented as soon as
confidentiality permits. Governance changes follow the same 72-hour vote rule.

## Release and intellectual-property gates

No release vote may start while [IP_CLEARANCE.md](IP_CLEARANCE.md) is pending or
required license, provenance, DCO, third-party notice, source-artifact, or
signature checks are incomplete. [docs/releasing.md](docs/releasing.md) is the
operational release procedure.
