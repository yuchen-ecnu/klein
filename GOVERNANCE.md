<!-- SPDX-License-Identifier: Apache-2.0 -->

# Governance

Klein is an independent project. It does not claim acceptance, affiliation, or
endorsement by Ray or the Apache Software Foundation.

## Roles and decisions

Contributors propose changes through public issues and pull requests.
Committers review and integrate changes. Maintainers are responsible for
releases, project direction, security, and the project's authorization records.
The current roster is listed in [COMMUNITY.md](COMMUNITY.md).

Klein currently has one maintainer, Yu Chen (`@yuchen-ecnu`). The maintainer may
approve releases and governance changes without a multi-person quorum or a
fixed waiting period. The project owner adopted this operating model on
2026-10-17 while preparing the first public release. Material decisions and
changes to the maintainer roster are recorded
in public pull requests or issues.

Routine changes use reviewed pull requests and the repository's required CI
checks. Contributors can raise objections and provide evidence; maintainers
record the resolution of substantive compatibility, security, or authorization
concerns before proceeding with the affected change.

## Releases

A current maintainer manually starts the `Release` workflow from the default
branch. The release tag must be annotated, match the package version, and point
to the exact workflow commit. Required CI, CodeQL, and secret-scanning checks
must have succeeded on that commit. The workflow also checks the maintainer's
published authorization record in [IP_CLEARANCE.md](IP_CLEARANCE.md).

The workflow builds the canonical source archive, wheel, and Python sdist;
validates their contents and installation; and publishes checksums, dependency
inventories, and CI evidence. GitHub artifact attestations and PyPI Trusted
Publishing identify the publishing workflow. A separately managed GPG key and
public release-vote issue are not required.

The release procedure is documented in [docs/releasing.md](docs/releasing.md).
As the maintainer team grows, it may adopt a broader review or voting policy
through a documented governance change.

## Membership and conduct

Maintainers evaluate new committers and maintainers using the contribution and
stewardship criteria in `COMMUNITY.md`, with the appointment recorded publicly.
Inactive members may move to emeritus status after notice and an opportunity
to respond. Security or conduct incidents may require immediate temporary
access restrictions, followed by a documented review.
