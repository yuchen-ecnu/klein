---
myst:
  html_meta:
    description: "Build, verify, sign, and publish a Klein for Ray release."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release process

1. Confirm CI, dependency review, CodeQL, package checks, and the full integration
   suite are green on the release commit.
1. Update `CHANGELOG.md`, package version, and `CITATION.cff`.
1. Build with `python -m build` and inspect both archives for `LICENSE`, `NOTICE`,
   expected package data, and absence of credentials or private endpoints.
1. Run `python -m twine check dist/*` and install the wheel in a clean environment.
1. Create a signed `vX.Y.Z` tag. The release workflow publishes through PyPI
   Trusted Publishing; long-lived PyPI tokens are not used.
1. Publish GitHub release notes and verify the PyPI provenance attestation.

Do not publish from an unclean worktree or reuse a version already uploaded to
PyPI. Test releases should use a PEP 440 pre-release version.
