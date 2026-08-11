---
myst:
  html_meta:
    description: "Build, vote on, verify, sign, and publish a Klein source release."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release process

Klein treats the signed source archive as the release. Wheels are convenience
artifacts built from the same tag. An official release is blocked until
`IP_CLEARANCE.md` is `CLEARED` and the maintainer quorum in `GOVERNANCE.md` can
be met.

## One-time repository setup

Configure the release workflow with:

- `RELEASE_GPG_PUBLIC_KEY`, an armored public key repository variable;
- `RELEASE_GPG_PRIVATE_KEY` and `RELEASE_GPG_PASSPHRASE`, environment secrets
  limited to the `release-candidate` environment;
- required human reviewers on `release-candidate` and `pypi`; and
- PyPI Trusted Publishing for the `pypi` environment.

Keep signing keys outside the repository. Publish key transitions before using
them, and retain revoked keys in the public `KEYS` history.

## Prepare and publish a candidate

1. Resolve every release-blocking issue; update the version, `CHANGELOG.md`, and
   compatibility documentation. Do not put a release date in `CITATION.cff`
   before approval.
2. Confirm CI, CodeQL, dependency review, secret scanning, Scorecard, full
   integration tests, frontend tests, coverage, REUSE, and documentation are
   green on the exact commit.
3. Create and verify a signed annotated tag. From a clean checkout of that tag,
   run `make source-release`, `python -m build`, `python -m twine check dist/*`,
   and `python scripts/check_distribution.py` over the source archive, wheel,
   and Python sdist.
4. Generate CycloneDX SBOMs, `THIRD_PARTY_NOTICES`, and a SHA-512 digest. Sign
   the source candidate with an ASCII-armored detached OpenPGP signature.
5. Publish the source archive, signature, digest, tag, commit, SBOMs, test
   evidence, and known issues at a stable public candidate URL.
6. Open a public vote lasting at least 72 hours. The vote is on the identified
   source bytes, not on a branch name. Follow `GOVERNANCE.md`; record every
   binding vote and the final result at a stable URL.

If the candidate changes by one byte, cancel the vote, issue a new candidate,
and restart the 72-hour window.

## Promote an approved candidate

Run the `Release` workflow manually with:

- the signed final tag;
- the public vote-result URL; and
- the voted source archive's SHA-512 digest.

The workflow verifies the tag, main-branch ancestry, exact package version,
HTTPS vote evidence, voted source digest, tests, source/distribution policy,
SBOM generation, signatures, and checksums. It then publishes the wheel and
sdist through PyPI Trusted Publishing and creates the immutable GitHub release.
Every source, Python, SBOM, checksum, and key artifact receives a detached
signature; `SHA512SUMS` covers the release payload.

After promotion, independently install the wheel, verify all signatures and
digests from a clean machine, confirm PyPI/GitHub provenance, then add the
approved version and date to `CITATION.cff` and the changelog in the next
development commit.

Never reuse an uploaded version. If promotion is partial, stop, preserve the
evidence, publish an incident note, and either complete the exact approved
candidate or issue a new version; do not replace files under the same version.
