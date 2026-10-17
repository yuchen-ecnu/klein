---
myst:
  html_meta:
    description: "Publish a verified Klein release from the maintainer's GitHub workflow."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Release process

Klein uses a single-maintainer release process. A current maintainer starts the
GitHub `Release` workflow after the exact default-branch commit passes CI.
The source archive, wheel, and Python sdist share that commit and version.

## One-time setup

Create a PyPI account, verify its email, and enable two-factor authentication.
Register a GitHub Trusted Publisher with project `ray-klein`, owner
`yuchen-ecnu`, repository `klein`, workflow `release.yml`, and environment
`pypi`. For the first release, use the pending-publisher form under the PyPI
account's Publishing settings. The first upload creates the project.

Create the matching `pypi` GitHub environment. Additional environment reviewers
are optional. Publishing uses GitHub OIDC and PyPI attestations; no saved PyPI
API token or separately managed GPG key is required.

Keep the current maintainer roster in `COMMUNITY.md` and the owner's publication
authorization in `IP_CLEARANCE.md`. The workflow requires an explicit `CLEARED`
status and an authorization attributed to a current maintainer.

## Prepare the release

1. Update the package version, PyPI installation examples, changelog,
   `CITATION.cff`, and compatibility documentation through a reviewed pull
   request. Keep the release's alpha, beta, or stable status accurate and
   record the intended release date.
2. Merge the change and wait for CI, CodeQL, and secret scanning to pass on the
   exact `main` commit. An older successful check does not replace a pending
   or failed rerun.
3. Create an annotated version tag on that commit. For the first alpha release:

   ```bash
   git fetch origin main --tags
   git tag -a v0.1.0a1 origin/main -m "Klein 0.1.0a1"
   git push origin refs/tags/v0.1.0a1
   ```

4. From GitHub Actions, run `Release` on `main` with the tag, or use:

   ```bash
   gh workflow run release.yml --ref main -f tag=v0.1.0a1
   ```

The tag must match `pyproject.toml` and resolve to the exact workflow commit.
Only current maintainers may dispatch or rerun publication. The workflow
records the initiating identities, authorization record, and successful
check-run links in `release-evidence.json`.

## Build and publication

The workflow builds a reproducible source archive from the verified commit,
then rebuilds the Dashboard and Python distributions from that archive. It
checks package metadata, license and artifact contents, namespace compatibility,
and installation in a clean environment. Dependency inventories and SHA-512
checksums accompany the release.

The publishing job verifies those checksums, creates GitHub artifact
attestations, and uploads the wheel and sdist through PyPI Trusted Publishing.
The GitHub release then publishes the source archive, distributions, checksums,
and evidence. Alpha, beta, and release-candidate versions are marked as
prereleases on GitHub.

Documentation keeps `latest` and retained version directories in English and
Chinese. Version tags must be annotated, belong to the checked-out main
lineage, and match the package version.

## Verify the published version

Install the exact published version in a fresh environment and run the quick
start. Confirm that PyPI's project description, images, installation metadata,
and provenance links render correctly, and compare downloaded artifact hashes
with the GitHub release's `SHA512SUMS`.

After successful publication, confirm the release date in the changelog and
`CITATION.cff`. Never replace an uploaded file or reuse its version. If a
publication fails partway through, inspect the published artifacts and finish
only missing steps for the same verified bytes, or prepare a new version.
