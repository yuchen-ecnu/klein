<!-- SPDX-License-Identifier: Apache-2.0 -->

# Provenance

Klein was extracted from the Apache-2.0-licensed Red-Ray fork of Ray.
Development of the extracted code occurred from 2024 through 2026 before the
standalone repository was created.

The first public commit is a sanitized source snapshot. The internal commit
graph was not copied because it also contains unrelated organization-only
connectors, endpoints, tickets, and operational metadata. This avoids
publishing those artifacts while preserving code attribution in
[AUTHORS.md](AUTHORS.md), [NOTICE](NOTICE), SPDX annotations, and this record.

All changes after extraction are recorded normally in the public Git history.
Project maintainers can perform a private provenance audit of the pre-extraction
history when required for licensing or security review.

The public history root is commit
`bcef4bf838ebb8ca9361d9f4252976891deb58ac` (2026-07-19). The current tree has
automated checks for credentials, organization-only markers, private package
resolutions, license metadata, and release contents. A 2026-08-11 audit found
historical private-registry coordinates in old frontend lock files and
corporate-domain identities in commit metadata, with no matches for the
repository's strong secret patterns. A coordinated rewrite then replaced the
registry coordinates with canonical public npm URLs and the affected identities
with the contributor's public noreply identity across every
maintainer-controlled branch. The rewrite preserved the current source tree;
an independent mirror audit confirmed that all branch content, commit metadata,
and messages contain none of the identified organization-only markers or strong
secret patterns. GitHub's read-only pull-request refs for 46 pre-rewrite pull
requests still retain old metadata and, in some cases, old lock-file blobs.
Provider-side dereferencing, garbage collection, and cached-view removal are
therefore pending through GitHub Support. One independently controlled fork
created before the rewrite also retains old history and requires coordination
with its owner. Neither copy is part of a maintained branch or release artifact,
and strong secret patterns were absent from both. Affected pre-rewrite commit
IDs are intentionally no longer part of branch history, so clones made before
2026-08-11 must fetch and rebase or re-clone. CI additionally runs Gitleaks over
branch history.

The remaining authorization evidence and release gate are tracked in
[IP_CLEARANCE.md](IP_CLEARANCE.md). Nothing in this provenance record should be
read as a copyright-holder grant or completed IP clearance.

## Third-party license metadata overrides

`fsspec==2026.7.0` is published without machine-readable license metadata. The
matching upstream tag contains the reviewed
[BSD-3-Clause license](https://github.com/fsspec/filesystem_spec/blob/2026.7.0/LICENSE),
so Klein applies a version-exact licensecheck override.

`rocketmq-client-python==2.0.0` is published without a machine-readable license
field or a license file in its source distribution. Its package description
identifies the Apache License 2.0, and the Apache RocketMQ upstream repository
contains the corresponding
[Apache-2.0 license](https://github.com/apache/rocketmq-client-python/blob/master/LICENSE).
Klein therefore applies a version-exact licensecheck override. Any dependency
version change must remove or re-review that override before CI can pass.

The tagged `pyvips-binary==8.18.4`
[build metadata](https://github.com/kleisauke/pyvips-binary/blob/v8.18.4/pyproject.toml)
declares `LGPL-3.0-or-later`. The pinned licensecheck version misparses part of
that expression as `UNKNOWN` before accepting it, so the audit suppresses that
single parser warning only after verifying the exact installed version and
metadata. The optional native binary retains its upstream license terms.
