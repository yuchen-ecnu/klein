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
the historical private-registry coordinates only in
`frontend/package-lock.json` across 14 reachable revisions, 30 commit author
records using a corporate domain, and no matches for the repository's strong
secret patterns. CI additionally runs Gitleaks over the complete history.
Removing the metadata requires a coordinated history rewrite and force-push,
so it is tracked as an explicit pending decision rather than being silently
rewritten.

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
