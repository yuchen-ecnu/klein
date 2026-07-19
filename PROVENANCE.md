<!-- SPDX-License-Identifier: Apache-2.0 -->

# Provenance

Klein for Ray was extracted from the Apache-2.0-licensed Red-Ray fork of Ray.
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

## Third-party license metadata overrides

`rocketmq-client-python==2.0.0` is published without a machine-readable license
field or a license file in its source distribution. Its package description
identifies the Apache License 2.0, and the Apache RocketMQ upstream repository
contains the corresponding
[Apache-2.0 license](https://github.com/apache/rocketmq-client-python/blob/master/LICENSE).
Klein therefore applies a version-exact licensecheck override. Any dependency
version change must remove or re-review that override before CI can pass.
