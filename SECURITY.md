<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security Policy

## Supported versions

Until the first stable release, only the latest release is eligible for security
fixes. Supported versions will be listed here when releases begin.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/yuchen-ecnu/klein/security/advisories/new).
Do not create a public issue. If that form is unavailable, contact
[@yuchen-ecnu](https://github.com/yuchen-ecnu) and ask for a private reporting
channel without including vulnerability details in the initial message.

Include the affected version, environment, reproduction steps, impact, and any
known mitigations. Maintainers should acknowledge a report within three business
days and provide a status update within seven business days. Timelines may change
as investigation proceeds.

Never include credentials, private endpoints, production data, or personal data
in a report or reproducer.

## Trust boundary

Checkpoint metadata and user-defined source/operator state can contain Python
pickle payloads. Restore checkpoints only from storage controlled by the job
operator. Treat write access to a checkpoint prefix as equivalent to code
execution in Klein workers. Use least-privilege storage credentials, encryption
in transit, object versioning, and a dedicated prefix per trust domain.

Klein redacts common credential fields from its state API and operational logs,
but applications must not place secrets in record values, operator names,
exception messages, metric labels, or job names.

## Upstream Ray advisories

As of the 0.1 development release, the latest published Ray wheel in Klein's
supported range has unresolved advisories `PYSEC-2026-518`, `PYSEC-2026-520`,
`PYSEC-2026-2271`, `PYSEC-2026-2272`, and `PYSEC-2026-2273`. No published Ray
wheel fixes the complete set. Klein keeps these identifiers in a narrow,
reviewable audit allowlist so all other dependency findings still fail CI.

Do not expose a Ray cluster or Dashboard directly to untrusted networks. Apply
network isolation and authentication controls from Ray's security guidance,
and move to a fixed Ray release as soon as one is publicly available and passes
Klein's compatibility suite. The allowlist must be removed when the supported
Ray range advances to that release.
