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
