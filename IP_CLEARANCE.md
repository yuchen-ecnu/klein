<!-- SPDX-License-Identifier: Apache-2.0 -->

# Intellectual-property clearance

**Status: PENDING — this document is not a grant or a clearance decision.**

Klein's initial public snapshot was extracted from the Apache-2.0-licensed
Red-Ray fork described in [PROVENANCE.md](PROVENANCE.md). An Apache-2.0 header on
that fork is necessary but is not, by itself, evidence that every extracted
contribution and bundled asset was authorized for independent publication.

## Release-blocking evidence

| Evidence | Status | Required record |
| --- | --- | --- |
| Exact initial public tree | Recorded | Root commit and extraction scope in `PROVENANCE.md` |
| Post-extraction contributions | Enforced | Public Git history plus DCO sign-offs checked in CI |
| Extracted-code contributor inventory | Pending | Path/commit ownership report for every extracted file |
| Employer or copyright-holder authorization where applicable | Pending | Verifiable grant covering the extracted code and assets |
| Third-party dependency and frontend asset notices | Automated | `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES`, SPDX/REUSE, SBOM, and license checks |
| Private endpoint and credential removal from current artifacts | Automated | Public-source, secret-scan, and distribution-policy CI checks |
| Historical internal metadata disposition | In progress | Branch rewrite completed; provider purge of 46 read-only pull-request refs pending |
| Independent clearance review | Pending | Public maintainer vote recording reviewed evidence and exceptions |

Before changing this status to `CLEARED`, maintainers must attach the evidence
to a public issue (redacting only legally necessary material), identify every
exception, and pass the governance vote. If authorization cannot be established
for a file, it must be removed or independently reimplemented without relying on
the uncleared material.

No official release may be approved while this status remains `PENDING`.
