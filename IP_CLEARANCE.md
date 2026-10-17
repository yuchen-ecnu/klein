<!-- SPDX-License-Identifier: Apache-2.0 -->

# Intellectual-property clearance

**Status: CLEARED**

**Authorized by:** @yuchen-ecnu

**Confirmed on:** 2026-10-17

## Maintainer authorization

On 2026-10-17, project owner and maintainer Yu Chen (`@yuchen-ecnu`) explicitly
confirmed that they own the extracted Klein code or have obtained permission
from the appropriate rights holder to independently open-source it and publish
it to PyPI. The maintainer authorized the first public `ray-klein` release.
This record documents that maintainer confirmation; it does not claim an
independent legal review or a separately published employer authorization.

The confirmation covers the Klein code extracted from the Red-Ray fork as
identified in [PROVENANCE.md](PROVENANCE.md), together with the subsequent
public project changes. Upstream and third-party license terms, attribution,
and notices remain applicable.

## Evidence and release scope

| Record | Basis |
| --- | --- |
| Initial source and extraction scope | Public root commit and scope in `PROVENANCE.md` |
| Authorization for independent publication | Maintainer confirmation recorded above |
| Post-extraction contributions | Public Git history and DCO sign-offs checked in CI |
| Dependencies and frontend assets | `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES`, SPDX/REUSE, SBOMs, and license checks |
| Published artifact contents | Public-source, secret-scan, and distribution-policy checks |

The release contains the current reviewed source tree and its built artifacts.
The historical provider PR-ref and external-fork cleanup described in
`PROVENANCE.md` remains a separate ongoing task. This clearance does not assert
that those external historical copies have been removed.

Revisit this record when new provenance or authorization concerns are reported.
Unresolved authorization concerns block the affected release until the
maintainer records a resolution or removes the affected material.
