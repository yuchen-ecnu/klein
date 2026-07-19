---
myst:
  html_meta:
    description: "Klein for Ray compatibility policy for Python, Ray, Ray Data extension APIs, and package namespaces."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatibility policy

Klein for Ray 0.1 targets Python 3.10–3.12 and `ray>=2.56.1,<2.57`. The upper
bound is intentional: several Ray Data extension points used by the current
code are DeveloperAPI and can change between Ray minors.

Every supported Python version is tested against the latest compatible Ray
2.56 patch. Before widening the range, CI must run the full unit and integration
suite against the new Ray minor and the API inventory must be reviewed.

The distributed package keeps its small set of unavoidable Ray Data private
imports inside `ray.klein._compat`. New private imports are compatibility debt
and require an exact inventory entry, an isolated adapter, and tests against
the supported Ray line. Normal Klein modules are forbidden from importing them.
See [the inventory](private-api-inventory.md).

The Ray Data bridge discovers Dataset factories and methods at runtime rather
than maintaining a version-specific list. It preserves the installed method's
signature and documentation and validates Dataset-producing operations at the
execution boundary. Compatibility tests enumerate every public API exported by
the pinned Ray version so newly added methods cannot be silently hidden.

Supported Ray Serve releases still read the protobuf `FieldDescriptor.label`
attribute, which protobuf 7 removed. Klein for Ray therefore constrains
`protobuf<7` until that incompatibility is fixed upstream.
