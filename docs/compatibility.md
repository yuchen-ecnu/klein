---
myst:
  html_meta:
    description: "Klein for Ray compatibility policy for Python, Ray, Ray Data extension APIs, and package namespaces."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatibility policy

Klein for Ray 0.1 targets Python 3.10–3.12 and `ray>=2.50.1,<2.52`. This is the
published Ray range exercised by the initial release. The upper bound is
intentional: several Ray Data extension points used by the current code are
DeveloperAPI and can change between Ray minors.

Every supported Python version is tested against the latest compatible Ray
release, and integration CI separately exercises Ray 2.50.1 and 2.51.2. Before
widening the range, CI must run the full unit and integration suite against the
new Ray minor and the API inventory must be reviewed.

The distributed package contains no direct Ray private imports. New
private imports are compatibility debt and require a documented compatibility
shim, fallback behavior, and tests against the minimum and maximum supported Ray
versions. See [the inventory](private-api-inventory.md).

The Ray Data bridge discovers Dataset factories and methods at runtime rather
than maintaining a version-specific list. It preserves the installed method's
signature and documentation and validates Dataset-producing operations at the
execution boundary. Compatibility tests enumerate every public API exported by
the pinned Ray version so newly added methods cannot be silently hidden.

Supported Ray Serve releases still read the protobuf `FieldDescriptor.label`
attribute, which protobuf 7 removed. Klein for Ray therefore constrains
`protobuf<7` until that incompatibility is fixed upstream.

The supported public Ray wheels currently have unresolved upstream security
advisories. They are explicitly tracked in the
[security policy](https://github.com/yuchen-ecnu/klein/blob/main/SECURITY.md),
not silently hidden by an unavailable dependency floor.
