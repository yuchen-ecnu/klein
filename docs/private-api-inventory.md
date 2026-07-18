---
myst:
  html_meta:
    description: "Inventory Klein for Ray dependencies on Ray private APIs and Developer APIs."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray private-API inventory

The shipped `ray.klein` package has no direct imports from `ray._private`,
`ray.data._internal`, or `ray.air`. Bounded representations, batched column
validation and ragged arrays are implemented locally.

The project still uses several Ray Data extension classes whose stability is
DeveloperAPI rather than PublicAPI, including datasource and datasink base
classes. That is why the initial release intentionally supports only the
published Ray 2.50.1–2.51.x range. Widening the range requires unit,
integration, and clean-wheel tests against the proposed Ray version.

CI rejects direct private imports. Code that requires a private Ray API is not
accepted into the package.
