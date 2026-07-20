---
myst:
  html_meta:
    description: "Klein for Ray API stability, compatibility, deprecation, and public-contract policy."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-api-stability)=
# API stability and compatibility

Klein for Ray is an alpha project. The word **public** identifies an intended
application or extension boundary; it does not yet mean the 1.0 compatibility
guarantee of a mature stable release. Pin the Klein and Ray versions used in
production and read [CHANGELOG.md](https://github.com/yuchen-ecnu/klein/blob/main/CHANGELOG.md)
before upgrading.

## Contract categories

| Category | How to recognize it | Current promise |
| --- | --- | --- |
| Documented application API | Listed in the [API reference](api/api.rst), connector catalog, CLI reference, or configuration reference | Supported for the documented release. Alpha changes are announced in the changelog and upgrade guide. |
| Documented extension API | Custom source/sink, table factory, partitioner, state backend, metric, and related contracts explicitly described in the reference | Intended for integrations, but more likely to evolve before 1.0. Pin the exact release and test lifecycle behavior. |
| Delegated Ray API | Dynamic `ray.klein.read_*` and `stream.data.*` methods | Signature, schema, and batch behavior belong to the pinned Ray release. Klein only promises the documented adapter boundary. |
| Operational contract | Configuration keys, CLI syntax, JSON snapshots, metric names/labels, checkpoint layout, and environment variables | Public only where explicitly documented. Consumers must tolerate additive JSON fields. Alpha releases can make announced breaking changes. |
| Internal implementation | `ray.klein._internal`, most of `ray.klein.runtime`, underscore-prefixed names, undocumented actor methods, and undocumented files | No compatibility promise. Do not import or automate against these surfaces. |

An object appearing in Python's `dir()` or a package `__all__` is not by itself
a stability declaration. The documentation is the source of truth. Some
runtime bridge names remain top-level for the bundled CLI; the
[top-level namespace reference](api/top_level.rst) identifies them as
non-application APIs.

## What compatibility means

Compatibility has several independent dimensions:

Python source compatibility
: Existing documented calls continue to import and accept the same arguments.

Behavioral compatibility
: Ordering, state, delivery, execution-mode, and failure semantics remain the
  same. A source-compatible change can still be behaviorally incompatible.

Operational compatibility
: Automation can continue to use documented configuration, CLI, snapshot, and
  metric contracts. New JSON fields and new enum values can be added; clients
  must ignore fields they do not understand.

State compatibility
: A new deployment can read checkpoint metadata, source positions, managed
  state, timers, and sink committables written by the old deployment. This is
  not guaranteed across arbitrary alpha versions.

Wire compatibility
: Running tasks, actors, and drivers can exchange messages. Klein does not
  support a rolling binary upgrade in which one live job mixes Klein versions.

Dependency compatibility
: The application, all eligible workers, connector clients, and Ray cluster
  use versions in the tested matrix. See [Compatibility](compatibility.md).

Do not infer state or wire compatibility from a successful import. Use the
procedure in [Upgrading](upgrading.md).

## Public API rules

- Application code should import ordinary graph contracts from `ray.klein`,
  specialized contracts from `ray.klein.api`, configuration descriptors from
  `ray.klein.config`, and managed-state contracts from `ray.klein.state`.
- `ray.klein.runtime.partitioning` is the documented exception for custom
  partitioners. Other runtime modules are implementation details unless a
  reference page says otherwise.
- A method, option, or connector is supported only in the execution modes shown
  in the [operator compatibility matrix](operator-compatibility.md) or connector
  catalog.
- Optional integrations may raise `ModuleNotFoundError` until their named
  package extra and native dependencies are installed on every worker.
- A callable's Python type annotation does not upgrade its runtime contract.
  Record shape, batch format, serialization, and lifecycle rules remain those
  in the programming guide.

## Dynamic Ray Data surface

Klein discovers public Ray Data readers and Dataset methods from the installed
compatible Ray version. This avoids copying a large, version-sensitive API, but
it means the names are not all present in `ray.klein.__all__` or static API
pages.

Use:

```python
"read_parquet" in dir(ray.klein)
"map_batches" in stream.data.available
stream.data.kind("map_batches")
```

Library code that supports several Ray patches should check availability rather
than assume a method exists. Native `DataStream` methods retain Klein semantics;
dynamic methods retain Ray Data semantics and are batch-only unless the
[interop guide](ray-data-interop.md) explicitly documents a streaming form.

## Deprecation policy before 1.0

Klein has no fixed multi-release deprecation window while it is alpha. The
project follows this best-effort process for a documented contract:

1. Record the old behavior, replacement, and compatibility impact in the
   changelog and upgrade guide.
2. Emit a warning when a safe runtime warning is possible and does not create
   excessive worker log volume.
3. Keep read compatibility for persisted state when practical, or state clearly
   that a migration or clean start is required.
4. Add or update contract tests so the announced boundary is reviewable.

Security fixes, correctness fixes, and dependency incompatibilities can require
an immediate change. Do not suppress deprecation or compatibility warnings
without reviewing the associated release note.

## Contracts that require explicit review

Changes to any of the following need documentation, tests, and an upgrade note:

- a top-level or documented extension API;
- operator ordering, partitioning, changelog, or delivery behavior;
- a configuration key, default, type, precedence, or environment spelling;
- a CLI command, exit behavior, or documented JSON field;
- a metric name, kind, unit, label, or histogram boundary;
- checkpoint manifests, state codecs, key hashing, key groups, source state, or
  sink committables;
- connector schema, offset ownership, transaction behavior, or option;
- supported Python, Ray, SQLGlot, PyArrow, or connector dependency ranges.

The [documentation contract tests](testing.md) mechanically cover part of this
surface. They complement, rather than replace, upgrade testing with real
production-shaped state.

## Reporting an accidental break

Open a reproducible issue with:

- old and new Klein, Ray, Python, and connector versions;
- whether the break concerns source, behavior, operations, or persisted state;
- the smallest graph and configuration that demonstrate it;
- the old checkpoint format/version and a copy of non-sensitive error output;
- whether a clean start works and whether rollback remains possible.

Use the private process in the project security policy when the break can expose
data, credentials, or code execution.
