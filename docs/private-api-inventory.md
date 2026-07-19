---
myst:
  html_meta:
    description: "Inventory Klein for Ray dependencies on Ray private APIs and Developer APIs."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Ray private-API inventory

The shipped `ray.klein` package isolates every private Ray dependency in
`ray.klein._compat`. Normal API, runtime, state, connector, and SQL modules may
not import private Ray symbols. CI scans the real package tree and compares the
imports against this exact inventory, so a missing source directory cannot
silently turn the guard green.

Ray Data 2.56 does not expose public equivalents for evaluating one expression
against an externally supplied Arrow block or for applying its datasource
filesystem retry wrapper. `ray.klein._compat.ray_data_expression` therefore
adapts these symbols:

- `ray.data._internal.execution.interfaces.task_context.TaskContext`;
- `ray.data._internal.planner.plan_expression.expression_evaluator.eval_expr`;
- `ray.data._internal.planner.plan_expression.expression_visitors._CallableClassUDFCollector`;
- `ray.data._internal.util.RetryingPyFileSystem`;
- `ray.data.datasource.path_util._resolve_paths_and_filesystem`;
- `ray.data.datasource.path_util._validate_and_wrap_filesystem`.

The adapter owns task-context construction, class-UDF initialization,
single-block evaluation, URI resolution, and retry setup. Callers use only the
stable Klein adapter surface.

The project still uses several Ray Data extension classes whose stability is
DeveloperAPI rather than PublicAPI, including datasource and datasink base
classes. That is why the initial release intentionally supports only the Ray
2.56 minor line. Widening the range requires unit, integration, and clean-wheel
tests against the proposed Ray version.

CI rejects unlisted private imports and private imports outside `_compat`.
Widening the Ray version range requires verifying every symbol above and adding
minimum/maximum-version clean-wheel tests. The preferred migration is always to
delete an inventory item when Ray exposes a public replacement.
