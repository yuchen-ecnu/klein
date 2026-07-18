# SPDX-License-Identifier: Apache-2.0
"""Lowering a Klein operator to a ray.data call.

Why this is split in two
------------------------
Klein has two execution backends: its own streaming operators, and lowering to
``ray.data`` for batch jobs. The lowering is split by what each family actually
is:

* **Transforms** (map / map_batches / flat_map / filter / union) are *behavior*.
  Each is a small named ``lower_*`` function that just writes its real
  ``Dataset.xxx(...)`` call. ``filter``'s quirks read as code, not decoded flags.
  They take everything from a :class:`LoweringContext`, capture no locals, and
  so are module-level and reusable (map_reduce reuses ``lower_flat_map`` etc.).

* Public Ray Data factories and Dataset methods use the version-adaptive
  :class:`~ray.klein.api.ray_data.RayDataCall`. It resolves names against
  the installed Ray version and forwards arguments without a Klein API table.

* Klein-native bridges use :class:`~ray.klein.api.ray_data.RayDataCall`, the
  same explicit call model as the public Ray Data adapter.

Both forms are lowerings over :class:`LoweringContext`; Dataset-producing
operations return a Dataset while terminal operations can return any public
Ray Data result. ``LogicalFunction.to_batch`` builds the context and calls
whichever lowering it holds.
"""

from ray.data import Dataset

from ray.klein._internal.collections import filter_none_items
from ray.klein.api.functions.lowering_context import LoweringContext

# --- transform lowerings: behavior, one named fn each -----------------------


def lower_map(ctx: LoweringContext) -> Dataset:
    return Dataset.map(
        ctx.upstream_ds[0],
        ctx.user_fn,
        num_cpus=ctx.resources.num_cpus,
        num_gpus=ctx.resources.num_gpus,
        concurrency=ctx.resources.concurrency,
        **ctx.user_fn_ctor_kwargs_for_ray_data,
    )


def lower_flat_map(ctx: LoweringContext) -> Dataset:
    return Dataset.flat_map(
        ctx.upstream_ds[0],
        ctx.user_fn,
        num_cpus=ctx.resources.num_cpus,
        num_gpus=ctx.resources.num_gpus,
        concurrency=ctx.resources.concurrency,
        **ctx.user_fn_ctor_kwargs_for_ray_data,
    )


def lower_map_batches(ctx: LoweringContext) -> Dataset:
    return Dataset.map_batches(
        ctx.upstream_ds[0],
        ctx.user_fn,
        num_cpus=ctx.resources.num_cpus,
        num_gpus=ctx.resources.num_gpus,
        concurrency=ctx.resources.concurrency,
        batch_size=ctx.runtime_info.batch_size,
        batch_format=ctx.runtime_info.batch_format,
        **ctx.user_fn_ctor_kwargs_for_ray_data,
    )


def lower_filter(ctx: LoweringContext) -> Dataset:
    # filter is the odd one: it filters None cpus/gpus and does NOT forward the
    # user fn's constructor args to ray.data.
    return Dataset.filter(
        ctx.upstream_ds[0],
        ctx.user_fn,
        concurrency=ctx.resources.concurrency,
        **filter_none_items({"num_cpus": ctx.resources.num_cpus, "num_gpus": ctx.resources.num_gpus}),
    )


def lower_union(ctx: LoweringContext) -> Dataset:
    ds = ctx.upstream_ds
    return Dataset.union(ds[0], *ds[1:])
