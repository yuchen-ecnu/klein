# SPDX-License-Identifier: Apache-2.0
"""Lock the named transform lowering produced by ``LogicalFunction.to_batch``."""

import unittest

from ray.data import Dataset

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.functions.lowering_context import LoweringContext
from ray.klein.api.functions.ray_data_lowering import (
    lower_filter,
    lower_map_batches,
    lower_union,
)
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.runtime.resources import Resources


def _user_fn(batch):
    return batch


class _FakeDS:
    """Stand in for an upstream Dataset; identity is all that matters."""


class TransformLoweringTest(unittest.TestCase):
    """Named transform lowerings: verify the real Dataset.xxx call kwargs."""

    def setUp(self):
        self._calls = []
        self._orig = {}
        for name in ("map", "map_batches", "flat_map", "filter", "union"):
            self._orig[name] = getattr(Dataset, name)

            def make(n):
                def fake(ds, *a, **k):
                    self._calls.append((n, ds, a, k))
                    return f"ds::{n}"

                return fake

            setattr(Dataset, name, make(name))

    def tearDown(self):
        for name, fn in self._orig.items():
            setattr(Dataset, name, fn)

    def test_map_batches_passes_user_fn_and_batch(self):
        # CallableClass fn: ctor args/kwargs must flow through to ray.data so
        # the actor pool can instantiate the class per-worker.
        class _UserCallable:
            def __init__(self, k="default"):
                self.k = k

            def __call__(self, batch):
                return batch

        lf = LogicalFunction(
            _UserCallable,
            fn_constructor_args=[1],
            fn_constructor_kwargs={"k": "v"},
            lowering=lower_map_batches,
            resources=Resources(num_cpus=2, num_gpus=1, concurrency=4),
            batch_size=8,
            batch_timeout=3,
            batch_format="numpy",
        )
        ds = _FakeDS()
        lf.to_batch([ds])
        name, got_ds, args, kw = self._calls[-1]
        self.assertEqual(name, "map_batches")
        self.assertIs(got_ds, ds)
        self.assertIs(args[0], _UserCallable)
        self.assertEqual(kw["fn_constructor_args"], (1,))
        self.assertEqual(kw["fn_constructor_kwargs"], {"k": "v"})
        self.assertEqual(kw["num_cpus"], 2)
        self.assertEqual(kw["num_gpus"], 1)
        self.assertEqual(kw["concurrency"], 4)
        self.assertEqual(kw["batch_size"], 8)
        self.assertEqual(kw["batch_format"], "numpy")

    def test_map_batches_plain_fn_omits_ctor_args(self):
        # Plain function: ray.data's get_compute_strategy rejects non-None
        # fn_constructor_args / fn_constructor_kwargs when fn is not a
        # CallableClass, so the lowering must NOT forward them.
        lf = LogicalFunction(
            _user_fn,
            lowering=lower_map_batches,
            resources=Resources(num_cpus=1, concurrency=2),
            batch_size=4,
            batch_timeout=1,
        )
        lf.to_batch([_FakeDS()])
        kw = self._calls[-1][3]
        self.assertNotIn("fn_constructor_args", kw)
        self.assertNotIn("fn_constructor_kwargs", kw)
        self.assertEqual(kw["concurrency"], 2)
        self.assertEqual(kw["batch_size"], 4)

    def test_map_batches_keeps_none_resources(self):
        lf = LogicalFunction(_user_fn, lowering=lower_map_batches, resources=Resources())
        lf.to_batch([_FakeDS()])
        kw = self._calls[-1][3]
        self.assertIsNone(kw["num_cpus"])
        self.assertIsNone(kw["concurrency"])

    def test_filter_filters_none_and_forwards_callable_class_constructor(self):
        class _Predicate:
            def __init__(self, threshold, *, inclusive=False):
                self.threshold = threshold
                self.inclusive = inclusive

            def __call__(self, row):
                return row["value"] >= self.threshold if self.inclusive else row["value"] > self.threshold

        lf = LogicalFunction(
            _Predicate,
            fn_constructor_args=[1],
            fn_constructor_kwargs={"inclusive": True},
            lowering=lower_filter,
            resources=Resources(num_cpus=None, num_gpus=None, concurrency=3),
        )
        lf.to_batch([_FakeDS()])
        name, _ds, args, kw = self._calls[-1]
        self.assertEqual(name, "filter")
        self.assertIs(args[0], _Predicate)
        self.assertEqual(kw["concurrency"], 3)
        self.assertNotIn("num_cpus", kw)  # None dropped
        self.assertEqual(kw["fn_constructor_args"], (1,))
        self.assertEqual(kw["fn_constructor_kwargs"], {"inclusive": True})

    def test_union_passes_all_upstreams(self):
        lf = LogicalFunction(_user_fn, lowering=lower_union)
        a, b, c = _FakeDS(), _FakeDS(), _FakeDS()
        lf.to_batch([a, b, c])
        name, got_ds, args, kw = self._calls[-1]
        self.assertEqual(name, "union")
        self.assertIs(got_ds, a)
        self.assertEqual(args, (b, c))
        self.assertEqual(kw, {})


class MiscTest(unittest.TestCase):
    def test_batch_supported_reflects_lowering(self):
        self.assertTrue(LogicalFunction(_user_fn, lowering=lower_union).batch_supported)
        self.assertFalse(LogicalFunction(_user_fn).batch_supported)

    def test_lowering_context_runtime_context_injection(self):
        ctx = LoweringContext(
            upstream_ds=[],
            resources=Resources(),
            runtime_info=RuntimeInfo(),
            fn_constructor_kwargs={"a": 1},
            runtime_context="RC",
            needs_runtime_context=True,
        )
        self.assertEqual(ctx.user_fn_ctor_kwargs, {"a": 1, "runtime_context": "RC"})
        ctx2 = LoweringContext(
            upstream_ds=[],
            resources=Resources(),
            runtime_info=RuntimeInfo(),
            fn_constructor_kwargs={"a": 1},
            needs_runtime_context=False,
        )
        self.assertEqual(ctx2.user_fn_ctor_kwargs, {"a": 1})
