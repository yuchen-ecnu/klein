# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.runtime.resources import Resources


@pytest.mark.parametrize("value", [0, -1, True, 1.5, (0, 2), (2, 1), (1, True)])
def test_concurrency_rejects_non_positive_or_non_integer_values(value):
    with pytest.raises((TypeError, ValueError)):
        Resources(concurrency=value)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_resources_must_be_finite_and_non_negative(value):
    with pytest.raises(ValueError):
        Resources(num_cpus=value)


def test_resources_reject_boolean_quantities():
    with pytest.raises(TypeError):
        Resources(num_gpus=True)
