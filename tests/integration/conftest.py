# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

import pytest

from tests.support.debug_runtime import reset_debug_runtime

_INTEGRATION_ROOT = Path(__file__).resolve().parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """The integration tier is defined by directory, not a filename allowlist."""

    marker = pytest.mark.integration
    for item in items:
        if item.path.is_relative_to(_INTEGRATION_ROOT):
            item.add_marker(marker)


@pytest.fixture(scope="module", autouse=True)
def _module_ray_cluster(ray_cluster):
    """Every integration module receives a managed Ray lifecycle."""

    return ray_cluster


@pytest.fixture(autouse=True)
def _isolated_debug_runtime():
    reset_debug_runtime()
    yield
    reset_debug_runtime()
