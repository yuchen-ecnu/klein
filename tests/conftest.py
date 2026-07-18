# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import gc
import os
import sys
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

import ray

# ``ray`` is a regular installed package, so a source checkout cannot extend
# it through PYTHONPATH alone. Wheels install ``ray/klein`` next to Ray; tests
# add the source-tree package portion explicitly before importing it.
_SOURCE_RAY = Path(__file__).resolve().parents[1] / "src" / "ray"


def _activate_source_klein() -> None:
    """Make this checkout's ``ray.klein`` win in drivers and Ray workers."""

    source_ray = str(_SOURCE_RAY)
    if source_ray not in ray.__path__:
        ray.__path__.insert(0, source_ray)

    loaded = sys.modules.get("ray.klein")
    loaded_path = Path(getattr(loaded, "__file__", "")) if loaded is not None else None
    source_klein = _SOURCE_RAY / "klein"
    if loaded_path is not None and not loaded_path.is_relative_to(source_klein):
        for module_name in tuple(sys.modules):
            if module_name == "ray.klein" or module_name.startswith("ray.klein."):
                sys.modules.pop(module_name, None)


_activate_source_klein()

from ray.klein.api.klein_context import KleinContext  # noqa: E402
from ray.klein.config.configuration import Configuration  # noqa: E402
from tests.support.waiting import wait_until  # noqa: E402


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-external",
        action="store_true",
        default=False,
        help="run tests that require Docker or external services",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """External-service tests are opt-in; test tier is never inferred by filename."""

    if config.getoption("--run-external"):
        return
    skip = pytest.mark.skip(reason="requires --run-external")
    for item in items:
        if item.get_closest_marker("external") is not None:
            item.add_marker(skip)


@pytest.fixture()
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture()
def test_data_dir() -> Path:
    return Path(__file__).resolve().parent / "data"


@pytest.fixture()
def configuration() -> Configuration:
    return Configuration()


@pytest.fixture()
def context(configuration: Configuration) -> KleinContext:
    return KleinContext(configuration)


@pytest.fixture()
def interactive_context(configuration: Configuration) -> KleinContext:
    context = KleinContext(configuration)
    context.enable_interactive_mode()
    return context


@pytest.fixture()
def eventually():
    return wait_until


@pytest.fixture(scope="module")
def ray_cluster(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    """Start one isolated local Ray runtime per integration-test module."""

    import ray

    ray_temp_dir = tmp_path_factory.mktemp("ray")
    short_temp_link: Path | None = None
    if os.name != "nt":
        # Ray's session/socket suffix leaves little room under macOS's 103-byte
        # AF_UNIX limit. Keep pytest ownership of the real directory but expose
        # it to Ray through a collision-free short path.
        short_temp_link = Path("/tmp") / f"rk-{uuid4().hex[:8]}"
        short_temp_link.symlink_to(ray_temp_dir, target_is_directory=True)
        ray_temp_dir = short_temp_link
    environment = pytest.MonkeyPatch()
    environment.setenv("RAY_TMPDIR", str(ray_temp_dir))
    # Ray 2.50 warns about its legacy accelerator-env override during init;
    # opt into the announced future behavior so warnings-as-errors remains useful.
    environment.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

    source_ray = str(_SOURCE_RAY)

    def activate_source_klein_worker() -> None:
        # A nested function is serialized by value; workers need not import the
        # checkout's ``tests`` package before this hook can adjust ray.__path__.
        import ray

        if source_ray not in ray.__path__:
            ray.__path__.insert(0, source_ray)

    options = {
        # Klein's placement planner reads the public dashboard node API.
        "include_dashboard": True,
        "num_cpus": 8,
        "_temp_dir": str(ray_temp_dir),
        "_system_config": {"enable_metrics_collection": False},
        # Ray worker processes do not execute pytest's conftest import path
        # setup. Apply the same source selection before task deserialization so
        # drivers and workers cannot mix different ray.klein installations.
        "runtime_env": {"worker_process_setup_hook": activate_source_klein_worker},
    }

    def shutdown() -> None:
        # Recent Ray versions can finalize reaper pipes during Python 3.12 shutdown.
        # Collect them while ResourceWarning is locally suppressed; application
        # warnings remain errors everywhere else in the suite.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            ray.shutdown()
            gc.collect()

    try:
        if ray.is_initialized():
            shutdown()
        # Ray 2.50 leaves temporary /dev/null wrappers for cyclic GC after
        # process startup. Collect them at the integration boundary while only
        # ResourceWarning is suppressed; all application warnings stay errors.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            context = ray.init(**options)
            gc.collect()
        data_context = ray.data.DataContext.get_current()
        data_context.enable_progress_bars = False
        data_context.print_on_execution_start = False
        yield context
    finally:
        if ray.is_initialized():
            shutdown()
        environment.undo()
        if short_temp_link is not None:
            short_temp_link.unlink(missing_ok=True)
