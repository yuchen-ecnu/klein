# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.state import state_backend_factory
from ray.klein.state.memory_state_backend import MemoryStateBackend
from ray.klein.state.rocks_db_state_backend import RocksDBStateBackend


def _configuration(tmp_path: Path, backend: str) -> Configuration:
    config = Configuration(include_environment=False)
    config.set(StateOptions.BACKEND, backend)
    config.set(StateOptions.LOCAL_DIRECTORY, str(tmp_path))
    return config


def test_memory_backend_is_created_without_local_files(tmp_path: Path) -> None:
    config = _configuration(tmp_path, " MeMoRy ")

    backend = state_backend_factory.create_state_backend(config, "job", "task")
    state_backend_factory.discard_state_backend(config, "job", "task")

    assert isinstance(backend, MemoryStateBackend)
    assert list(tmp_path.iterdir()) == []


def test_rocksdb_backend_uses_escaped_task_local_path_and_can_be_discarded(tmp_path: Path) -> None:
    config = _configuration(tmp_path, "RoCkSdB")

    backend = state_backend_factory.create_state_backend(config, "job/一", "task name", reset=True)
    expected = tmp_path / "job%2F%E4%B8%80" / "task%20name"

    assert isinstance(backend, RocksDBStateBackend)
    assert expected.is_dir()
    backend.close()
    state_backend_factory.discard_state_backend(config, "job/一", "task name")
    assert not expected.exists()


def test_unknown_backend_is_rejected(tmp_path: Path) -> None:
    config = _configuration(tmp_path, "remote")

    with pytest.raises(ValueError, match="unsupported managed state backend: 'remote'"):
        state_backend_factory.create_state_backend(config, "job", "task")


def test_missing_rocksdict_has_actionable_error(monkeypatch, tmp_path: Path) -> None:
    config = _configuration(tmp_path, "rocksdb")

    def missing(*_args, **_kwargs):
        raise ModuleNotFoundError("missing rocksdict", name="rocksdict")

    monkeypatch.setattr(state_backend_factory, "RocksDBStateBackend", missing)

    with pytest.raises(ModuleNotFoundError, match=r"ray-klein\[rocksdb\]"):
        state_backend_factory.create_state_backend(config, "job", "task")


def test_unrelated_backend_import_error_is_not_rewritten(monkeypatch, tmp_path: Path) -> None:
    config = _configuration(tmp_path, "rocksdb")

    def missing(*_args, **_kwargs):
        raise ModuleNotFoundError("missing dependency", name="other_dependency")

    monkeypatch.setattr(state_backend_factory, "RocksDBStateBackend", missing)

    with pytest.raises(ModuleNotFoundError) as error:
        state_backend_factory.create_state_backend(config, "job", "task")
    assert error.value.name == "other_dependency"
