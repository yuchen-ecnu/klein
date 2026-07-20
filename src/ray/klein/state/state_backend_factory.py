# SPDX-License-Identifier: Apache-2.0
import shutil
from pathlib import Path
from urllib.parse import quote

from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.state.managed_state_backend import ManagedStateBackend
from ray.klein.state.memory_state_backend import MemoryStateBackend
from ray.klein.state.rocks_db_state_backend import RocksDBStateBackend


def create_state_backend(
    config: Configuration,
    job_id: str,
    task_name: str,
    *,
    reset: bool = False,
) -> ManagedStateBackend:
    backend_type = config.get(StateOptions.BACKEND).strip().lower()
    if backend_type == "memory":
        return MemoryStateBackend()
    if backend_type != "rocksdb":
        raise ValueError(f"unsupported managed state backend: {backend_type!r}")
    path = _state_backend_path(config, job_id, task_name)
    try:
        return RocksDBStateBackend(str(path), reset=reset)
    except ModuleNotFoundError as error:
        if error.name != "rocksdict":
            raise
        raise ModuleNotFoundError(
            "The RocksDB backend requires the optional dependency; "
            "install it with `python -m pip install 'ray-klein[rocksdb]'`."
        ) from error


def discard_state_backend(config: Configuration, job_id: str, task_name: str) -> None:
    """Delete one closed task-local RocksDB backend.

    Runtime-rescale candidates use an operation-scoped backend identity. Once a
    candidate is rolled back, or its predecessor is retired after commit, that
    exact backend can be removed without touching the runtime that remains live.
    """

    if config.get(StateOptions.BACKEND).strip().lower() != "rocksdb":
        return
    shutil.rmtree(_state_backend_path(config, job_id, task_name), ignore_errors=True)


def _state_backend_path(config: Configuration, job_id: str, task_name: str) -> Path:
    root = config.get(StateOptions.LOCAL_DIRECTORY)
    return Path(root) / quote(job_id, safe="") / quote(task_name, safe="")
