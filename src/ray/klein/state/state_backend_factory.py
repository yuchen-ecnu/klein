# SPDX-License-Identifier: Apache-2.0
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
    root = config.get(StateOptions.LOCAL_DIRECTORY)
    path = Path(root) / quote(job_id, safe="") / quote(task_name, safe="")
    try:
        return RocksDBStateBackend(str(path), reset=reset)
    except ModuleNotFoundError as error:
        if error.name != "rocksdict":
            raise
        raise ModuleNotFoundError(
            "The RocksDB backend requires the optional dependency; "
            "install it with `python -m pip install 'ray-klein[rocksdb]'`."
        ) from error
