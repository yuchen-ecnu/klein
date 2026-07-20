# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import sys
from pathlib import Path


def test_public_root_does_not_eagerly_import_optional_connectors(project_root: Path) -> None:
    source_ray = project_root / "src" / "ray"
    code = f"""
import sys
import ray
ray.__path__.insert(0, {str(source_ray)!r})
import ray.klein
for name in ('confluent_kafka', 'httpx', 'orjson', 'redis', 'rocksdict', 'rocketmq'):
    assert name not in sys.modules, name
assert callable(ray.klein.from_items)
assert callable(ray.klein.read_canal)
assert callable(ray.klein.read_rocketmq)
assert 'confluent_kafka' not in sys.modules
assert 'redis' not in sys.modules
assert 'rocketmq' not in sys.modules
"""

    child_env = os.environ.copy()
    for name in tuple(child_env):
        if name.startswith("COV_CORE_") or name == "COVERAGE_PROCESS_START":
            child_env.pop(name)

    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        env=child_env,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
