# SPDX-License-Identifier: Apache-2.0

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
for name in ('aiohttp', 'confluent_kafka', 'orjson', 'redis', 'rocksdict'):
    assert name not in sys.modules, name
assert callable(ray.klein.from_items)
assert 'confluent_kafka' not in sys.modules
assert 'redis' not in sys.modules
"""

    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
