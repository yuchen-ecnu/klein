# SPDX-License-Identifier: Apache-2.0

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import ray
from ray.klein._internal.constants import ComponentName
from ray.klein.api.job_status import JobStatus


def test_detached_job_manager_survives_creator_process(ray_cluster, project_root: Path) -> None:
    """A named Klein control actor must outlive the driver that created it."""
    namespace = f"klein-driver-survival-{uuid4().hex[:8]}"
    address = ray_cluster.address_info["address"]
    code = """
import sys
import ray
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.job_manager.job_manager import JobManager

ray.init(address=sys.argv[1], logging_level='ERROR')
manager = JobManager.create(Configuration(), namespace=sys.argv[2])
ray.get(manager.job_status())
"""
    environment = os.environ.copy()
    environment.pop("RAY_KLEIN_DEBUG", None)
    result = subprocess.run(
        [sys.executable, "-c", code, address, namespace],
        cwd=project_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stderr

    manager = ray.get_actor(ComponentName.KLEIN_JOB_MANAGER, namespace=namespace)
    try:
        assert ray.get(manager.job_status.remote()) == JobStatus.CREATED
    finally:
        ray.kill(manager, no_restart=True)
