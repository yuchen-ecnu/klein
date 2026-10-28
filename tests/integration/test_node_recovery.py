# SPDX-License-Identifier: Apache-2.0
"""Node loss must restore source, keyed state, and file commits consistently."""

import gc
import json
import time
import warnings
from collections import Counter
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from ray.cluster_utils import Cluster

import ray
import ray.klein as klein
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.restart_strategy_options import RestartStrategyOptions
from ray.klein.config.state_options import StateOptions
from ray.klein.state.value_state_descriptor import ValueStateDescriptor
from tests.support.waiting import wait_until


class _GatedSource(SourceFunction):
    def __init__(self, gate):
        self.gate = gate
        self.index = 0
        self.stopped = False

    def run(self, context):
        while not self.stopped:
            control = json.loads(Path(self.gate).read_text())
            if self.index < control["limit"]:
                self.index += 1
                context.collect({"id": self.index, "key": self.index % 32})
                time.sleep(0.002)
            elif control["finish"]:
                return
            else:
                context.flush()
                context.on_idle()
                time.sleep(0.01)

    def snapshot_state(self, checkpoint_id):
        return self.index

    def restore_state(self, state):
        self.index = state

    def cancel(self):
        self.stopped = True


def _advance(gate, limit, *, finish=False):
    temporary = gate.with_suffix(".next")
    temporary.write_text(json.dumps({"limit": limit, "finish": finish}))
    temporary.replace(gate)


def _rows(directory):
    return [json.loads(line) for path in directory.glob("*.json") for line in path.read_text().splitlines()]


def _ready_counter(namespace):
    snapshot = klein.get_job_snapshot(namespace)
    if snapshot and snapshot["status"] == "RUNNING":
        return next(
            (op for op in snapshot["operators"] if op["name"].startswith("NodeCounter") and op["can_rescale"]),
            None,
        )
    return None


@pytest.fixture
def node_cluster(monkeypatch):
    source_ray = str(Path(klein.__file__).resolve().parent.parent)

    def activate_worker():
        import ray

        if source_ray not in ray.__path__:
            ray.__path__.insert(0, source_ray)

    cluster = Cluster()
    with TemporaryDirectory(prefix="kn-", dir="/tmp") as ray_temp_dir, warnings.catch_warnings():
        # Cluster writes its address to the default Ray directory, even when
        # the head has an explicit temp_dir. Use one isolated root for both,
        # with a short path to stay within Unix-domain socket length limits.
        monkeypatch.setenv("RAY_TMPDIR", ray_temp_dir)
        # Ray's local multi-node test helper can leave socket wrappers to GC.
        warnings.filterwarnings("ignore", message="unclosed <socket.socket", category=ResourceWarning)
        try:
            cluster.add_node(
                num_cpus=0,
                include_dashboard=False,
                dashboard_port=0,
                object_store_memory=256 * 1024**2,
                _system_config={"enable_metrics_collection": False},
            )
            worker = cluster.add_node(num_cpus=4, object_store_memory=256 * 1024**2)
            ray.init(address=cluster.address, runtime_env={"worker_process_setup_hook": activate_worker})
            yield cluster, worker
        finally:
            ray.shutdown()
            cluster.shutdown()
            gc.collect()


@pytest.fixture(scope="module")
def ray_cluster():
    # Each case owns multiple raylets; do not start the default one-node runtime.
    yield


@pytest.mark.timeout(300)
@pytest.mark.parametrize("scale_first", [False, True])
def test_node_loss_preserves_keyed_counts_and_transactional_output(tmp_path, node_cluster, scale_first):
    cluster, worker = node_cluster
    gate, output = tmp_path / "gate.json", tmp_path / "output"
    _advance(gate, 160)
    config = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    config.set(StateOptions.MAX_PARALLELISM, 64)
    config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    config.set(CheckpointOptions.MAX_CONCURRENT, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 80)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(seconds=1))
    config.set(JobManagerOptions.HEALTH_CHECK_INTERVAL, 1)
    config.set(RestartStrategyOptions.DELAY, timedelta(0))
    descriptor = ValueStateDescriptor("node-count")

    def count(row, context):
        state = context.state(descriptor)
        state.value = (state.value or 0) + 1
        return {**row, "count": state.value}

    context = KleinContext(config)
    context.source(
        _GatedSource, fn_constructor_args=[str(gate)], bounded=False, num_cpus=0.1, name="NodeSource"
    ).key_by(lambda row: row["key"]).process(count, concurrency=2, num_cpus=0.4, name="NodeCounter").write_json(
        str(output), concurrency=1, ray_remote_args={"num_cpus": 0.1}
    )
    handle = context.execute("node-recovery")
    try:
        wait_until(lambda: len(_rows(output)) >= 160, timeout=60, description="committed prefix")
        cluster.add_node(num_cpus=4, object_store_memory=256 * 1024**2)
        wait_until(lambda: ray.cluster_resources().get("CPU") == 8, timeout=30, description="added node")
        counter = wait_until(lambda: _ready_counter(handle.namespace), timeout=30)
        if scale_first:
            result = klein.rescale_operator(handle.namespace, counter["op_id"], 4, timeout=45)
            assert result["status"] == "COMPLETED", result
            wait_until(lambda: _ready_counter(handle.namespace), timeout=30)
        _advance(gate, 600)
        time.sleep(0.1)
        cluster.remove_node(worker, allow_graceful=False)
        wait_until(
            lambda: len({row["id"] for row in _rows(output)}) >= 600,
            timeout=100,
            description="committed output after node loss",
        )
        counter = wait_until(lambda: _ready_counter(handle.namespace), timeout=30)
        result = klein.rescale_operator(handle.namespace, counter["op_id"], 1, timeout=45)
        assert result["status"] == "COMPLETED", result
        _advance(gate, 900, finish=True)
        wait_until(lambda: handle.status.is_terminal, timeout=60, description="completion after recovery")
        assert handle.status.name == "FINISHED"
        rows = _rows(output)
        assert Counter(row["id"] for row in rows) == Counter(range(1, 901))
        for key, count in Counter(index % 32 for index in range(1, 901)).items():
            assert sorted(row["count"] for row in rows if row["key"] == key) == list(range(1, count + 1))
    finally:
        if not handle.status.is_terminal:
            handle.cancel(timeout=30)
