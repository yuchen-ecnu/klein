# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from unittest.mock import MagicMock

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.coordinator.checkpoint_strategy import AlignedCheckpointStrategy
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import Barrier
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.worker.pump import InboxPump


class _Coordinator:
    def __init__(self) -> None:
        self.aligned = []

    def notify_checkpoint_aligned(self, barrier_id, vertex_id):
        self.aligned.append((barrier_id, vertex_id))
        return True


def test_shared_epoch_prepares_parallel_sink_once_after_all_inputs_align() -> None:
    root_a = ExecutionVertexId(1, 0)
    root_b = ExecutionVertexId(2, 0)
    upstream_a = ExecutionVertexId(3, 0)
    upstream_b = ExecutionVertexId(4, 0)
    sink_id = ExecutionVertexId(5, 0)
    coordinator = _Coordinator()
    strategy = AlignedCheckpointStrategy(
        coordinator,
        {root_a: 1, root_b: 1},
        sink_id,
        OperatorType.SINK,
        Configuration(include_environment=False),
        is_committer=True,
        synchronous_notify=True,
        input_vertex_ids=(upstream_a, upstream_b),
    )
    operator = MagicMock()
    metrics = SimpleNamespace(
        barriers_out=SimpleNamespace(inc=MagicMock()),
        observe_barrier=MagicMock(),
    )
    state = SimpleNamespace(operator=operator, checkpoint_strategy=strategy, metrics=metrics)
    task = MagicMock()
    task.snapshot_operator_state.return_value = 0
    pump = InboxPump(task, state, watermark=None, emit_pipeline=None)

    pump.handle_barrier(Barrier(7, source_id=root_a), sender_vertex_id=upstream_a)
    task.prepare_sink_commit.assert_not_called()

    pump.handle_barrier(Barrier(7, source_id=root_b), sender_vertex_id=upstream_b)
    pump.handle_barrier(Barrier(7, source_id=root_a), sender_vertex_id=upstream_a)

    task.prepare_sink_commit.assert_called_once_with(7)
    operator.flush.assert_called_once_with()
    task.snapshot_operator_state.assert_called_once_with(7)
    task.checkpoint_barrier_aligned.assert_called_once_with(7)
    assert coordinator.aligned == [(7, sink_id)]

    # A timed-out partial epoch must not prepare on a late peer barrier, and it
    # must not poison the next complete epoch.
    pump.handle_barrier(Barrier(8, source_id=root_a), sender_vertex_id=upstream_a)
    assert strategy.abort_checkpoint(8)
    pump.handle_barrier(Barrier(8, source_id=root_b), sender_vertex_id=upstream_b)
    assert task.prepare_sink_commit.call_count == 1

    pump.handle_barrier(Barrier(9, source_id=root_a), sender_vertex_id=upstream_a)
    pump.handle_barrier(Barrier(9, source_id=root_b), sender_vertex_id=upstream_b)
    assert [call.args for call in task.prepare_sink_commit.call_args_list] == [(7,), (9,)]
    assert coordinator.aligned == [(7, sink_id), (9, sink_id)]
