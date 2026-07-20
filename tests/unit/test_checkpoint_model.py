# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.coordinator.checkpoint import Checkpoint, CheckpointStatus
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId


def test_domain_checkpoint_rejects_acknowledgements_from_other_committers() -> None:
    source = ExecutionVertexId(1, 0)
    sink = ExecutionVertexId(3, 0)
    outsider = ExecutionVertexId(4, 0)
    checkpoint = Checkpoint(
        7,
        1,
        (source,),
        coordinated=True,
        domain_id="checkpoint-domain-a",
        required_committers=[sink],
    )

    assert checkpoint.required_committers == (sink,)
    assert checkpoint.acknowledge(outsider) is False
    assert checkpoint.acknowledgements == 0
    assert checkpoint.status == CheckpointStatus.CREATED

    assert checkpoint.acknowledge(sink) is True
    assert checkpoint.acknowledge(sink) is True
    assert checkpoint.acknowledgements == 1
    assert checkpoint.status == CheckpointStatus.NOTIFYING


def test_legacy_checkpoint_keeps_positional_coordinated_and_open_ack_set() -> None:
    source = ExecutionVertexId(1, 0)
    sink = ExecutionVertexId(2, 0)
    checkpoint = Checkpoint(7, 1, (source,), True)

    assert checkpoint.coordinated is True
    assert checkpoint.domain_id is None
    assert checkpoint.required_committers == ()
    assert checkpoint.acknowledge(sink) is True
