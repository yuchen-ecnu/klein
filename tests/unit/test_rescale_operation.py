# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.runtime.job_manager.rescale_operation import RescaleOperation


def _accepted() -> RescaleOperation:
    return RescaleOperation.accepted(
        operation_id="resize-1",
        job_id="job-1",
        operator_id=2,
        operator_name="Map",
        previous_parallelism=2,
        target_parallelism=4,
        now_ms=10,
    )


def test_rescale_operation_enforces_ordered_lifecycle() -> None:
    operation = _accepted()

    operation.transition(status="RUNNING", phase="COORDINATING", started_at_ms=11, now_ms=11)
    operation.transition(status="STABILIZING", phase="STABILIZING", now_ms=12)
    operation.transition(status="COMPLETED", phase="COMPLETED", now_ms=13)

    assert operation.to_dict() == {
        "operation_id": "resize-1",
        "job_id": "job-1",
        "operator_id": 2,
        "operator_name": "Map",
        "previous_parallelism": 2,
        "parallelism": 4,
        "target_parallelism": 4,
        "status": "COMPLETED",
        "phase": "COMPLETED",
        "accepted_at_ms": 10,
        "started_at_ms": 11,
        "updated_at_ms": 13,
        "ended_at_ms": 13,
        "error": None,
    }


def test_rescale_operation_rejects_skipped_status_transition() -> None:
    operation = _accepted()

    with pytest.raises(RuntimeError, match="ACCEPTED -> COMPLETED"):
        operation.transition(status="COMPLETED", phase="COMPLETED", now_ms=11)


def test_rescale_operation_rejects_backward_phase() -> None:
    operation = _accepted()
    operation.transition(status="RUNNING", phase="COORDINATING", started_at_ms=11, now_ms=11)

    with pytest.raises(RuntimeError, match="move backward"):
        operation.transition(status="FAILED", phase="QUEUED", now_ms=12)
