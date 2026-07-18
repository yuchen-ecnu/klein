# SPDX-License-Identifier: Apache-2.0
"""Tests for PlacementPlan + the PG placement cascade — pure Python, no cluster."""

from unittest.mock import MagicMock, patch

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.scheduler.errors import DeploymentError, PlacementError
from ray.klein.runtime.scheduler.placement import PlacementPlan


def test_empty_plan_is_native():
    p = PlacementPlan()
    assert not p.uses_placement_group
    assert p.node_for(ExecutionVertexId(1, 0)) is None
    assert p.bundle_for(ExecutionVertexId(1, 0)) == -1


def test_node_pin_plan():
    ev = ExecutionVertexId(1, 0)
    p = PlacementPlan(node_by_vertex={ev: "node-abc"})
    assert not p.uses_placement_group
    assert p.node_for(ev) == "node-abc"


def test_placement_group_plan():
    ev0, ev1 = ExecutionVertexId(1, 0), ExecutionVertexId(2, 0)
    pg = object()
    p = PlacementPlan(placement_group=pg, bundle_by_vertex={ev0: 0, ev1: 1})
    assert p.uses_placement_group
    assert p.placement_group is pg
    assert p.bundle_for(ev0) == 0
    assert p.bundle_for(ev1) == 1
    # an unknown ev falls back to -1 (any bundle)
    assert p.bundle_for(ExecutionVertexId(9, 0)) == -1


def test_error_hierarchy():
    # PlacementError is a DeploymentError so the cascade's `except PlacementError`
    # catches it AND schedule()'s `except DeploymentError` would too.
    e = PlacementError("placement-group", "group not ready")
    assert isinstance(e, DeploymentError)
    assert e.stage == "create workers"
    assert "placement-group" in str(e) and "group not ready" in str(e)


def test_create_remote_actor_pg_branch():
    """A placement_group arg must produce a PlacementGroupSchedulingStrategy,
    taking precedence over schedule_node_id."""
    import ray.klein.runtime.actor as au

    captured = {}

    def fake_create(actor_clz, **ray_remote_args):
        captured.update(ray_remote_args)
        m = MagicMock()
        m.remote.return_value = MagicMock()
        return m

    pg = MagicMock()
    with (
        patch.object(au, "_create_remote_actor", side_effect=fake_create),
        patch.object(au.ray.klein, "is_debug_mode", return_value=False),
    ):
        au.create_remote_actor(
            _PlainActor,
            construct_args={},
            ray_remote_args={"num_cpus": 1},
            schedule_node_id="node-should-be-ignored",
            placement_group=pg,
            placement_group_bundle_index=3,
        )
    strat = captured.get("scheduling_strategy")
    assert strat is not None
    # PlacementGroupSchedulingStrategy carries the pg + bundle index.
    assert getattr(strat, "placement_group", None) is pg
    assert getattr(strat, "placement_group_bundle_index", None) == 3


class _PlainActor:
    def __init__(self, **kwargs):
        pass
