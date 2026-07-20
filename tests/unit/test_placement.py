# SPDX-License-Identifier: Apache-2.0
"""Tests for PlacementPlan + the PG placement cascade — pure Python, no cluster."""

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.scheduler.assignment import WorkerNode
from ray.klein.runtime.scheduler.errors import (
    DeploymentError,
    PlacementCleanupError,
    PlacementError,
)
from ray.klein.runtime.scheduler.placement import (
    PlacementGroupStrategy,
    PlacementPlan,
    RoundRobinStrategy,
)

placement_group_module = importlib.import_module("ray.util.placement_group")


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
    assert p.placement_group_for(ev0) is pg
    assert p.bundle_for(ev0) == 0
    assert p.bundle_for(ev1) == 1
    # an unknown ev falls back to -1 (any bundle)
    assert p.bundle_for(ExecutionVertexId(9, 0)) == -1


def test_vertex_scoped_placement_group_plan_is_elastic_and_bounded_by_one_timeout():
    first = _vertex(1, cpus=1, gpus=0)
    second = _vertex(2, cpus=2, gpus=1)
    graph = SimpleNamespace(execution_vertices=(first, second))
    first_group, second_group = MagicMock(), MagicMock()
    first_group.ready.return_value = "first-ready"
    second_group.ready.return_value = "second-ready"

    with (
        patch.object(placement_group_module, "placement_group", side_effect=(first_group, second_group)) as create,
        patch.object(placement_group_module, "remove_placement_group") as remove,
        patch("ray.klein.get") as get,
    ):
        plan = PlacementGroupStrategy("PACK", ready_timeout=7).plan(graph)
        assert create.call_args_list == [
            call([{"CPU": 1}], strategy="PACK"),
            call([{"CPU": 2, "GPU": 1}], strategy="PACK"),
        ]
        get.assert_called_once_with(["first-ready", "second-ready"], timeout=7)
        assert plan.placement_group_for(first.id) is first_group
        assert plan.placement_group_for(second.id) is second_group
        assert plan.bundle_for(first.id) == 0
        assert plan.bundle_for(second.id) == 0

        plan.rollback()
        assert remove.call_args_list == [call(first_group), call(second_group)]
        plan.rollback()
        assert remove.call_count == 2


def test_placement_close_keeps_failed_group_owned_for_reconciliation():
    first_id = ExecutionVertexId(1, 0)
    second_id = ExecutionVertexId(1, 1)
    first_group, second_group = object(), object()
    remove = MagicMock(side_effect=[None, RuntimeError("ray unavailable"), None])
    plan = PlacementPlan(
        placement_group_by_vertex={first_id: first_group, second_id: second_group},
        bundle_by_vertex={first_id: 0, second_id: 0},
        _remove_group=remove,
    )

    with pytest.raises(RuntimeError, match="ray unavailable"):
        plan.close()

    assert first_id not in plan.placement_group_by_vertex
    assert plan.placement_group_by_vertex[second_id] is second_group
    assert plan.uses_placement_group

    plan.close()
    assert remove.call_args_list == [call(first_group), call(second_group), call(second_group)]
    assert not plan.uses_placement_group


def test_failed_pg_reservation_cleans_every_group_and_keeps_only_failed_ownership():
    first, second, third = _vertex(0), _vertex(1), _vertex(2)
    groups = (MagicMock(), MagicMock(), MagicMock())
    for group in groups:
        group.ready.return_value = object()
    remove = MagicMock(side_effect=(RuntimeError("g0 unavailable"), None, None))

    with (
        patch.object(placement_group_module, "placement_group", side_effect=groups),
        patch.object(placement_group_module, "remove_placement_group", remove),
        patch("ray.klein.get", side_effect=TimeoutError("not ready")),
        pytest.raises(PlacementCleanupError) as captured,
    ):
        PlacementGroupStrategy("PACK", ready_timeout=7).plan(
            SimpleNamespace(execution_vertices=(first, second, third))
        )

    assert remove.call_args_list == [call(group) for group in groups]
    retry_plan = captured.value.plan
    assert retry_plan.placement_group_by_vertex == {first.id: groups[0]}
    remove.side_effect = None
    retry_plan.reconcile()
    assert not retry_plan.uses_placement_group


def test_round_robin_delta_reserves_retained_actor_nodes_before_placing_addition():
    retained_a, retained_b, added = _vertex(0), _vertex(1), _vertex(2)
    graph = SimpleNamespace(execution_vertices=(retained_a, retained_b, added))
    strategy = RoundRobinStrategy()
    owner = PlacementPlan(
        node_by_vertex={retained_a.id: "node-a", retained_b.id: "node-a"},
        strategy=strategy,
    )

    with patch(
        "ray.klein.runtime.scheduler.assignment.cluster_worker_nodes",
        return_value=([WorkerNode(0, 2, 0), WorkerNode(1, 1, 0)], ["node-a", "node-b"]),
    ):
        transition = owner.begin_rescale(graph, added=(added,), removed=())

    assert transition.candidate_plan.node_for(added.id) == "node-b"


@pytest.mark.parametrize("strategy", ("STRICT_PACK", "STRICT_SPREAD"))
def test_actor_scoped_elastic_groups_reject_strict_strategy_at_construction(strategy):
    with pytest.raises(ValueError, match=r"support only PACK or SPREAD.*STRICT_\*"):
        PlacementGroupStrategy(strategy, ready_timeout=7)


def test_placement_rescale_reserves_added_then_releases_only_retired_group():
    first = _vertex(1)
    second = _vertex(2)
    third = _vertex(3)
    graph = SimpleNamespace(execution_vertices=(first, second, third))
    first_group, second_group, third_group = MagicMock(), MagicMock(), MagicMock()
    for group in (first_group, second_group, third_group):
        group.ready.return_value = object()

    with (
        patch.object(
            placement_group_module,
            "placement_group",
            side_effect=(first_group, second_group, third_group),
        ),
        patch.object(placement_group_module, "remove_placement_group") as remove,
        patch("ray.klein.get"),
    ):
        plan = PlacementGroupStrategy("PACK", ready_timeout=7).plan(
            SimpleNamespace(execution_vertices=(first, second))
        )
        transition = plan.begin_rescale(graph, added=(third,), removed=(second,))

        assert transition.candidate_plan.placement_group_for(third.id) is third_group
        assert plan.placement_group_for(third.id) is None
        transition.commit()
        assert plan.placement_group_for(first.id) is first_group
        assert plan.placement_group_for(second.id) is second_group
        assert plan.placement_group_for(third.id) is third_group
        assert remove.call_count == 0

        transition.release_retired()
        remove.assert_called_once_with(second_group)
        assert plan.placement_group_for(first.id) is first_group
        assert plan.placement_group_for(second.id) is None
        assert plan.placement_group_for(third.id) is third_group


def test_placement_rescale_rollback_releases_only_candidate_group():
    first = _vertex(1)
    second = _vertex(2)
    first_group, second_group = MagicMock(), MagicMock()
    first_group.ready.return_value = object()
    second_group.ready.return_value = object()

    with (
        patch.object(placement_group_module, "placement_group", side_effect=(first_group, second_group)),
        patch.object(placement_group_module, "remove_placement_group") as remove,
        patch("ray.klein.get"),
    ):
        strategy = PlacementGroupStrategy("PACK", ready_timeout=7)
        plan = strategy.plan(SimpleNamespace(execution_vertices=(first,)))
        transition = plan.begin_rescale(
            SimpleNamespace(execution_vertices=(first, second)),
            added=(second,),
            removed=(),
        )
        transition.rollback()

        remove.assert_called_once_with(second_group)
        assert plan.placement_group_for(first.id) is first_group
        assert plan.placement_group_for(second.id) is None


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


def _vertex(index: int, *, cpus: int = 1, gpus: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        id=ExecutionVertexId(1, index),
        index=index,
        resources=SimpleNamespace(cpus=cpus, gpus=gpus),
    )
