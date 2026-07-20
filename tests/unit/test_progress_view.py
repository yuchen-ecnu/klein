# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import builtins
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from rich.console import Console
from rich.spinner import Spinner

from ray.klein.observability import progress_view
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.job_manager.progress import (
    InstanceCounts,
    OperatorProgress,
    ProgressSnapshot,
)
from ray.klein.runtime.resources import Resources


def _operator(op_id: int, **changes) -> OperatorProgress:
    values = {
        "name": f"operator-{op_id}",
        "op_id": op_id,
        "parallelism": 1,
        "status": "running",
        "rows_out": 0,
    }
    values.update(changes)
    return OperatorProgress(**values)


class _TTY:
    def __init__(self, value: bool = True, error: Exception | None = None) -> None:
        self._value = value
        self._error = error

    def isatty(self) -> bool:
        if self._error is not None:
            raise self._error
        return self._value


@pytest.mark.parametrize("variable", ["KLEIN_NO_RICH_UI", "NO_COLOR"])
def test_interactive_view_honors_environment_opt_out(monkeypatch, variable: str) -> None:
    monkeypatch.setattr(sys, "stdout", _TTY())
    monkeypatch.setenv(variable, "1")

    assert progress_view.is_interactive() is False


@pytest.mark.parametrize("stdout", [_TTY(False), _TTY(error=AttributeError()), _TTY(error=ValueError())])
def test_interactive_view_requires_a_usable_tty(monkeypatch, stdout: _TTY) -> None:
    monkeypatch.delenv("KLEIN_NO_RICH_UI", raising=False)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr(sys, "stdout", stdout)

    assert progress_view.is_interactive() is False


def test_interactive_view_requires_rich(monkeypatch) -> None:
    original_import = builtins.__import__

    def import_without_rich(name, *args, **kwargs):
        if name == "rich":
            raise ImportError("rich unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.delenv("KLEIN_NO_RICH_UI", raising=False)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr(sys, "stdout", _TTY())
    monkeypatch.setattr(builtins, "__import__", import_without_rich)

    assert progress_view.is_interactive() is False


def test_interactive_view_is_enabled_for_a_rich_tty(monkeypatch) -> None:
    monkeypatch.delenv("KLEIN_NO_RICH_UI", raising=False)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setattr(sys, "stdout", _TTY())

    assert progress_view.is_interactive() is True


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0"),
        (999, "999"),
        (1_000, "1.0k"),
        (1_000_000, "1.0M"),
        (1_000_000_000, "1.0B"),
    ],
)
def test_count_formatting_uses_compact_decimal_units(value: int, expected: str) -> None:
    assert progress_view._fmt_count(value) == expected


def test_rate_and_number_formatting_preserve_zero_and_fractional_values() -> None:
    assert progress_view._fmt_rate(None) == "-"
    assert progress_view._fmt_rate(0.0) == "0"
    assert progress_view._fmt_rate(1_999.9) == "2.0k"
    assert progress_view._fmt_num(2.0) == "2"
    assert progress_view._fmt_num(0.25) == "0.25"


def test_updates_compute_rates_clamp_utilization_and_keep_peak_rows(monkeypatch) -> None:
    clock = iter((0.0, 1.0, 2.0, 3.0))
    monkeypatch.setattr(progress_view.time, "monotonic", lambda: next(clock))
    view = progress_view.ProgressView("orders", "STREAMING")

    view.update(
        ProgressSnapshot(
            operators=(
                _operator(1, parallelism=2, rows_out=10, busy_ns=100, backpressure_ns=100),
                _operator(2, rows_out=5),
            )
        )
    )
    view.update(
        ProgressSnapshot(
            operators=(
                _operator(
                    1,
                    parallelism=2,
                    rows_out=30,
                    busy_ns=3_000_000_100,
                    backpressure_ns=50,
                ),
            ),
            restarts=2,
            max_restarts=4,
        )
    )

    assert view._rate[1] == pytest.approx(20.0)
    assert view._util[1] == (1.0, 0.0)
    assert view.total_rows == 35
    assert view._restarts == 2
    assert view._max_restarts == 4

    view.update(
        ProgressSnapshot(operators=(_operator(1, parallelism=2, rows_out=4, busy_ns=0, backpressure_ns=3_000_000_050),))
    )

    assert view._rate[1] == 0.0
    assert view._util[1] == (0.0, 1.0)
    assert view.total_rows == 35


def test_zero_duration_sample_is_ignored_and_live_view_is_refreshed(monkeypatch) -> None:
    clock = iter((0.0, 1.0, 1.0, 2.0))
    monkeypatch.setattr(progress_view.time, "monotonic", lambda: next(clock))
    view = progress_view.ProgressView("orders", "STREAMING")
    view.update(ProgressSnapshot(operators=(_operator(1, rows_out=1, busy_ns=1),)))
    live = Mock()
    view._live = live

    view.update(ProgressSnapshot(operators=(_operator(1, rows_out=2, busy_ns=2),)))

    assert 1 not in view._rate
    assert 1 not in view._util
    live.update.assert_called_once()


def test_progress_cells_have_stable_plain_text() -> None:
    view = progress_view.ProgressView("orders", "STREAMING")
    detailed = _operator(
        1,
        rows_in=1_200,
        rows_out=2_500,
        queued=8,
        capacity=10,
        parallelism=2,
        cpus=0.5,
        gpus=1.0,
        instances=InstanceCounts(running=1, restarting=1, failed=1),
    )

    assert isinstance(view._status_cell("running"), Spinner)
    assert isinstance(view._status_cell("recovering"), Spinner)
    assert view._status_cell("unknown").plain == "·"
    assert view._pct_cell(None, (0.5, 0.8)).plain == "-"
    assert view._pct_cell(0.1, (0.5, 0.8)).plain == "● 10%"
    assert view._pct_cell(0.75, (0.5, 0.8)).plain == "● 75%"
    assert view._pct_cell(0.9, (0.5, 0.8)).plain == "● 90%"
    assert view._backlog_cell(_operator(2, capacity=0)).plain == ""
    assert view._backlog_cell(_operator(2, queued=1, capacity=10)).plain == "10% (1)"
    assert view._backlog_cell(_operator(2, queued=6, capacity=10)).plain == "60% (6)"
    assert view._backlog_cell(detailed).plain == "80% (8)"
    assert view._instances_cell(detailed).plain == "1 run · 1 restart · 1 fail"
    assert view._instances_cell(_operator(3, instances=None)).plain == ""
    assert view._resource_cell(detailed).plain == "1cpu 2gpu"
    assert view._rows_cell(detailed).plain == "1.2k → 2.5k rows"
    assert view._rows_cell(_operator(4, rows_out=3)).plain == "3 rows"


def test_tree_order_renders_fanout_and_union_once() -> None:
    view = progress_view.ProgressView("orders", "STREAMING")
    view._latest = [
        _operator(1, downstream=(2, 3)),
        _operator(2, downstream=(4,)),
        _operator(3, downstream=(4,)),
        _operator(4),
    ]

    rows = view._tree_order()

    assert [(operator.op_id, prefix) for operator, prefix in rows] == [
        (1, ""),
        (2, "├─ "),
        (4, "│  └─ "),
        (3, "└─ "),
    ]


def test_tree_order_keeps_cyclic_dangling_and_legacy_snapshots_visible() -> None:
    view = progress_view.ProgressView("orders", "STREAMING")
    view._latest = [_operator(2, downstream=(1,)), _operator(1, downstream=(2,))]
    assert [(operator.op_id, prefix) for operator, prefix in view._tree_order()] == [(2, ""), (1, "")]

    view._latest = [_operator(3, downstream=(99,)), _operator(2)]
    assert [operator.op_id for operator, _prefix in view._tree_order()] == [2, 3]

    view._latest = [_operator(5), _operator(4)]
    assert [operator.op_id for operator, _prefix in view._tree_order()] == [4, 5]


def test_live_context_performs_initial_and_final_paints() -> None:
    events = []

    class _Live:
        def __init__(self, renderable, **options) -> None:
            events.append(("init", renderable, options))

        def __enter__(self):
            events.append(("enter",))
            return self

        def update(self, renderable) -> None:
            events.append(("update", renderable))

        def __exit__(self, *exc) -> None:
            events.append(("exit", exc))

    view = progress_view.ProgressView("orders", "STREAMING")
    view._live_cls = _Live

    with view as entered:
        assert entered is view

    assert [event[0] for event in events] == ["init", "enter", "update", "exit"]
    assert events[0][2]["refresh_per_second"] == 8
    assert events[0][2]["transient"] is False


def test_exit_without_enter_is_a_noop() -> None:
    view = progress_view.ProgressView("orders", "STREAMING")

    assert view.__exit__(None, None, None) is None


def test_render_contains_semantic_progress_fields() -> None:
    view = progress_view.ProgressView("orders", "STREAMING")
    view._restarts = 1
    view._max_restarts = 3
    view._latest = [
        _operator(
            1,
            name="ReadOrders",
            status="finished",
            rows_out=12,
            instances=InstanceCounts(finished=1),
            downstream=(2,),
        ),
        _operator(2, name="WriteOrders", rows_in=12, rows_out=12),
    ]
    console = Console(width=180, color_system=None, force_terminal=False)

    with console.capture() as capture:
        console.print(view._render())
    rendered = capture.get()

    assert "RAY KLEIN orders" in rendered
    assert "restarts 1/3" in rendered
    assert "ReadOrders" in rendered
    assert "WriteOrders" in rendered
    assert "ROWS (in→out)" in rendered
    assert "RESOURCES" in rendered


class _StopAfterIterations:
    def __init__(self, iterations: int) -> None:
        self._iterations = iterations
        self._checks = 0
        self.waits = []

    def is_set(self) -> bool:
        self._checks += 1
        return self._checks > self._iterations

    def wait(self, timeout: float) -> None:
        self.waits.append(timeout)


def test_render_loop_tolerates_poll_failures_and_keeps_latest_total(monkeypatch) -> None:
    updates = []

    class _View:
        total_rows = 17

        def __init__(self, job_name: str, mode: str) -> None:
            assert (job_name, mode) == ("orders", "STREAMING")

        def __enter__(self):
            return self

        def __exit__(self, *exc) -> None:
            return None

        def update(self, snapshot: ProgressSnapshot) -> None:
            updates.append(snapshot)

    calls = 0

    def provider() -> ProgressSnapshot:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary RPC failure")
        if calls == 2:
            return None
        if calls == 3:
            return ProgressSnapshot()
        return ProgressSnapshot(operators=(_operator(1),))

    stop = _StopAfterIterations(4)
    result = {}
    debug = Mock()
    monkeypatch.setattr(progress_view, "ProgressView", _View)
    monkeypatch.setattr(progress_view.logger, "debug", debug)

    progress_view.render_until_terminal(provider, "orders", "STREAMING", stop, result)

    assert calls == 4
    assert len(updates) == 1
    assert result == {"rows": 17}
    assert stop.waits == [0.25, 0.25, 0.25, 0.25]
    debug.assert_called_once()


def test_render_loop_disables_itself_when_view_setup_fails(monkeypatch) -> None:
    class _BrokenView:
        def __init__(self, *_args) -> None:
            raise RuntimeError("terminal unavailable")

    debug = Mock()
    monkeypatch.setattr(progress_view, "ProgressView", _BrokenView)
    monkeypatch.setattr(progress_view.logger, "debug", debug)

    progress_view.render_until_terminal(Mock(), "orders", "STREAMING", Mock(), {})

    debug.assert_called_once()
    assert "progress view disabled" in debug.call_args.args[0]


class _Graph:
    def __init__(self) -> None:
        source = VertexId("orders", 1)
        transform = VertexId("orders", 2)
        sink = VertexId("orders", 3)
        self.sources = (source,)
        self.vertices = {
            source: SimpleNamespace(
                name="ReadOrders",
                node_type=SimpleNamespace(value="SOURCE"),
                resources=Resources(0.5, 0, 2),
                concurrency=2,
                batch_size=None,
                async_buffer_size=None,
            ),
            transform: SimpleNamespace(
                name="Normalize",
                node_type=SimpleNamespace(value="TRANSFORM"),
                resources=Resources(1, 0, 2),
                concurrency=(2, 4),
                batch_size=8,
                async_buffer_size=16,
            ),
            sink: SimpleNamespace(
                name="WriteOrders",
                node_type=SimpleNamespace(value="SINK"),
                resources=Resources(1, 1, 2),
                concurrency=2,
                batch_size=None,
                async_buffer_size=None,
            ),
        }
        self._edges = {
            source: (SimpleNamespace(target=transform, partitioner="FORWARD"),),
            transform: (SimpleNamespace(target=sink, partitioner="RESCALE"),),
            sink: (),
        }

    def get(self, vertex_id: VertexId):
        return self.vertices[vertex_id]

    def out_edges(self, vertex_id: VertexId):
        return self._edges[vertex_id]


def test_logical_graph_rendering_exposes_topology_and_resource_specs() -> None:
    graph = _Graph()
    graph.sources = (*graph.sources, *graph.sources)
    rendered = progress_view.render_logical_graph(graph)

    assert rendered is not None
    assert "LogicalGraph 3 ops" in rendered
    assert "Source: ReadOrders" in rendered
    assert rendered.count("Source: ReadOrders") == 1
    assert "Normalize" in rendered
    assert "Sink: WriteOrders" in rendered
    assert "[FORWARD]→" in rendered
    assert "[RESCALE]→" in rendered
    assert "parallelism=2" in rendered
    assert "batch=8" in rendered
    assert "async_buf=16" in rendered


def test_logical_graph_rendering_returns_none_without_rich(monkeypatch) -> None:
    original_import = builtins.__import__

    def import_without_rich(name, *args, **kwargs):
        if name.startswith("rich"):
            raise ImportError("rich unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_rich)

    assert progress_view.render_logical_graph(_Graph()) is None


def test_resource_and_node_labels_include_only_configured_details() -> None:
    assert progress_view._resource_specs(Resources(1, 0, 1)) == "cpu=1"
    specs = progress_view._resource_specs(Resources(0.5, 1, 2), batch_size=8, async_buffer_size=16)
    assert specs == "cpu=0.5  gpu=1  batch=8  async_buf=16"

    label = progress_view._node_label("Map", "TRANSFORM", "cyan", (2, 4), specs, "FORWARD")
    assert "[FORWARD]→" in label
    assert "parallelism=2" in label
    assert specs in label

    minimal = progress_view._node_label("Read", "SOURCE", "green", 1, "")
    assert minimal == "[green]Source: Read[/green]"


def test_summary_uses_rich_when_available(monkeypatch) -> None:
    rendered = []
    monkeypatch.setattr(Console, "print", lambda _self, value: rendered.append(value))

    progress_view.print_summary("orders", "FINISHED", 1.25, 1_500)

    assert len(rendered) == 1
    assert rendered[0].plain == "✔ FINISHED  orders  1.2s  1.5k rows"


def test_summary_falls_back_to_plain_print(monkeypatch) -> None:
    def fail_print(_self, _value) -> None:
        raise RuntimeError("console closed")

    fallback = Mock()
    debug = Mock()
    monkeypatch.setattr(Console, "print", fail_print)
    monkeypatch.setattr(builtins, "print", fallback)
    monkeypatch.setattr(progress_view.logger, "debug", debug)

    progress_view.print_summary("orders", "FAILED", 2.0, 3)

    fallback.assert_called_once_with("FAILED  orders  2.0s  3 rows")
    debug.assert_called_once()
