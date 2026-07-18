# SPDX-License-Identifier: Apache-2.0
"""Daft-style live progress view for ``JobClient.wait()``.

Client-side only. Renders one progress line per operator (spinner, name, rows,
rows/s, status) using ``rich`` when stdout is an interactive terminal; in CI /
pipes / ``ray job submit`` it stays disabled and the caller keeps its plain
logging path. Strictly read-only — it never touches job control, so any error
here is swallowed rather than allowed to disturb the running job.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, MutableMapping, Sequence
from threading import Event
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.job_manager.progress import OperatorProgress, ProgressSnapshot

if TYPE_CHECKING:
    from rich.console import Group
    from rich.spinner import Spinner
    from rich.text import Text

logger = get_logger(__name__)

_STATUS_STYLE = {
    "running": ("●", "cyan"),
    "finished": ("✔", "green"),
    "failed": ("✖", "red"),
    "recovering": ("⟳", "yellow"),
    "pending": ("·", "grey50"),
}


def is_interactive() -> bool:
    """True only when a live UI is safe and wanted.

    Requires a TTY, no opt-out env, and an importable ``rich``. Honors the
    conventional ``NO_COLOR`` and a klein-specific ``KLEIN_NO_RICH_UI`` escape.
    """
    if os.environ.get("KLEIN_NO_RICH_UI") or os.environ.get("NO_COLOR"):
        return False
    try:
        if not sys.stdout.isatty():
            return False
    except (AttributeError, ValueError):
        return False
    try:
        import rich  # noqa: F401  # pylint: disable=unused-import
    except ImportError:
        return False
    return True


def _fmt_count(n: int) -> str:
    for unit, div in (("B", 1_000_000_000), ("M", 1_000_000), ("k", 1_000)):
        if n >= div:
            return f"{n / div:.1f}{unit}"
    return str(n)


def _fmt_rate(r: float | None) -> str:
    # None => no sample computed yet (first snapshot); show a placeholder.
    # 0.0  => a real measured zero (idle/low traffic); show "0". Unit (r/s)
    # lives in the column header, not on every cell.
    if r is None:
        return "-"
    return _fmt_count(int(r))


def _fmt_num(x: float) -> str:
    """Compact number: drop the decimal for integers (1cpu), keep it otherwise."""
    return str(int(x)) if x == int(x) else f"{x:g}"


class ProgressView:
    """A ``rich.Live`` table of operator progress lines.

    Driven by repeated ``update(snapshot)`` calls; computes a rolling rows/s from
    successive snapshots using a monotonic clock.
    """

    def __init__(self, job_name: str, mode: str) -> None:
        from rich.console import Console
        from rich.live import Live

        self._job_name = job_name
        self._mode = str(mode)
        self._console = Console()
        self._live: Live | None = None
        self._start = time.monotonic()
        # op_id -> (last_rows, last_monotonic) for rate; op_id -> last rate
        self._last: dict[int, tuple[int, float]] = {}
        self._rate: dict[int, float] = {}
        self._peak_rows: dict[int, int] = {}
        # op_id -> (busy_ns, backpressure_ns, last_monotonic) for the time-
        # accounting columns; op_id -> last computed (busy%, bp%).
        self._last_busy: dict[int, tuple[int, int, float]] = {}
        self._util: dict[int, tuple[float, float]] = {}
        self._latest: list[OperatorProgress] = []
        self._restarts: int = 0
        self._max_restarts: int = 0
        self._live_cls = Live

    def __enter__(self) -> ProgressView:
        self._live = self._live_cls(
            self._render(),
            console=self._console,
            refresh_per_second=8,
            transient=False,
        )
        self._live.__enter__()
        return self

    def __exit__(self, *exc: object) -> None:
        if self._live is not None:
            # Final paint, then release the terminal.
            self._live.update(self._render())
            self._live.__exit__(*exc)

    def update(self, snapshot: ProgressSnapshot) -> None:
        now = time.monotonic()
        operators = snapshot.operators
        self._restarts = snapshot.restarts
        self._max_restarts = snapshot.max_restarts
        for op in operators:
            prev = self._last.get(op.op_id)
            if prev is not None:
                prev_rows, prev_t = prev
                dt = now - prev_t
                if dt > 0:
                    self._rate[op.op_id] = max(0.0, (op.rows_out - prev_rows) / dt)
            self._last[op.op_id] = (op.rows_out, now)
            # Track the peak per-op count so a post-terminal teardown (handles
            # gone -> 0) can't erase the total we display in the summary.
            self._peak_rows[op.op_id] = max(self._peak_rows.get(op.op_id, 0), op.rows_out)
            # busy% / bp%: diff the summed ns counters, normalize by the wall
            # interval times parallelism (the counters sum across subtasks, so
            # N subtasks fully busy advance ~N×dt). clamp to [0,1] so a counter
            # reset on restart (negative delta) or float jitter never overshoots.
            pbusy = self._last_busy.get(op.op_id)
            busy_ns = getattr(op, "busy_ns", 0)
            bp_ns = getattr(op, "backpressure_ns", 0)
            if pbusy is not None:
                p_busy, p_bp, p_t = pbusy
                dt_ns = (now - p_t) * 1e9 * max(1, op.parallelism)
                if dt_ns > 0:
                    self._util[op.op_id] = (
                        max(0.0, min(1.0, (busy_ns - p_busy) / dt_ns)),
                        max(0.0, min(1.0, (bp_ns - p_bp) / dt_ns)),
                    )
            self._last_busy[op.op_id] = (busy_ns, bp_ns, now)
        self._latest = operators
        if self._live is not None:
            self._live.update(self._render())

    @property
    def total_rows(self) -> int:
        """Peak total rows emitted across all operators (for the final summary)."""
        return sum(self._peak_rows.values())

    def _status_cell(self, status: str) -> Spinner | Text:
        """Animated spinner for active ops, static glyph for terminal ones."""
        from rich.spinner import Spinner
        from rich.text import Text

        if status == "running":
            return Spinner("dots", style="cyan")
        if status == "recovering":
            return Spinner("dots", style="yellow")
        glyph, style = _STATUS_STYLE.get(status, ("·", "grey50"))
        return Text(glyph, style=style)

    @staticmethod
    def _pct_cell(
        pct: float | None,
        bands: tuple[float, float],
        none_text: str = "-",
    ) -> Text:
        """A colored ``● NN%`` cell. ``bands`` is (warn, high) thresholds.

        ``pct`` is a 0..1 ratio or None (no sample yet). Below warn = green,
        warn..high = yellow, >= high = bold red.
        """
        from rich.text import Text

        if pct is None:
            return Text(none_text, style="dim")
        warn, high = bands
        if pct >= high:
            style = "bold red"
        elif pct >= warn:
            style = "yellow"
        else:
            style = "green"
        return Text.assemble(("● ", style), (f"{int(pct * 100)}%", style))

    def _busy_cell(self, op: OperatorProgress) -> Text:
        """Fraction of wall time this operator spent processing records.

        High busy = the operator is CPU-bound and is itself the bottleneck.
        """
        util = self._util.get(op.op_id)
        return self._pct_cell(util[0] if util else None, bands=(0.5, 0.8))

    def _bp_cell(self, op: OperatorProgress) -> Text:
        """Fraction of wall time this operator spent blocked emitting downstream.

        High backpressure = this operator is *not* the bottleneck; its downstream
        is, and the stall propagates upstream from there.
        """
        util = self._util.get(op.op_id)
        return self._pct_cell(util[1] if util else None, bands=(0.25, 0.5))

    def _backlog_cell(self, op: OperatorProgress) -> Text:
        """Inbox fill ``queued / capacity`` — a secondary congestion signal.

        Demoted from the headline metric (BUSY/BP now carry that) to a plain
        backlog gauge: how full this operator's input queue is right now.
        Sources have no inbox (capacity 0) — nothing to show.
        """
        from rich.text import Text

        capacity = getattr(op, "capacity", 0)
        queued = getattr(op, "queued", 0)
        if capacity <= 0:
            return Text("")  # source: no inbox
        ratio = max(0.0, min(1.0, queued / capacity))
        style = "dim" if ratio < 0.5 else ("yellow" if ratio < 0.8 else "red")
        return Text.assemble(
            (f"{int(ratio * 100)}%", style),
            (f" ({_fmt_count(queued)})", "dim"),
        )

    def _instances_cell(self, op: OperatorProgress) -> Text:
        """Per-instance state breakdown: running / pending / restarting.

        The aggregate status collapses an operator to one label; this shows the
        raw subtask counts so a partial recovery ("3 running, 1 restarting") is
        visible. Only non-zero states are listed to keep the cell compact.
        """
        from rich.text import Text

        inst = getattr(op, "instances", None)
        if inst is None:
            return Text("")
        segs = []
        # (count, label, style) — labelled so each state reads at a glance.
        for count, label, style in (
            (inst.running, "run", "green"),
            (inst.restarting, "restart", "bold yellow"),
            (inst.pending, "pend", "grey50"),
            (inst.finished, "done", "cyan"),
            (inst.failed, "fail", "bold red"),
        ):
            if count > 0:
                if segs:
                    segs.append((" · ", "dim"))
                segs.append((f"{count} {label}", style))
        if not segs:
            return Text("")
        return Text.assemble(*segs)

    def _resource_cell(self, op: OperatorProgress) -> Text:
        """Total logical resources this operator reserves from Ray Core.

        ``cpus``/``gpus`` are one subtask's ``ray_remote_args`` reservation; the
        operator's total ask is that times its parallelism — the number that
        actually lands on the cluster.
        """
        from rich.text import Text

        par = max(1, getattr(op, "parallelism", 1))
        cpus = getattr(op, "cpus", 0.0) * par
        gpus = getattr(op, "gpus", 0.0) * par
        total = f"{_fmt_num(cpus)}cpu"
        if gpus > 0:
            total += f" {_fmt_num(gpus)}gpu"
        return Text(total, style="dim")

    def _rows_cell(self, op: OperatorProgress) -> Text:
        """Rows ``in → out`` for a transform; just ``out`` for a source.

        Sources have no input (rows_in == 0), so an ``in → out`` form there is
        noise — we show the single emitted count instead.
        """
        from rich.text import Text

        out = _fmt_count(op.rows_out)
        rows_in = getattr(op, "rows_in", 0)
        if rows_in <= 0:
            return Text.assemble((out, "bold"), (" rows", "dim"))
        return Text.assemble(
            (_fmt_count(rows_in), "dim"),
            (" → ", "dim"),
            (out, "bold"),
            (" rows", "dim"),
        )

    def _tree_order(self) -> list[tuple[OperatorProgress, str]]:
        """Operators in topological order with a tree-prefix per row.

        Walks ``downstream`` edges from the sources so fan-out, union, and
        multi-sink shapes read as an indented tree. Each op is emitted once (on
        first reach); a union therefore appears under its first-seen upstream,
        with its other upstreams still visible as separate branches above. Falls
        back to the flat op_id order if no topology is present (older snapshot).

        Returns a list of ``(op, prefix)`` where ``prefix`` is the tree glyph
        string to prepend to the operator name.
        """
        ops = self._latest
        by_id = {op.op_id: op for op in ops}
        roots = self._tree_roots(ops)
        rows: list[tuple[OperatorProgress, str]] = []
        seen: set[int] = set()
        for index, root in enumerate(roots):
            self._append_tree_rows(root, by_id, rows, seen, "", index == len(roots) - 1, True)
        self._append_unreachable_operators(ops, rows)
        return rows

    @staticmethod
    def _tree_roots(operators: Sequence[OperatorProgress]) -> list[OperatorProgress]:
        targets = {target for op in operators for target in getattr(op, "downstream", ())}
        return sorted(
            (op for op in operators if op.op_id not in targets),
            key=lambda op: op.op_id,
        )

    @staticmethod
    def _append_unreachable_operators(
        operators: Sequence[OperatorProgress],
        rows: list[tuple[OperatorProgress, str]],
    ) -> None:
        # A cycle or malformed snapshot should not hide an operator.
        emitted = {id(row[0]) for row in rows}
        rows.extend((operator, "") for operator in operators if id(operator) not in emitted)

    def _append_tree_rows(
        self,
        operator: OperatorProgress,
        by_id: dict[int, OperatorProgress],
        rows: list[tuple[OperatorProgress, str]],
        seen: set[int],
        prefix: str,
        is_last: bool,
        is_root: bool,
    ) -> None:
        if operator.op_id in seen:
            return
        seen.add(operator.op_id)
        branch = "" if is_root else prefix + ("└─ " if is_last else "├─ ")
        child_prefix = "" if is_root else prefix + ("   " if is_last else "│  ")
        rows.append((operator, branch))
        children = [by_id[target] for target in operator.downstream if target in by_id]
        for index, child in enumerate(children):
            self._append_tree_rows(
                child,
                by_id,
                rows,
                seen,
                child_prefix,
                index == len(children) - 1,
                False,
            )

    def _render(self) -> Group:
        from rich.table import Table
        from rich.text import Text

        elapsed = int(time.monotonic() - self._start)
        parts = [
            ("RAY KLEIN ", "bold magenta"),
            (self._job_name, "bold"),
            (f"  {self._mode}  ", "dim"),
            (f"{elapsed // 60:02d}:{elapsed % 60:02d}", "dim"),
        ]
        if self._restarts > 0:
            limit = f"/{self._max_restarts}" if self._max_restarts else ""
            parts.append((f"  ⟳ restarts {self._restarts}{limit}", "yellow"))
        title = Text.assemble(*parts)
        from rich import box

        table = Table(
            box=box.SIMPLE_HEAD,
            padding=(0, 1),
            header_style="bold dim",
            expand=False,
        )
        table.add_column("")  # spinner / status glyph (no header)
        table.add_column("OPERATOR", no_wrap=True)
        table.add_column("PAR", justify="right", no_wrap=True)
        table.add_column("INSTANCES", no_wrap=True)
        table.add_column("ROWS (in→out)", justify="right", no_wrap=True)
        table.add_column("RATE (r/s)", justify="right", no_wrap=True)
        table.add_column("BUSY", justify="right", no_wrap=True, min_width=6)
        table.add_column("BP", justify="right", no_wrap=True, min_width=6)
        table.add_column("BACKLOG", no_wrap=True)
        table.add_column("RESOURCES", no_wrap=True)
        for op, prefix in self._tree_order():
            name = Text.assemble((prefix, "grey50"), (op.name,))
            # Always show concurrency, including 1, so every row is comparable.
            conc = Text(str(op.parallelism), style="dim")
            table.add_row(
                self._status_cell(op.status),
                name,
                conc,
                self._instances_cell(op),
                self._rows_cell(op),
                Text(_fmt_rate(self._rate.get(op.op_id)), style="green"),
                self._busy_cell(op),
                self._bp_cell(op),
                self._backlog_cell(op),
                self._resource_cell(op),
            )
        from rich.console import Group

        return Group(title, table)


def render_until_terminal(
    progress_provider: Callable[[], ProgressSnapshot],
    job_name: str,
    mode: str,
    stop_event: Event,
    result: MutableMapping[str, int],
) -> None:
    """Poll the progress snapshot ~4x/s and render until ``stop_event`` is set.

    Runs on a daemon thread spawned by ``JobClient.wait()``. Read-only and
    fully defensive: any failure just ends the view, never the job. ``result``
    is a mutable holder the caller reads after join() for the final summary
    (post-terminal re-polling is useless — task handles are torn down).
    """
    try:
        with ProgressView(job_name, mode) as view:
            while not stop_event.is_set():
                try:
                    snapshot = progress_provider()
                    if snapshot and snapshot.operators:
                        view.update(snapshot)
                        result["rows"] = view.total_rows
                except Exception as error:
                    logger.debug("progress poll failed: %s", error)
                stop_event.wait(0.25)
    except Exception as error:
        logger.debug("progress view disabled: %s", error)


_NODE_STYLE = {
    "SOURCE": "green",
    "SINK": "red",
    "TAKE": "red",
    "UNION": "magenta",
    "TRANSFORM": "cyan",
}


def _resource_specs(resources, batch_size=None, async_buffer_size=None) -> str:
    """Format an operator's user-set specs as a ``key=value`` string.

    Shows the knobs a user actually set — cpu, gpu, batch size, async buffer —
    omitting any that are unset/default (gpu=0, no batching, no async buffer) so
    a plain operator stays compact. Shared by the StreamGraph and LogicalGraph
    renderers so both surface the same fields. Returned without styling; the
    caller wraps it (see ``_node_label``).
    """
    parts = [f"cpu={resources.cpus}"]
    if resources.gpus:
        parts.append(f"gpu={resources.gpus}")
    if batch_size and batch_size > 1:
        parts.append(f"batch={batch_size}")
    if async_buffer_size and async_buffer_size > 0:
        parts.append(f"async_buf={async_buffer_size}")
    return "  ".join(parts)


def _node_label(name: str, ntype: str, style: str, concurrency, specs: str, partitioner=None) -> str:
    """One-line Flink-dashboard-style node label for the graph tree.

    Layout: ``[EDGE]→ Role: name (parallelism)   specs`` where

    * the inbound edge type (FORWARD / RESCALE / ADAPTIVE …) is tagged at the
      front, right after the tree branch char, so the connection kind reads with
      the line that connects the two nodes (Flink shows the partition type on
      the edge);
    * ``Role:`` is ``Source:`` / ``Sink:`` for endpoints (mirrors Flink's
      ``Source:`` / ``Sink:`` node titles), omitted for transforms;
    * parallelism is labeled ``parallelism=N`` so the number is unambiguous,
      omitted when 1;
    * the dim ``key=value`` specs trail the line.

    Single line keeps a long pipeline scannable; the tree's own ``├``/``└``
    branches carry the fan-out topology.
    """
    par = concurrency[0] if isinstance(concurrency, tuple) else concurrency
    edge = f"[dim]\\[{partitioner}]→[/dim] " if partitioner is not None else ""
    role = {"SOURCE": "Source: ", "SINK": "Sink: ", "TAKE": "Sink: "}.get(ntype, "")
    head = f"{edge}[{style}]{role}{name}[/{style}]"
    detail = []
    if par and par > 1:
        detail.append(f"parallelism={par}")
    if specs:
        detail.append(specs)
    if detail:
        head += f"   [dim]{'  '.join(detail)}[/dim]"
    return head


def render_logical_graph(graph) -> str | None:
    """Render a LogicalGraph as a rich tree string (sources at the root).

    Returns None if rich is unavailable, so the caller can fall back to the
    graph's plain ``repr``. Pure string production — safe in any context.
    """
    try:
        from rich.console import Console
        from rich.tree import Tree

        def label(vid, partitioner=None) -> str:
            v = graph.get(vid)
            ntype = getattr(v.node_type, "value", str(v.node_type))
            style = _NODE_STYLE.get(ntype, "white")
            specs = _resource_specs(
                v.resources,
                batch_size=v.batch_size,
                async_buffer_size=v.async_buffer_size,
            )
            return _node_label(v.name, ntype, style, v.concurrency, specs, partitioner)

        # Build child trees recursively from each source; an operator with
        # multiple upstreams (union) is shown under each — acceptable for a
        # readable plan view.
        seen_root = set()

        def build(vid, tree) -> None:
            for edge in graph.out_edges(vid):
                child = tree.add(label(edge.target, edge.partitioner))
                build(edge.target, child)

        root = Tree(f"[bold]LogicalGraph[/bold] [dim]{len(graph.vertices)} ops[/dim]")
        for sid in graph.sources:
            if sid in seen_root:
                continue
            seen_root.add(sid)
            node = root.add(label(sid))
            build(sid, node)

        console = Console(width=100, force_terminal=False)
        with console.capture() as cap:
            console.print(root)
        return cap.get()
    except ImportError:
        return None


def render_stream_graph(graph) -> str | None:
    """Render an API-level StreamGraph as a rich tree string (sources at root).

    Returns None if rich is unavailable so the caller falls back to plain text.
    """
    try:
        from rich.console import Console
        from rich.tree import Tree

        def label(node_id, partitioner=None) -> str:
            n = graph.nodes[node_id]
            ntype = getattr(n.node_type, "value", str(n.node_type))
            style = _NODE_STYLE.get(ntype, "white")
            res = n.resources
            rt = getattr(n.operator, "runtime_info", None)
            specs = _resource_specs(
                res,
                batch_size=getattr(rt, "batch_size", None),
                async_buffer_size=getattr(rt, "async_buffer_size", None),
            )
            return _node_label(n.name, ntype, style, res.effective_concurrency, specs, partitioner)

        def build(node_id, tree) -> None:
            for dst in graph.downstream_nodes(node_id):
                part = graph.partitioner_for(node_id, dst)
                child = tree.add(label(dst, part))
                build(dst, child)

        root = Tree(f"[bold]StreamGraph[/bold] [dim]{graph.job_name}[/dim]")
        for sid in sorted(graph.source_nodes):
            node = root.add(label(sid))
            build(sid, node)

        console = Console(width=100, force_terminal=False)
        with console.capture() as cap:
            console.print(root)
        return cap.get()
    except ImportError:
        return None


def print_summary(job_name: str, status: str, elapsed_s: float, rows_out: int) -> None:
    """One-line terminal summary printed after the job reaches a terminal state."""
    try:
        from rich.console import Console
        from rich.text import Text

        glyph, style = {
            "FINISHED": ("✔", "bold green"),
            "FAILED": ("✖", "bold red"),
            "CANCELLED": ("⚠", "bold yellow"),
        }.get(status, ("•", "bold"))
        Console().print(
            Text.assemble(
                (f"{glyph} {status}", style),
                (f"  {job_name}", "bold"),
                (f"  {elapsed_s:.1f}s", "dim"),
                (f"  {_fmt_count(rows_out)} rows", "dim"),
            )
        )
    except Exception:
        logger.debug("Rich terminal summary rendering failed", exc_info=True)
        print(f"{status}  {job_name}  {elapsed_s:.1f}s  {_fmt_count(rows_out)} rows")
