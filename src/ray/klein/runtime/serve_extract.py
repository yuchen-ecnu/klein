# SPDX-License-Identifier: Apache-2.0
"""In-process serve-operator extraction.

The serve deployment needs the *operators* of a job's ``ray_serve_enabled``
region, but a Klein workflow is a plain Python script that ends in
``ctx.execute()``. We don't want users to maintain a second, declarative copy of
that script just so the server can read it — one script, one image, one flag.

So we run the user's original script unchanged with :func:`runpy.run_path`
(``run_name="__main__"``, so ``if __name__ == "__main__"`` blocks fire and the
graph actually gets built), and intercept the moment ``execute()`` is called:
at that point the LogicalGraph can be captured but nothing has been submitted yet.
:class:`JobClient.execute` consults :func:`extracting` and, when set, hands the
sink list here, then raises :class:`_ServeExtractDone` to unwind the script
before it submits a job / sleeps / loops.

``_ServeExtractDone`` extends ``BaseException`` on purpose: user scripts often
wrap ``execute()`` in ``try/except Exception: ... finally: stop()``; a plain
``Exception`` would be swallowed there and the script would carry on to submit a
real job. ``BaseException`` slips past those handlers.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.klein.api.stream_sink import StreamSink
    from ray.klein.config.configuration import Configuration

# Thread-local so a deployment replica that extracts on one thread never affects
# a genuine job submission happening on another.
_state = threading.local()
_extraction_lock = threading.RLock()


class _ServeExtractDone(BaseException):
    """Carries the extracted operators back out of the user script."""

    def __init__(self, operators: list) -> None:
        super().__init__("serve operator extraction complete")
        self.operators = operators


def extracting() -> bool:
    return getattr(_state, "active", False)


def capture_from_sinks(sinks: Sequence[StreamSink], config: Configuration) -> None:
    """Build operators from the in-flight graph and abort the script.

    Called from ``JobClient.execute`` while the user script is mid-run. Raises
    :class:`_ServeExtractDone` so the script never reaches job submission.
    """
    from ray.klein.runtime.graph.logical_graph import LogicalGraph
    from ray.klein.runtime.graph.serve_rewriter import ServeRewriter
    from ray.klein.runtime.serve_functions import instantiate_logical_functions

    graph = LogicalGraph.from_sinks(sinks, "klein-serve-extract", config)
    serve_fns = ServeRewriter(graph).extract_serve_functions()
    if not serve_fns:
        raise RuntimeError(
            "Workflow has no ray_serve_enabled region to extract. Mark the serve "
            "operators with `ray_serve_enabled=True`."
        )
    operators = instantiate_logical_functions(serve_fns)
    captured_chains = getattr(_state, "captured_chains", None)
    if captured_chains is not None:
        captured_chains.append(operators)
    raise _ServeExtractDone(operators)


def run_extraction(entrypoint: str) -> list:
    """Run ``entrypoint`` as ``__main__`` and return its serve operators.

    The script is executed unchanged; ``execute()`` is intercepted to extract
    operators and unwind before any job is submitted.
    """
    import runpy

    from ray.klein.api.klein_context import KleinContext
    from ray.klein.runtime.serve_functions import close_operators

    previous_active = extracting()
    previous_captured_chains = getattr(_state, "captured_chains", None)
    captured_chains: list[list] = []
    handed_off: list = []
    # runpy temporarily mutates interpreter-global __main__/argv state, so the
    # ContextVar isolates Klein's builder while this lock serializes runpy itself.
    with _extraction_lock, KleinContext._isolated():
        _state.active = True
        _state.captured_chains = captured_chains
        try:
            runpy.run_path(entrypoint, run_name="__main__")
        except _ServeExtractDone as done:
            handed_off = done.operators
            return done.operators
        finally:
            _state.active = previous_active
            if previous_captured_chains is None:
                del _state.captured_chains
            else:
                _state.captured_chains = previous_captured_chains
                if handed_off:
                    previous_captured_chains.append(handed_off)
            close_operators(
                [operator for chain in captured_chains for operator in chain],
                excluding=handed_off,
            )

    raise RuntimeError(
        f"Workflow {entrypoint} finished without calling execute(); no serve operators could be extracted."
    )
