# SPDX-License-Identifier: Apache-2.0
"""Klein lineage tracker: extracts source/sink info from StreamGraph and reports OpenLineage events."""

import uuid

from ray.klein._internal.logging import get_logger
from ray.klein.observability.lineage.extractors import extract_datasets_from_klein_graph
from ray.klein.observability.lineage.models import DatasetInfo, LineageEmitter

logger = get_logger(__name__)


class KleinLineageTracker:
    """One instance per JobClient; manages START/COMPLETE/FAIL/CANCEL lineage reporting."""

    def __init__(self, job_name: str, emitter: LineageEmitter | None = None) -> None:
        if not job_name:
            raise ValueError("lineage job name must not be empty")
        self._run_id = str(uuid.uuid4())
        self._job_name = job_name
        self._inputs: list[DatasetInfo] = []
        self._outputs: list[DatasetInfo] = []
        self._emitter = emitter
        self._enabled = emitter is not None

    def initialize(self, stream_graph) -> None:
        """Extract source/sink lineage from the StreamGraph. No-op when lineage is disabled."""
        if not self._enabled:
            return
        self._inputs, self._outputs = self._extract(stream_graph)

    @property
    def has_lineage(self) -> bool:
        return self._enabled and bool(self._inputs or self._outputs)

    @property
    def inputs(self) -> tuple[DatasetInfo, ...]:
        return tuple(self._inputs)

    @property
    def outputs(self) -> tuple[DatasetInfo, ...]:
        return tuple(self._outputs)

    def report_start(self) -> None:
        if not self.has_lineage:
            logger.debug(
                "Lineage skipping START: enabled=%s, inputs=%d, outputs=%d",
                self._enabled,
                len(self._inputs),
                len(self._outputs),
            )
            return
        try:
            self._emitter.emit("START", self._job_name, self._run_id, self._inputs, self._outputs)
        except Exception:
            logger.warning("Lineage START reporting failed unexpectedly", exc_info=True)

    def report_complete(self) -> None:
        if not self.has_lineage:
            logger.debug(
                "Lineage skipping COMPLETE: enabled=%s, inputs=%d, outputs=%d",
                self._enabled,
                len(self._inputs),
                len(self._outputs),
            )
            return
        try:
            self._emitter.emit("COMPLETE", self._job_name, self._run_id, self._inputs, self._outputs)
        except Exception:
            logger.warning("Lineage COMPLETE reporting failed unexpectedly", exc_info=True)

    def report_fail(self, error: Exception | None = None) -> None:
        if not self.has_lineage:
            logger.debug(
                "Lineage skipping FAIL: enabled=%s, inputs=%d, outputs=%d",
                self._enabled,
                len(self._inputs),
                len(self._outputs),
            )
            return
        try:
            self._emitter.emit(
                "FAIL",
                self._job_name,
                self._run_id,
                self._inputs,
                self._outputs,
                error=str(error) if error else None,
            )
        except Exception:
            logger.warning("Lineage FAIL reporting failed unexpectedly", exc_info=True)

    def report_cancel(self, error: Exception | None = None) -> None:
        if not self.has_lineage:
            logger.debug(
                "Lineage skipping CANCEL: enabled=%s, inputs=%d, outputs=%d",
                self._enabled,
                len(self._inputs),
                len(self._outputs),
            )
            return
        try:
            self._emitter.emit(
                "CANCEL",
                self._job_name,
                self._run_id,
                self._inputs,
                self._outputs,
                error=str(error) if error else None,
            )
        except Exception:
            logger.warning("Lineage CANCEL reporting failed unexpectedly", exc_info=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract(stream_graph) -> tuple[list[DatasetInfo], list[DatasetInfo]]:
        try:
            return extract_datasets_from_klein_graph(stream_graph)
        except Exception:
            logger.debug("Failed to extract lineage from Klein StreamGraph", exc_info=True)
            return [], []
