# SPDX-License-Identifier: Apache-2.0
"""Tests for replay-watermark durability boundaries."""

from unittest.mock import MagicMock

import pytest

from ray.klein.runtime.worker.watermark import WatermarkController, WatermarkMode


@pytest.mark.asyncio
async def test_sink_watermark_flushes_before_advancing() -> None:
    operator = MagicMock()
    controller = WatermarkController(WatermarkMode.SINK, flush_interval_batches=1)
    controller.bind(None, operator, None, None)

    assert controller.note_processed("source-1", 7) is True
    await controller.advance()

    operator.flush.assert_called_once_with()
    assert controller.forwarded_sequence_for("source-1") == 7
