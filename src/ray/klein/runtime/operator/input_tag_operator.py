# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class InputTagOperator(StreamOperator, OneInputOperator):
    """Internal graph operator that preserves a stable two-input side tag."""

    def __init__(self, logical_function=None, *, input_tag: int) -> None:
        super().__init__(logical_function)
        if input_tag not in {0, 1}:
            raise ValueError("input_tag must be 0 or 1")
        self._input_tag = input_tag

    def process_element(self, record: Record) -> None:
        record.input_tag = self._input_tag
        self.collect(record)

    def _spec_parameters(self) -> dict[str, Any]:
        return {"input_tag": self._input_tag}
