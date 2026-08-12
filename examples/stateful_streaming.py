# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.klein import KeyedProcessFunction, Pipeline
from ray.klein.state import KeyedStateContext, ValueStateDescriptor


class RunningTotal(KeyedProcessFunction):
    total = ValueStateDescriptor("running-total")

    def process(self, row: dict[str, Any], context: KeyedStateContext) -> dict[str, Any]:
        state = context.state(self.total)
        total = (state.value or 0) + row["amount"]
        state.value = total
        return {"customer": row["customer"], "total": total}


def run() -> list[dict]:
    pipeline = Pipeline({"execution.runtime.mode": "streaming"}, name="stateful-streaming")
    return (
        pipeline.from_items(
            [
                {"customer": "Ada", "amount": 4},
                {"customer": "Ada", "amount": 7},
                {"customer": "Grace", "amount": 3},
            ]
        )
        .key_by(lambda row: row["customer"])
        .process(RunningTotal())
        .collect()
        .result()
    )


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
