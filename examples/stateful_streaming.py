# SPDX-License-Identifier: Apache-2.0
from typing import Any

import ray
from ray.klein import KeyedProcessFunction
from ray.klein.state import KeyedStateContext, ValueStateDescriptor


class RunningTotal(KeyedProcessFunction):
    total = ValueStateDescriptor("running-total")

    def process(self, row: dict[str, Any], context: KeyedStateContext) -> dict[str, Any]:
        state = context.state(self.total)
        total = (state.value or 0) + row["amount"]
        state.value = total
        return {"customer": row["customer"], "total": total}


def run() -> list[dict]:
    context = ray.klein.reset_context({"execution.runtime.mode": "streaming"}).enable_interactive_mode()
    return (
        context.from_items(
            [
                {"customer": "Ada", "amount": 4},
                {"customer": "Ada", "amount": 7},
                {"customer": "Grace", "amount": 3},
            ]
        )
        .key_by(lambda row: row["customer"])
        .process(RunningTotal())
        .take_all()
    )


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
