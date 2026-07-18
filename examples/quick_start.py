# SPDX-License-Identifier: Apache-2.0
import ray


def main() -> None:
    ray.klein.reset_context().enable_interactive_mode()
    rows = (
        ray.klein.from_items(
            [
                {"name": "Ada", "amount": 4},
                {"name": "Grace", "amount": 7},
            ]
        )
        .map(lambda row: {**row, "amount": row["amount"] * 2})
        .take_all()
    )
    print(rows)


if __name__ == "__main__":
    main()
