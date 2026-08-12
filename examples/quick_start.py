# SPDX-License-Identifier: Apache-2.0
import ray.klein as klein


def main() -> None:
    pipeline = klein.pipeline(name="quick-start")
    rows = (
        pipeline.from_items(
            [
                {"name": "Ada", "amount": 4},
                {"name": "Grace", "amount": 7},
            ]
        )
        .map(lambda row: {**row, "amount": row["amount"] * 2})
        .collect()
        .result()
    )
    print(rows)


if __name__ == "__main__":
    main()
