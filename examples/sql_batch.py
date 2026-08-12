# SPDX-License-Identifier: Apache-2.0
from ray.klein import Pipeline


def run() -> list[dict]:
    pipeline = Pipeline(name="sql-batch")
    orders = pipeline.from_items(
        [
            {"customer": "Ada", "amount": 4},
            {"customer": "Ada", "amount": 7},
            {"customer": "Grace", "amount": 3},
        ]
    )
    result = pipeline.sql(
        """
        SELECT customer, SUM(amount) AS total
        FROM orders
        GROUP BY customer
        """,
        tables={"orders": orders},
    )
    rows = result.collect().result()
    return sorted(rows, key=lambda row: row["customer"])


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
