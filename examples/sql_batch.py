# SPDX-License-Identifier: Apache-2.0
import ray
import ray.klein


def run() -> list[dict]:
    orders = ray.klein.from_items(
        [
            {"customer": "Ada", "amount": 4},
            {"customer": "Ada", "amount": 7},
            {"customer": "Grace", "amount": 3},
        ]
    )
    result = ray.klein.sql(
        """
        SELECT customer, SUM(amount) AS total
        FROM orders
        GROUP BY customer
        """,
        tables={"orders": orders},
    )
    result.take_all()
    rows = ray.klein.execute("sql-batch").get()
    return sorted(rows, key=lambda row: row["customer"])


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
