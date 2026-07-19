# SPDX-License-Identifier: Apache-2.0
import ray


def run() -> list[dict]:
    ray.klein.reset_context().enable_interactive_mode()
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
    return sorted(result.take_all(), key=lambda row: row["customer"])


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
