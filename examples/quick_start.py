# SPDX-License-Identifier: Apache-2.0
import ray
import ray.klein


def main() -> None:
    stream = ray.klein.from_items(
        [
            {"name": "Ada", "amount": 4},
            {"name": "Grace", "amount": 7},
        ]
    ).map(lambda row: {**row, "amount": row["amount"] * 2})
    stream.take_all()
    rows = ray.klein.execute("quick-start").get()
    print(rows)


if __name__ == "__main__":
    main()
