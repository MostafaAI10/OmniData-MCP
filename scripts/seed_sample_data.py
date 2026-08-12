"""
One-off script to populate data/omnidata.duckdb with sample tables so
Phase 1's tools have something real to query.

Run with:  uv run python scripts/seed_sample_data.py

Creates:
  - sales:     ~500 rows of order-level data with some NULLs and
               outliers (realistic profiling target)
  - customers: ~50 rows, referenced by sales.customer_id
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import duckdb

from omnidata_mcp.config import settings


def main() -> None:
    random.seed(42)

    db_path = settings.duckdb_path
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_path)

    conn.execute("DROP TABLE IF EXISTS sales")
    conn.execute("DROP TABLE IF EXISTS customers")

    conn.execute(
        """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name VARCHAR,
            region VARCHAR,
            signup_date DATE
        )
        """
    )

    regions = ["EMEA", "APAC", "AMER", "MEA"]
    start = date(2023, 1, 1)
    customers = [
        (i, f"Customer {i}", random.choice(regions), start + timedelta(days=random.randint(0, 700)))
        for i in range(1, 51)
    ]
    conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?)", customers)

    conn.execute(
        """
        CREATE TABLE sales (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date DATE,
            product_category VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            revenue DECIMAL(12,2),
            discount_pct DECIMAL(5,2)
        )
        """
    )

    categories = ["Electronics", "Office Supplies", "Furniture", "Software", "Services"]
    order_start = date(2024, 1, 1)
    sales_rows = []
    for order_id in range(1, 501):
        customer_id = random.randint(1, 50)
        order_date = order_start + timedelta(days=random.randint(0, 550))
        category = random.choice(categories)
        quantity = random.randint(1, 20)
        unit_price = round(random.uniform(5, 500), 2)
        discount_pct = round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2)
        revenue = round(quantity * unit_price * (1 - discount_pct / 100), 2)

        # Sprinkle in some NULLs and an outlier to make get_data_profile interesting.
        if random.random() < 0.03:
            discount_pct = None
        if order_id == 250:
            revenue = 999999.99  # deliberate outlier

        sales_rows.append(
            (order_id, customer_id, order_date, category, quantity, unit_price, revenue, discount_pct)
        )

    conn.executemany(
        "INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)", sales_rows
    )

    conn.close()
    print(f"Seeded {db_path} with 'customers' (50 rows) and 'sales' (500 rows).")


if __name__ == "__main__":
    main()
