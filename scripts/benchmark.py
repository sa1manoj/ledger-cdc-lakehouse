"""Time the daily-revenue question on the source Postgres vs. the dbt gold table.

This is a like-for-like *question* comparison, not an engine benchmark:
Postgres aggregates the normalised OLTP table, DuckDB reads the precomputed
gold table. Both run on the same machine, each query is executed once to warm
caches, then timed ``--runs`` times; the median is reported along with the
row counts so the result can be quoted with its conditions.

Usage:
    python scripts/benchmark.py --runs 10
"""
from __future__ import annotations

import argparse
import os
import platform
import statistics
import time

PG_QUERY = """
SELECT CAST(order_ts AS date) AS order_date,
       COUNT(*) AS orders,
       SUM(amount) FILTER (WHERE status <> 'cancelled') AS gross_revenue
FROM orders
GROUP BY 1
ORDER BY 1
"""

GOLD_QUERY = "SELECT order_date, orders, gross_revenue FROM fct_daily_revenue ORDER BY 1"


def _time(fn, runs: int) -> float:
    fn()  # warm-up
    samples = []
    for _ in range(runs):
        t = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t)
    return statistics.median(samples)


def main() -> None:
    import duckdb
    import psycopg

    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()

    dsn = os.getenv("POSTGRES_DSN", "postgresql://ledger:ledger@localhost:5432/ledger")
    duck_path = os.getenv("DUCKDB_PATH", "ledger.duckdb")

    with psycopg.connect(dsn) as pg:
        n_orders = pg.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        pg_s = _time(lambda: pg.execute(PG_QUERY).fetchall(), args.runs)

    duck = duckdb.connect(duck_path, read_only=True)
    gold_rows = duck.execute("SELECT COUNT(*) FROM fct_daily_revenue").fetchone()[0]
    gold_s = _time(lambda: duck.execute(GOLD_QUERY).fetchall(), args.runs)

    print(f"machine: {platform.platform()} / {os.cpu_count()} CPUs")
    print(f"source orders: {n_orders:,}  gold daily rows: {gold_rows:,}  runs: {args.runs} (median, warm)")
    print(f"postgres (aggregate OLTP table): {pg_s * 1000:,.1f} ms")
    print(f"duckdb   (gold table):           {gold_s * 1000:,.1f} ms")


if __name__ == "__main__":
    main()
