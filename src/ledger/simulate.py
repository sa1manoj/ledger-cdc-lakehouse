"""Generate realistic OLTP activity in the source Postgres database.

All data is synthetic. Each tick performs a mix of:

* new orders                            (insert)
* status changes placed -> confirmed -> delivered / cancelled   (update)
* customer profile changes (city, segment)                      (update -> SCD2 dim)
* hard deletes of some cancelled orders                         (delete)
* backdated corrections: an amount fix whose ``updated_at`` is set in the past,
  as when finance corrects an order after the fact. Gold restates revenue for
  the order's original date.                                   (update)

Usage:
    python -m ledger.simulate --customers 500 --ticks 60 --orders-per-tick 200 --sleep 1
"""
from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

CITIES = ["Bengaluru", "Hyderabad", "Mumbai", "Delhi", "Chennai", "Pune", "Kolkata"]
SEGMENTS = ["new", "regular", "gold"]
NEXT_STATUS = {"placed": ["confirmed", "cancelled"], "confirmed": ["delivered", "cancelled"]}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def seed_customers(cur, n: int, rng: random.Random) -> None:
    cur.execute("SELECT COALESCE(MAX(customer_id), 0) FROM customers")
    start = cur.fetchone()[0] + 1
    rows = [
        (cid, f"Customer {cid}", f"customer{cid}@example.com", rng.choice(CITIES), rng.choice(SEGMENTS))
        for cid in range(start, start + n)
    ]
    cur.executemany(
        "INSERT INTO customers (customer_id, name, email, city, segment) VALUES (%s,%s,%s,%s,%s)", rows
    )


def tick(cur, rng: random.Random, orders_per_tick: int, stats: dict) -> None:
    cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
    next_id = cur.fetchone()[0] + 1
    cur.execute("SELECT customer_id FROM customers")
    customers = [r[0] for r in cur.fetchall()]

    # 1. new orders
    new_rows = []
    for oid in range(next_id, next_id + orders_per_tick):
        amount = Decimal(rng.randint(9900, 250000)) / 100
        new_rows.append((oid, rng.choice(customers), "placed", amount, _now()))
    cur.executemany(
        "INSERT INTO orders (order_id, customer_id, status, amount, order_ts) VALUES (%s,%s,%s,%s,%s)",
        new_rows,
    )
    stats["inserts"] += len(new_rows)

    # 2. status transitions for open orders
    cur.execute(
        "SELECT order_id, status FROM orders WHERE status IN ('placed','confirmed') "
        "ORDER BY random() LIMIT %s",
        (orders_per_tick,),
    )
    for oid, status in cur.fetchall():
        new_status = rng.choices(NEXT_STATUS[status], weights=[0.9, 0.1])[0]
        cur.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s", (new_status, oid))
        stats["updates"] += 1

    # 3. customer profile changes
    for cid in rng.sample(customers, k=max(1, len(customers) // 100)):
        cur.execute(
            "UPDATE customers SET city=%s, segment=%s, updated_at=now() WHERE customer_id=%s",
            (rng.choice(CITIES), rng.choice(SEGMENTS), cid),
        )
        stats["customer_updates"] += 1

    # 4. hard deletes of a few cancelled orders
    cur.execute("SELECT order_id FROM orders WHERE status='cancelled' ORDER BY random() LIMIT 2")
    for (oid,) in cur.fetchall():
        cur.execute("DELETE FROM orders WHERE order_id=%s", (oid,))
        stats["deletes"] += 1

    # 5. backdated amount corrections
    cur.execute("SELECT order_id, amount, order_ts FROM orders ORDER BY random() LIMIT 2")
    for oid, amount, order_ts in cur.fetchall():
        corrected = (amount * Decimal("0.95")).quantize(Decimal("0.01"))
        effective = max(order_ts, _now() - timedelta(hours=rng.randint(1, 48)))
        cur.execute(
            "UPDATE orders SET amount=%s, updated_at=%s WHERE order_id=%s", (corrected, effective, oid)
        )
        stats["backdated"] += 1


def main(argv: list[str] | None = None) -> None:
    import psycopg

    from ledger.config import Settings

    parser = argparse.ArgumentParser(description="Synthetic OLTP workload for Ledger")
    parser.add_argument("--customers", type=int, default=200)
    parser.add_argument("--orders-per-tick", type=int, default=50)
    parser.add_argument("--ticks", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    stats = {"inserts": 0, "updates": 0, "customer_updates": 0, "deletes": 0, "backdated": 0}
    with psycopg.connect(Settings.from_env().postgres_dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers")
            if cur.fetchone()[0] == 0:
                seed_customers(cur, args.customers, rng)
                conn.commit()
        for i in range(args.ticks):
            with conn.cursor() as cur:
                tick(cur, rng, args.orders_per_tick, stats)
            conn.commit()
            print(f"tick {i + 1}/{args.ticks}: {stats}")
            time.sleep(args.sleep)


if __name__ == "__main__":
    main()
