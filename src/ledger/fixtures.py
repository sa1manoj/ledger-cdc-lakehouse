"""Deterministic synthetic Debezium event streams, for CI and local dbt runs without Docker.

``generate_events`` produces the same kinds of changes as ``ledger.simulate``
(inserts, status updates, customer moves, hard deletes, backdated amount
corrections, and an occasional replayed event) as Debezium JSON messages.
``write_fixture_lake`` turns them into silver Delta tables with the pandas
reference implementation, so the dbt project can be built and tested anywhere.

Usage:
    python -m ledger.fixtures --out ./lake_fixture --customers 50 --orders 400
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd

from ledger.cdc import parse_value
from ledger.scd2 import build_history
from ledger.tables import CUSTOMERS, ORDERS, TABLES, TableSpec

START = datetime(2025, 1, 1, tzinfo=timezone.utc)
CITIES = ["Bengaluru", "Hyderabad", "Mumbai", "Delhi", "Chennai", "Pune"]
SEGMENTS = ["new", "regular", "gold"]


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class _Stream:
    def __init__(self) -> None:
        self.lsn = 10_000
        self.offsets: dict[str, int] = {}
        self.messages: list[tuple[str, int, str]] = []

    def emit(self, spec: TableSpec, op: str, commit_ts: datetime, before=None, after=None, lsn=None) -> dict:
        if lsn is None:
            self.lsn += 8
            lsn = self.lsn
        value = {
            "before": before,
            "after": after,
            "op": op,
            "ts_ms": int(commit_ts.timestamp() * 1000) + 3,
            "source": {"lsn": lsn, "ts_ms": int(commit_ts.timestamp() * 1000)},
        }
        off = self.offsets.get(spec.topic, 0)
        self.offsets[spec.topic] = off + 1
        self.messages.append((spec.topic, off, json.dumps(value)))
        return value


def generate_events(customers: int = 50, orders: int = 400, seed: int = 7) -> list[tuple[str, int, str]]:
    rng = random.Random(seed)
    s = _Stream()
    clock = START

    cust_rows: dict[int, dict] = {}
    for cid in range(1, customers + 1):
        clock += timedelta(seconds=rng.randint(1, 30))
        row = {
            "customer_id": cid,
            "name": f"Customer {cid}",
            "email": f"customer{cid}@example.com",
            "city": rng.choice(CITIES),
            "segment": rng.choice(SEGMENTS),
            "updated_at": _iso(clock),
        }
        cust_rows[cid] = row
        s.emit(CUSTOMERS, "c", clock, after=row)

    order_rows: dict[int, dict] = {}
    for oid in range(1, orders + 1):
        clock += timedelta(minutes=rng.randint(1, 20))
        row = {
            "order_id": oid,
            "customer_id": rng.randint(1, customers),
            "status": "placed",
            "amount": str(Decimal(rng.randint(9900, 250000)) / 100),
            "order_ts": _iso(clock),
            "updated_at": _iso(clock),
        }
        order_rows[oid] = row
        s.emit(ORDERS, "c", clock, after=row)

        # progress a random earlier order
        if oid > 5:
            target = rng.randint(max(1, oid - 30), oid - 1)
            cur = order_rows.get(target)
            if cur and cur["status"] in ("placed", "confirmed"):
                nxt = {"placed": "confirmed", "confirmed": "delivered"}[cur["status"]]
                if rng.random() < 0.08:
                    nxt = "cancelled"
                clock += timedelta(seconds=rng.randint(5, 120))
                new = {**cur, "status": nxt, "updated_at": _iso(clock)}
                value = s.emit(ORDERS, "u", clock, before=cur, after=new)
                order_rows[target] = new
                if rng.random() < 0.03:  # at-least-once replay of the same event
                    s.emit(ORDERS, "u", clock, before=cur, after=new, lsn=value["source"]["lsn"])

        # customer moves city / changes segment
        if rng.random() < 0.05:
            cid = rng.randint(1, customers)
            clock += timedelta(seconds=rng.randint(5, 60))
            new = {**cust_rows[cid], "city": rng.choice(CITIES), "segment": rng.choice(SEGMENTS),
                   "updated_at": _iso(clock)}
            s.emit(CUSTOMERS, "u", clock, before=cust_rows[cid], after=new)
            cust_rows[cid] = new

        # hard delete of a cancelled order
        if rng.random() < 0.02:
            cancelled = [k for k, v in order_rows.items() if v["status"] == "cancelled"]
            if cancelled:
                victim = rng.choice(cancelled)
                clock += timedelta(seconds=rng.randint(5, 60))
                s.emit(ORDERS, "d", clock, before=order_rows.pop(victim))

        # backdated amount correction (effective earlier than its commit time)
        if rng.random() < 0.02 and order_rows:
            target = rng.choice(list(order_rows))
            cur = order_rows[target]
            clock += timedelta(seconds=rng.randint(5, 60))
            fixed = (Decimal(cur["amount"]) * Decimal("0.95")).quantize(Decimal("0.01"))
            new = {**cur, "amount": str(fixed), "updated_at": cur["order_ts"]}
            s.emit(ORDERS, "u", clock, before=cur, after=new)
            order_rows[target] = new

    return s.messages


def histories(messages: list[tuple[str, int, str]]) -> dict[str, pd.DataFrame]:
    out = {}
    for name, spec in TABLES.items():
        recs = [parse_value(v, spec, offset=o) for topic, o, v in messages if topic == spec.topic]
        out[name] = build_history(pd.DataFrame([r for r in recs if r]), spec)
    return out


def _arrow_table(df: pd.DataFrame, spec: TableSpec):
    import pyarrow as pa

    ts = pa.timestamp("us", tz="UTC")
    fields, arrays = [], []
    types = {**spec.columns, "version_id": "string", "valid_from": "timestamp", "valid_to": "timestamp",
             "is_current": "bool", "is_deleted": "bool", "op": "string", "lsn": "long"}
    for col in df.columns:
        t = types[col]
        values = df[col].tolist()
        if t == "long":
            typ = pa.int64()
        elif t == "string":
            typ = pa.string()
        elif t == "bool":
            typ = pa.bool_()
        elif t == "timestamp":
            typ = ts
            values = [None if pd.isna(v) else pd.Timestamp(v).to_pydatetime() for v in values]
        else:  # decimal(p,s)
            p, sc = (int(x) for x in t[t.index("(") + 1 : -1].split(","))
            typ = pa.decimal128(p, sc)
        fields.append(pa.field(col, typ))
        arrays.append(pa.array(values, type=typ))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write_fixture_lake(out: str, customers: int, orders: int, seed: int) -> dict[str, int]:
    from deltalake import write_deltalake

    counts = {}
    for name, df in histories(generate_events(customers, orders, seed)).items():
        write_deltalake(f"{out}/silver/{name}", _arrow_table(df, TABLES[name]), mode="overwrite")
        counts[name] = len(df)
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="./lake_fixture")
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--orders", type=int, default=400)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)
    counts = write_fixture_lake(args.out, args.customers, args.orders, args.seed)
    print(f"wrote silver fixture to {args.out}: {counts}")


if __name__ == "__main__":
    main()
