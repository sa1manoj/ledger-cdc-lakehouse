"""Builders for Debezium-style events used across tests."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

T0 = datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc)


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def order_row(order_id=1, customer_id=10, status="placed", amount="100.00", order_ts=T0, updated_at=T0):
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": status,
        "amount": amount,
        "order_ts": iso(order_ts),
        "updated_at": iso(updated_at),
    }


def event(op, lsn, commit_ts, before=None, after=None) -> dict:
    return {
        "before": before,
        "after": after,
        "op": op,
        "ts_ms": int(commit_ts.timestamp() * 1000) + 5,
        "source": {"lsn": lsn, "ts_ms": int(commit_ts.timestamp() * 1000)},
    }


def as_json(e: dict) -> str:
    return json.dumps(e)


def lifecycle_events() -> list[tuple[int, dict]]:
    """(kafka_offset, event) for one order: insert, two updates, backdated fix, delete.

    Plus a second order that is only inserted, and a replayed duplicate.
    """
    t = T0
    o1_placed = order_row(1, status="placed", amount="100.00", updated_at=t)
    o1_conf = order_row(1, status="confirmed", amount="100.00", updated_at=t + timedelta(hours=1))
    o1_deliv = order_row(1, status="delivered", amount="100.00", updated_at=t + timedelta(hours=3))
    # finance correction committed at t+5h but stamped as effective t+30min
    o1_fixed = order_row(1, status="delivered", amount="95.00", updated_at=t + timedelta(minutes=30))
    o2 = order_row(2, customer_id=11, status="placed", amount="250.50", updated_at=t + timedelta(hours=2))

    events = [
        (0, event("c", 1000, t, after=o1_placed)),
        (1, event("u", 1010, t + timedelta(hours=1), before=o1_placed, after=o1_conf)),
        (2, event("c", 1015, t + timedelta(hours=2), after=o2)),
        (3, event("u", 1020, t + timedelta(hours=3), before=o1_conf, after=o1_deliv)),
        (4, event("u", 1030, t + timedelta(hours=5), before=o1_deliv, after=o1_fixed)),
        # connector restart re-emits the delivered update (at-least-once delivery)
        (5, event("u", 1020, t + timedelta(hours=3), before=o1_conf, after=o1_deliv)),
        (6, event("d", 1040, t + timedelta(hours=6), before=o1_fixed, after=None)),
    ]
    return events
