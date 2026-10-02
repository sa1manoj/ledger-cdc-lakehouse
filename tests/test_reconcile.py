from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

from ledger.reconcile import reconcile
from ledger.tables import ORDERS

TS = datetime(2025, 1, 1, tzinfo=timezone.utc)


def frame(rows):
    return pd.DataFrame(
        rows, columns=["order_id", "customer_id", "status", "amount", "order_ts", "updated_at"]
    )


SOURCE = frame(
    [
        (1, 10, "delivered", Decimal("95.00"), TS, TS),
        (2, 11, "placed", Decimal("250.50"), TS, TS),
    ]
)


def test_matching_states_pass():
    # silver stores amounts as decimals read back from Delta; floats must also compare equal
    silver = SOURCE.copy()
    silver["amount"] = silver["amount"].astype(float)
    r = reconcile(SOURCE, silver, ORDERS, amount_col="amount")
    assert r.passed
    assert r.source_amount_total == r.silver_amount_total == "345.50"


def test_missed_delete_is_reported_as_extra():
    silver = pd.concat([SOURCE, frame([(3, 12, "cancelled", Decimal("10.00"), TS, TS)])])
    r = reconcile(SOURCE, silver, ORDERS, amount_col="amount")
    assert not r.passed
    assert r.extra_in_silver == [3]


def test_missing_key_is_reported():
    r = reconcile(SOURCE, SOURCE.iloc[:1], ORDERS, amount_col="amount")
    assert r.missing_in_silver == [2]
    assert not r.passed


def test_value_drift_is_reported_per_column():
    silver = SOURCE.copy()
    silver.loc[silver["order_id"] == 1, "status"] = "confirmed"
    r = reconcile(SOURCE, silver, ORDERS, amount_col="amount")
    assert r.mismatched == [{"key": 1, "diffs": {"status": ("delivered", "confirmed")}}]


def test_timezone_representation_does_not_cause_false_mismatch():
    silver = SOURCE.copy()
    silver["order_ts"] = pd.to_datetime(silver["order_ts"]).dt.tz_convert("Asia/Kolkata")
    assert reconcile(SOURCE, silver, ORDERS).passed
