from datetime import timedelta
from decimal import Decimal

import pytest

from ledger.cdc import parse_value
from ledger.tables import ORDERS
from tests.helpers import T0, as_json, event, order_row


def test_insert_uses_after_image():
    rec = parse_value(as_json(event("c", 100, T0, after=order_row(amount="12.50"))), ORDERS, offset=7)
    assert rec["op"] == "c"
    assert rec["order_id"] == 1
    assert rec["amount"] == Decimal("12.50")
    assert rec["lsn"] == 100
    assert rec["offset"] == 7
    assert rec["source_ts"] == T0
    assert rec["updated_at"] == T0


def test_update_takes_new_values():
    before = order_row(status="placed")
    after = order_row(status="confirmed", updated_at=T0 + timedelta(hours=1))
    rec = parse_value(event("u", 101, T0 + timedelta(hours=1), before=before, after=after), ORDERS)
    assert rec["status"] == "confirmed"
    assert rec["updated_at"] == T0 + timedelta(hours=1)


def test_delete_uses_before_image():
    before = order_row(status="cancelled", amount="40.00")
    rec = parse_value(event("d", 102, T0 + timedelta(hours=2), before=before), ORDERS)
    assert rec["op"] == "d"
    assert rec["status"] == "cancelled"
    assert rec["amount"] == Decimal("40.00")
    assert rec["source_ts"] == T0 + timedelta(hours=2)


def test_snapshot_read_is_treated_like_insert():
    rec = parse_value(event("r", 50, T0, after=order_row()), ORDERS)
    assert rec["op"] == "r"
    assert rec["order_id"] == 1


@pytest.mark.parametrize("op", ["t", "m"])
def test_non_data_ops_are_skipped(op):
    assert parse_value(event(op, 1, T0), ORDERS) is None


def test_delete_without_before_image_fails_loudly():
    with pytest.raises(ValueError, match="REPLICA IDENTITY FULL"):
        parse_value(event("d", 1, T0, before=None), ORDERS)


def test_timestamps_are_utc_aware():
    rec = parse_value(event("c", 1, T0, after=order_row()), ORDERS)
    assert rec["order_ts"].utcoffset() == timedelta(0)
