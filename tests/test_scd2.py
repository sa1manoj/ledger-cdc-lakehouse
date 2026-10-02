from datetime import timedelta
from decimal import Decimal

import pandas as pd

from ledger.cdc import parse_value
from ledger.scd2 import as_of, build_history, current_state
from ledger.tables import ORDERS
from tests.helpers import T0, event, lifecycle_events, order_row


def changes_df(events) -> pd.DataFrame:
    rows = [parse_value(e, ORDERS, offset=off) for off, e in events]
    return pd.DataFrame([r for r in rows if r is not None])


def history_for(order_id: int, events=None) -> pd.DataFrame:
    h = build_history(changes_df(events or lifecycle_events()), ORDERS)
    return h[h["order_id"] == order_id].reset_index(drop=True)


def test_versions_follow_commit_order():
    h = history_for(1)
    assert list(h["op"]) == ["c", "u", "u", "u", "d"]
    assert list(h["status"]) == ["placed", "confirmed", "delivered", "delivered", "delivered"]


def test_replayed_event_is_collapsed():
    h = history_for(1)
    assert h["lsn"].tolist() == [1000, 1010, 1020, 1030, 1040]


def test_validity_windows_are_contiguous_and_half_open():
    h = history_for(1)
    assert h["valid_to"].iloc[:-1].tolist() == h["valid_from"].iloc[1:].tolist()
    assert pd.isna(h["valid_to"].iloc[-1])
    assert h["is_current"].tolist() == [False, False, False, False, True]


def test_backdated_correction_becomes_current_not_overtaken():
    """The fix is stamped earlier (updated_at) but committed later, so it must win."""
    events = [e for e in lifecycle_events() if e[1]["op"] != "d"]
    h = history_for(1, events)
    current = h[h["is_current"]].iloc[0]
    assert current["amount"] == Decimal("95.00")
    # business effective time is preserved as a column for gold to use
    assert current["updated_at"] == T0 + timedelta(minutes=30)


def test_delete_produces_final_marker_and_keeps_history():
    h = history_for(1)
    last = h.iloc[-1]
    assert last["is_deleted"] and last["is_current"]
    assert last["amount"] == Decimal("95.00")  # last known values kept on the marker
    assert current_state(build_history(changes_df(lifecycle_events()), ORDERS))["order_id"].tolist() == [2]


def test_point_in_time_view_before_and_after_delete():
    h = build_history(changes_df(lifecycle_events()), ORDERS)
    before_delete = as_of(h, pd.Timestamp(T0 + timedelta(hours=5, minutes=30)))
    assert set(before_delete["order_id"]) == {1, 2}
    assert before_delete.set_index("order_id").loc[1, "amount"] == Decimal("95.00")
    at_hour_two = as_of(h, pd.Timestamp(T0 + timedelta(hours=2)))
    assert at_hour_two.set_index("order_id").loc[1, "status"] == "confirmed"
    after_delete = as_of(h, pd.Timestamp(T0 + timedelta(hours=7)))
    assert after_delete["order_id"].tolist() == [2]


def test_exactly_one_current_version_per_key():
    h = build_history(changes_df(lifecycle_events()), ORDERS)
    assert (h.groupby("order_id")["is_current"].sum() == 1).all()


def test_rebuild_is_idempotent():
    events = lifecycle_events()
    once = build_history(changes_df(events), ORDERS)
    twice = build_history(changes_df(events + events), ORDERS)
    pd.testing.assert_frame_equal(once, twice)


def test_late_arriving_event_is_slotted_in():
    events = lifecycle_events()
    shuffled = [events[i] for i in (0, 3, 1, 2, 6, 4)]  # bronze order != commit order
    h = history_for(1, shuffled)
    assert h["lsn"].tolist() == [1000, 1010, 1020, 1030, 1040]


def test_same_lsn_changes_get_distinct_versions():
    a = order_row(status="placed")
    b = order_row(status="confirmed")
    events = [(0, event("c", 500, T0, after=a)), (1, event("u", 500, T0, before=a, after=b))]
    h = history_for(1, events)
    assert h["version_id"].tolist() == ["1-500-1", "1-500-2"]
    assert h["status"].tolist() == ["placed", "confirmed"]


def test_delete_then_reinsert_starts_a_new_live_version():
    a = order_row(status="placed")
    events = [
        (0, event("c", 1, T0, after=a)),
        (1, event("d", 2, T0 + timedelta(hours=1), before=a)),
        (2, event("c", 3, T0 + timedelta(hours=2), after=order_row(status="placed", amount="5.00"))),
    ]
    h = history_for(1, events)
    assert h["is_deleted"].tolist() == [False, True, False]
    assert current_state(h)["amount"].tolist() == [Decimal("5.00")]


def test_empty_input():
    assert build_history(pd.DataFrame(), ORDERS).empty
