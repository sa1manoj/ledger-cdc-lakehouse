"""The synthetic stream exercises every case the pipeline claims to handle."""
from ledger.fixtures import generate_events, histories
from ledger.scd2 import current_state


def _hist():
    return histories(generate_events(customers=30, orders=300, seed=7))


def test_stream_contains_every_change_type():
    orders = _hist()["orders"]
    assert set(orders["op"]) == {"c", "u", "d"}
    assert orders["is_deleted"].any()


def test_histories_satisfy_scd2_invariants():
    for name, h in _hist().items():
        key = f"{name[:-1]}_id"
        assert (h.groupby(key)["is_current"].sum() == 1).all(), name
        assert h["version_id"].is_unique, name
        closed = h.dropna(subset=["valid_to"])
        assert (closed["valid_to"] >= closed["valid_from"]).all(), name


def test_deleted_orders_are_not_in_current_state():
    orders = _hist()["orders"]
    deleted_ids = set(orders.loc[orders["is_current"] & orders["is_deleted"], "order_id"])
    assert deleted_ids
    assert deleted_ids.isdisjoint(set(current_state(orders)["order_id"]))


def test_generation_is_deterministic():
    assert generate_events(10, 50, seed=1) == generate_events(10, 50, seed=1)
