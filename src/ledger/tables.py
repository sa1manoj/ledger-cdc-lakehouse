"""Table specifications shared by the parser, the Spark jobs, and the tests.

Each source table replicated by Debezium is described once here so the
bronze -> silver logic stays generic.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableSpec:
    name: str
    topic: str
    keys: tuple[str, ...]
    # column name -> logical type: "long" | "string" | "decimal(p,s)" | "timestamp"
    columns: dict[str, str] = field(default_factory=dict)

    @property
    def attr_cols(self) -> list[str]:
        return list(self.columns)


ORDERS = TableSpec(
    name="orders",
    topic="ledger.public.orders",
    keys=("order_id",),
    columns={
        "order_id": "long",
        "customer_id": "long",
        "status": "string",
        "amount": "decimal(12,2)",
        "order_ts": "timestamp",
        "updated_at": "timestamp",
    },
)

CUSTOMERS = TableSpec(
    name="customers",
    topic="ledger.public.customers",
    keys=("customer_id",),
    columns={
        "customer_id": "long",
        "name": "string",
        "email": "string",
        "city": "string",
        "segment": "string",
        "updated_at": "timestamp",
    },
)

TABLES: dict[str, TableSpec] = {t.name: t for t in (ORDERS, CUSTOMERS)}

# Metadata columns added to every silver history table.
HISTORY_COLS = ["version_id", "valid_from", "valid_to", "is_current", "is_deleted", "op", "lsn"]
