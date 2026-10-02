"""Parsing of Debezium change events (JSON converter, schemas disabled).

This is the pure-Python reference implementation. The Spark silver job uses
the equivalent column expressions in ``ledger.jobs.silver.parse_changes``;
``tests/test_spark_silver.py`` checks that both produce the same output.

Rules
-----
* ``c`` (insert), ``u`` (update) and ``r`` (snapshot read) carry the new row in
  ``after``.
* ``d`` (delete) carries the old row in ``before`` (requires
  ``REPLICA IDENTITY FULL``), so the delete marker keeps the last known values.
* Other ops (``t`` truncate, ``m`` message) are ignored.
* ``source.lsn`` is the Postgres log sequence number and ``source.ts_ms`` the
  commit time (used as ``valid_from``). Changes are ordered by ``(lsn, offset)``:
  the Kafka offset breaks ties, because events of one transaction can report the
  same LSN. Debezium keys messages by primary key, so all changes for a row land
  in one partition, where offsets are strictly increasing.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ledger.tables import TableSpec

DATA_OPS = {"c", "u", "d", "r"}


def _coerce(value: Any, logical_type: str) -> Any:
    if value is None:
        return None
    if logical_type == "long":
        return int(value)
    if logical_type == "string":
        return str(value)
    if logical_type.startswith("decimal"):
        return Decimal(str(value))
    if logical_type == "timestamp":
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    raise ValueError(f"unknown logical type {logical_type!r}")


def parse_value(value: str | dict, spec: TableSpec, offset: int = 0) -> dict | None:
    """Turn one Debezium message value into a flat change record, or None to skip."""
    event = json.loads(value) if isinstance(value, str) else value
    op = event.get("op")
    if op not in DATA_OPS:
        return None

    row = event.get("before") if op == "d" else event.get("after")
    if row is None:
        raise ValueError(
            f"{spec.name}: op={op!r} event has no row image; "
            "deletes need REPLICA IDENTITY FULL on the source table"
        )

    record = {col: _coerce(row.get(col), typ) for col, typ in spec.columns.items()}
    source = event["source"]
    source_ts = datetime.fromtimestamp(int(source["ts_ms"]) / 1000, tz=timezone.utc)

    record["op"] = op
    record["lsn"] = int(source["lsn"])
    record["source_ts"] = source_ts
    record["offset"] = int(offset)
    return record
