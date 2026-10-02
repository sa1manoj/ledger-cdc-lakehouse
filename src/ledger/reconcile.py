"""Reconciliation gate: compare the source database with silver before gold is built.

What is compared
----------------
The *current state* of each table, on both sides:

* source: ``SELECT * FROM <table>`` in Postgres
* silver: latest version per key that is not a delete
  (``is_current AND NOT is_deleted``)

Raw row counts are NOT compared, because silver keeps every historical version
and delete marker. The checks are:

1. keys present in source but missing in silver
2. keys present in silver but no longer in source (missed deletes)
3. keys present on both sides whose compared columns differ
4. sum of the amount column on both sides (catches decimal/rounding drift)

Run it after the silver job has caught up (``make silver``) and while the
simulator is paused; changes committed after silver ran will show up as
differences by design.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd

from ledger.tables import TABLES, TableSpec


@dataclass
class ReconResult:
    table: str
    source_rows: int
    silver_current_rows: int
    missing_in_silver: list = field(default_factory=list)
    extra_in_silver: list = field(default_factory=list)
    mismatched: list = field(default_factory=list)
    source_amount_total: str | None = None
    silver_amount_total: str | None = None

    @property
    def passed(self) -> bool:
        return (
            not self.missing_in_silver
            and not self.extra_in_silver
            and not self.mismatched
            and self.source_amount_total == self.silver_amount_total
        )

    def to_dict(self) -> dict:
        d = asdict(self)
        d["passed"] = self.passed
        return d


def _norm(value):
    """Normalise values so Decimal/float/str and tz-aware timestamps compare equal."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (Decimal, float)):
        return str(Decimal(str(value)).quantize(Decimal("0.01")))
    if isinstance(value, (pd.Timestamp, datetime)):
        ts = pd.Timestamp(value)
        return (ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")).isoformat()
    return value


def _total(series: pd.Series) -> str:
    return str(sum((Decimal(str(v)) for v in series), Decimal("0")).quantize(Decimal("0.01")))


def reconcile(
    source: pd.DataFrame,
    silver_current: pd.DataFrame,
    spec: TableSpec,
    compare_cols: list[str] | None = None,
    amount_col: str | None = None,
    sample: int = 20,
) -> ReconResult:
    keys = list(spec.keys)
    compare_cols = compare_cols or [c for c in spec.attr_cols if c not in keys]

    src = source.set_index(keys)
    slv = silver_current.set_index(keys)

    missing = src.index.difference(slv.index)
    extra = slv.index.difference(src.index)
    both = src.index.intersection(slv.index)

    mismatched = []
    for key in both:
        a = src.loc[key, compare_cols]
        b = slv.loc[key, compare_cols]
        diffs = {c: (_norm(a[c]), _norm(b[c])) for c in compare_cols if _norm(a[c]) != _norm(b[c])}
        if diffs:
            mismatched.append({"key": key, "diffs": diffs})

    result = ReconResult(
        table=spec.name,
        source_rows=len(src),
        silver_current_rows=len(slv),
        missing_in_silver=[k for k in missing[:sample]] if len(missing) else [],
        extra_in_silver=[k for k in extra[:sample]] if len(extra) else [],
        mismatched=mismatched[:sample],
    )
    if amount_col:
        result.source_amount_total = _total(source[amount_col])
        result.silver_amount_total = _total(silver_current[amount_col])
    return result


# ----------------------------------------------------------------------------- CLI

def _read_source(table: str, dsn: str) -> pd.DataFrame:
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table}")  # table name comes from TABLES, not user input
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def _read_silver_current(path: str, storage_options: dict | None) -> pd.DataFrame:
    from deltalake import DeltaTable

    df = DeltaTable(path, storage_options=storage_options).to_pandas()
    return df[df["is_current"] & ~df["is_deleted"]].reset_index(drop=True)


def main(argv: list[str] | None = None) -> int:
    from ledger.config import Settings

    settings = Settings.from_env()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tables", nargs="+", default=list(TABLES))
    args = parser.parse_args(argv)

    report = {"run_at": datetime.now(timezone.utc).isoformat(), "results": []}
    ok = True
    for name in args.tables:
        spec = TABLES[name]
        result = reconcile(
            _read_source(name, settings.postgres_dsn),
            _read_silver_current(settings.silver_path(name), settings.delta_storage_options()),
            spec,
            amount_col="amount" if "amount" in spec.columns else None,
        )
        ok &= result.passed
        report["results"].append(result.to_dict())
        status = "PASS" if result.passed else "FAIL"
        print(
            f"[{status}] {name}: source={result.source_rows} silver_current={result.silver_current_rows} "
            f"missing={len(result.missing_in_silver)} extra={len(result.extra_in_silver)} "
            f"mismatched={len(result.mismatched)} "
            f"amount source={result.source_amount_total} silver={result.silver_amount_total}"
        )

    if settings.is_local:
        out_dir = Path(settings.lake_root) / "_reconciliation"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"recon_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
        out_file.write_text(json.dumps(report, indent=2, default=str))
        print(f"report: {out_file}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
