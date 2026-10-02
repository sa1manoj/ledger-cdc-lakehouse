"""SCD Type 2 history built from a change log (pandas reference implementation).

Time model
----------
Versions are ordered by the Postgres LSN (commit order) and ``valid_from`` is the
source commit time. This is *system time*: "what did the source say at time t".
The row's own ``updated_at`` is kept as a normal column (business effective
time). Ordering by ``updated_at`` instead would be wrong: a backdated correction
committed today but stamped yesterday would sort *before* a status change made
this morning, and the stale amount carried by that older row image would win
as the current version.

Backdated corrections are therefore handled in two places:

* silver records the correction as a new version at the moment it was committed
  (an auditable "as known at" trail), and
* gold attributes amounts to the order's own date, so corrections restate the
  historical days they belong to.

Approach: rebuild-per-touched-key
---------------------------------
History for a key is always recomputed from *all* of that key's changes rather
than patching the latest version in place:

* **Hard deletes** become a final version with ``is_deleted = True``, so
  point-in-time queries before the delete still see the row.
* **Replays / reruns**: Debezium is at-least-once, so a connector restart can
  re-emit events. Identical events are collapsed (keeping the lowest Kafka
  offset), so processing a batch twice yields identical output.
* **Late-arriving events** (e.g. a bronze backfill) are sorted into place and
  neighbouring windows are recomputed.

Validity windows are half-open: ``valid_from <= t < valid_to``. The latest
version per key has ``valid_to = NULL`` and ``is_current = True``.

The Spark job (``ledger.jobs.silver.build_history``) implements the same logic
with window functions; the tests compare the two.
"""
from __future__ import annotations

import pandas as pd

from ledger.tables import HISTORY_COLS, TableSpec


def version_ids(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    """Deterministic id: <key...>-<lsn>-<seq>, seq = position among same-LSN changes."""
    seq = df.groupby(keys + ["lsn"], sort=False).cumcount() + 1
    parts = [df[k].astype("int64").astype(str) for k in keys]
    parts += [df["lsn"].astype("int64").astype(str), seq.astype(str)]
    out = parts[0]
    for p in parts[1:]:
        out = out + "-" + p
    return out


def dedupe(changes: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    """Collapse replayed events: identical content -> one row with the lowest offset."""
    content = spec.attr_cols + ["op", "lsn", "source_ts"]
    return changes.sort_values("offset", kind="mergesort").drop_duplicates(subset=content, keep="first")


def build_history(changes: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    """Return the full SCD2 history for every key present in ``changes``.

    ``changes`` needs the table's columns plus ``op``, ``lsn``, ``source_ts`` and ``offset``
    (the output of ``ledger.cdc.parse_value``).
    """
    keys = list(spec.keys)
    out_cols = spec.attr_cols + HISTORY_COLS
    if changes.empty:
        return pd.DataFrame(columns=out_cols)

    df = (
        dedupe(changes, spec)
        .sort_values(keys + ["lsn", "offset"], kind="mergesort")
        .reset_index(drop=True)
        .copy()
    )
    df["valid_from"] = df["source_ts"]
    df["valid_to"] = df.groupby(keys, sort=False)["source_ts"].shift(-1)
    df["is_current"] = df["valid_to"].isna()
    df["is_deleted"] = df["op"].eq("d")
    df["version_id"] = version_ids(df, keys)
    return df[out_cols]


def current_state(history: pd.DataFrame) -> pd.DataFrame:
    """Rows that exist in the source right now (latest version, not deleted)."""
    return history[history["is_current"] & ~history["is_deleted"]].reset_index(drop=True)


def as_of(history: pd.DataFrame, ts: pd.Timestamp) -> pd.DataFrame:
    """Point-in-time view: the version of each key that was valid at ``ts``."""
    valid = (history["valid_from"] <= ts) & (history["valid_to"].isna() | (history["valid_to"] > ts))
    return history[valid & ~history["is_deleted"]].reset_index(drop=True)
