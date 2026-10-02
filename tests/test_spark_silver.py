"""Spark silver job tests: parity with the pandas reference and MERGE behaviour.

Needs pyspark + delta-spark (installed in CI); skipped otherwise.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import SparkSession  # noqa: E402

from ledger.cdc import parse_value  # noqa: E402
from ledger.jobs import silver  # noqa: E402
from ledger.scd2 import build_history as pandas_history  # noqa: E402
from ledger.tables import ORDERS  # noqa: E402
from tests.helpers import as_json, lifecycle_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ledger-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
    )
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield s
    s.stop()


def bronze_df(spark, events):
    rows = [(ORDERS.topic, off, as_json(e)) for off, e in events]
    return spark.createDataFrame(rows, "topic string, offset long, value string")


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ("order_ts", "updated_at", "valid_from", "valid_to"):
        df[c] = pd.to_datetime(df[c], utc=True)
    df["amount"] = df["amount"].map(lambda v: f"{float(v):.2f}")
    df["is_current"] = df["is_current"].astype(bool)
    df["is_deleted"] = df["is_deleted"].astype(bool)
    return df.sort_values("version_id").reset_index(drop=True)


def test_spark_parse_matches_reference(spark):
    events = lifecycle_events()
    got = silver.parse_changes(bronze_df(spark, events), ORDERS).toPandas()
    want = pd.DataFrame([parse_value(e, ORDERS, offset=o) for o, e in events])
    assert got["lsn"].tolist() == want["lsn"].tolist()
    assert got["op"].tolist() == want["op"].tolist()
    assert got["status"].tolist() == want["status"].tolist()


def test_spark_history_matches_pandas_reference(spark):
    events = lifecycle_events()
    spark_hist = silver.build_history(silver.parse_changes(bronze_df(spark, events), ORDERS), ORDERS)
    ref = pandas_history(pd.DataFrame([parse_value(e, ORDERS, offset=o) for o, e in events]), ORDERS)
    cols = [
        "version_id", "order_id", "status", "amount", "valid_from", "valid_to", "is_current", "is_deleted",
    ]
    pd.testing.assert_frame_equal(
        _normalise(spark_hist.toPandas())[cols], _normalise(ref)[cols], check_dtype=False
    )


def test_merge_is_idempotent_and_removes_stale_versions(spark, tmp_path):
    path = str(tmp_path / "silver_orders")
    events = lifecycle_events()
    changes = silver.parse_changes(bronze_df(spark, events), ORDERS)
    touched = changes.select("order_id").distinct()
    history = silver.build_history(changes, ORDERS)

    silver.merge_history(spark, history, touched, path)
    first = spark.read.format("delta").load(path).toPandas()
    silver.merge_history(spark, history, touched, path)  # rerun of the same batch
    second = spark.read.format("delta").load(path).toPandas()
    assert len(first) == len(second) == 6
    assert sorted(first["version_id"]) == sorted(second["version_id"])

    # a version that is no longer produced for a touched key must disappear
    without_delete = silver.build_history(changes.where("op != 'd'"), ORDERS)
    silver.merge_history(spark, without_delete, touched, path)
    after = spark.read.format("delta").load(path).toPandas()
    assert not after["is_deleted"].any()
    assert (after.groupby("order_id")["is_current"].sum() == 1).all()
