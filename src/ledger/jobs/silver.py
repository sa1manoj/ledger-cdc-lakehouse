"""Silver: SCD Type 2 history per source table, maintained with an atomic Delta MERGE.

For every micro-batch of new bronze events:

1. parse the Debezium envelopes for this table and collect the touched keys
2. re-read *all* bronze changes for those keys (bronze is the full change log)
3. rebuild their history with window functions (same rules as ``ledger.scd2``)
4. MERGE into the silver table in one transaction:
   * versions that exist -> updated (``valid_to`` / ``is_current`` change when a
     newer version arrives)
   * new versions -> inserted
   * versions of touched keys that are no longer produced -> deleted

Because step 3 is a pure function of bronze, re-running a batch (e.g. after a
crash before the checkpoint was committed) produces the same rows and the
MERGE is a no-op. Readers never see a half-applied batch.

Usage:
    python -m ledger.jobs.silver                 # all tables, process new bronze data, stop
    python -m ledger.jobs.silver --tables orders
"""
from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ledger.config import Settings
from ledger.jobs.spark_session import build_spark
from ledger.tables import TABLES, TableSpec

_JSON_TYPES = {"long": T.LongType()}  # decimals and timestamps arrive as strings


def _row_struct(spec: TableSpec) -> T.StructType:
    return T.StructType(
        [T.StructField(c, _JSON_TYPES.get(t, T.StringType())) for c, t in spec.columns.items()]
    )


def envelope_schema(spec: TableSpec) -> T.StructType:
    row = _row_struct(spec)
    return T.StructType(
        [
            T.StructField("before", row),
            T.StructField("after", row),
            T.StructField("op", T.StringType()),
            T.StructField("ts_ms", T.LongType()),
            T.StructField(
                "source",
                T.StructType([T.StructField("lsn", T.LongType()), T.StructField("ts_ms", T.LongType())]),
            ),
        ]
    )


def _cast(col: Column, logical_type: str) -> Column:
    if logical_type == "long":
        return col.cast("long")
    if logical_type.startswith("decimal"):
        return col.cast(logical_type)
    if logical_type == "timestamp":
        return col.cast("timestamp")
    return col.cast("string")


def parse_changes(bronze: DataFrame, spec: TableSpec) -> DataFrame:
    """Bronze rows (topic, value) -> one flat change record per data event."""
    e = F.from_json(F.col("value"), envelope_schema(spec)).alias("e")
    parsed = bronze.where(F.col("topic") == spec.topic).select(e, F.col("offset")).where(
        F.col("e.op").isin("c", "u", "d", "r")
    )
    row = F.when(F.col("e.op") == "d", F.col("e.before")).otherwise(F.col("e.after"))
    source_ts = F.timestamp_millis(F.col("e.source.ts_ms"))
    cols = [_cast(row.getField(c), t).alias(c) for c, t in spec.columns.items()]
    return parsed.select(
        *cols,
        F.col("e.op").alias("op"),
        F.col("e.source.lsn").alias("lsn"),
        source_ts.alias("source_ts"),
        F.col("offset").cast("long").alias("offset"),
    )


def build_history(changes: DataFrame, spec: TableSpec) -> DataFrame:
    """Spark equivalent of ``ledger.scd2.build_history``."""
    keys = list(spec.keys)
    content = spec.attr_cols + ["op", "lsn", "source_ts"]
    # replayed events: identical content -> keep the lowest offset (deterministic)
    df = changes.groupBy(*content).agg(F.min("offset").alias("offset"))
    w = Window.partitionBy(*keys).orderBy("lsn", "offset")
    w_seq = Window.partitionBy(*keys, "lsn").orderBy("offset")
    version_id = F.concat_ws(
        "-",
        *[F.col(k).cast("string") for k in keys],
        F.col("lsn").cast("string"),
        F.row_number().over(w_seq).cast("string"),
    )
    return df.select(
        *spec.attr_cols,
        version_id.alias("version_id"),
        F.col("source_ts").alias("valid_from"),
        F.lead("source_ts").over(w).alias("valid_to"),
        F.lead("offset").over(w).isNull().alias("is_current"),
        (F.col("op") == "d").alias("is_deleted"),
        F.col("op"),
        F.col("lsn"),
    )


def merge_history(spark: SparkSession, history: DataFrame, touched: DataFrame, path: str) -> None:
    """Atomically replace the history of the touched keys in the silver table."""
    if not DeltaTable.isDeltaTable(spark, path):
        history.write.format("delta").mode("overwrite").save(path)
        return

    target = DeltaTable.forPath(spark, path)
    keys = touched.columns
    stale = (
        target.toDF()
        .join(touched, keys, "left_semi")
        .join(history.select("version_id"), "version_id", "left_anti")
        .select(*history.columns)
    )
    source = history.withColumn("_action", F.lit("upsert")).unionByName(
        stale.withColumn("_action", F.lit("delete"))
    )
    cols = {c: f"s.{c}" for c in history.columns}
    (
        target.alias("t")
        .merge(source.alias("s"), "t.version_id = s.version_id")
        .whenMatchedDelete(condition="s._action = 'delete'")
        .whenMatchedUpdate(condition="s._action = 'upsert'", set=cols)
        .whenNotMatchedInsert(condition="s._action = 'upsert'", values=cols)
        .execute()
    )


def process_table(spark: SparkSession, settings: Settings, spec: TableSpec, new_bronze: DataFrame) -> int:
    """Apply one batch of new bronze rows to silver. Returns the number of touched keys."""
    keys = list(spec.keys)
    touched = parse_changes(new_bronze, spec).select(*keys).distinct().cache()
    n = touched.count()
    if n == 0:
        touched.unpersist()
        return 0
    all_bronze = spark.read.format("delta").load(settings.bronze_path).where(F.col("topic") == spec.topic)
    changes = parse_changes(all_bronze, spec).join(touched, keys, "left_semi")
    merge_history(spark, build_history(changes, spec), touched, settings.silver_path(spec.name))
    touched.unpersist()
    return n


def run_table(spark: SparkSession, settings: Settings, spec: TableSpec) -> None:
    def _batch(batch_df: DataFrame, batch_id: int) -> None:
        n = process_table(spark, settings, spec, batch_df)
        print(f"[silver:{spec.name}] batch {batch_id}: rebuilt history for {n} keys")

    stream = (
        spark.readStream.format("delta")
        .load(settings.bronze_path)
        .where(F.col("topic") == spec.topic)
        .writeStream.foreachBatch(_batch)
        .option("checkpointLocation", settings.checkpoint(f"silver_{spec.name}"))
        .trigger(availableNow=True)
        .start()
    )
    stream.awaitTermination()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs="+", default=list(TABLES), choices=list(TABLES))
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    spark = build_spark("ledger-silver", settings)
    for name in args.tables:
        run_table(spark, settings, TABLES[name])


if __name__ == "__main__":
    main()
