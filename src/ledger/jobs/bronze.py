"""Bronze: land raw Debezium events from Kafka into an append-only Delta table.

Bronze keeps the untouched message value plus Kafka coordinates
(topic/partition/offset). The Delta sink plus the streaming checkpoint give
exactly-once writes: after a crash the job resumes from the last committed
offsets and Delta ignores the already-committed micro-batch.

Usage:
    python -m ledger.jobs.bronze            # run continuously (10s micro-batches)
    python -m ledger.jobs.bronze --once     # drain what is in Kafka now, then stop
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ledger.config import Settings
from ledger.jobs.spark_session import build_spark
from ledger.tables import TABLES


def kafka_source(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", ",".join(t.topic for t in TABLES.values()))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def to_bronze(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.current_timestamp().alias("ingested_at"),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="process available data then stop")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    spark = build_spark("ledger-bronze", settings, kafka=True)

    writer = (
        to_bronze(kafka_source(spark, settings))
        .writeStream.format("delta")
        .partitionBy("topic")
        .option("checkpointLocation", settings.checkpoint("bronze"))
        .outputMode("append")
    )
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime="10 seconds")
    query = writer.start(settings.bronze_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
