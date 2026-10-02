"""SparkSession factory with Delta Lake (and optionally Kafka / ADLS Gen2) configured."""
from __future__ import annotations

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from ledger.config import Settings

SCALA = "2.13"  # Spark 4.x is built for Scala 2.13
HADOOP_AZURE = "org.apache.hadoop:hadoop-azure:3.4.1"


def build_spark(app_name: str, settings: Settings | None = None, kafka: bool = False) -> SparkSession:
    settings = settings or Settings.from_env()
    extra = []
    if kafka:
        extra.append(f"org.apache.spark:spark-sql-kafka-0-10_{SCALA}:{pyspark.__version__}")
    if not settings.is_local:
        extra.append(HADOOP_AZURE)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if not settings.is_local and settings.azure_account:
        builder = builder.config(
            f"spark.hadoop.fs.azure.account.key.{settings.azure_account}.dfs.core.windows.net",
            settings.azure_key or "",
        )

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
