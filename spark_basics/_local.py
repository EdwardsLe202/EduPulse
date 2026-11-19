"""Shared Spark setup and daily event counts for the Phase 1 exercises."""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "local"
EVENTS_JSON = DATA_DIR / "learning_events.json"
EVENTS_CSV = DATA_DIR / "learning_events.csv"


def create_spark(app_name: str) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def daily_event_counts(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("event_day", F.to_date(F.to_timestamp("event_time")))
        .groupBy("event_day")
        .agg(F.count("*").alias("event_count"))
        .orderBy("event_day")
    )
