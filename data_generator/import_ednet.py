"""Prepare the Phase 1 learning-event sample from EdNet KT3 on Hugging Face."""

import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from event_files import LOCAL_DATA_DIR, write_events

REVISION = "2e0fb7f2f0dbcb8cd62a29bb30d9e89ec943a145"
SHARD = "kt3/train-00000-of-00012.parquet"
SHARD_URL = f"https://huggingface.co/datasets/mgor/EDNet/resolve/{REVISION}/{SHARD}"
SHARD_SHA256 = "70d96d6e238280d214ee576c5e82ff00690bc2c2119a6542ab7cd69cd24145c1"
SHARD_PATH = LOCAL_DATA_DIR / "ednet" / SHARD
WEEK_START = datetime(2019, 4, 8, tzinfo=timezone.utc)
WEEK_END = datetime(2019, 4, 15, tzinfo=timezone.utc)
SAMPLE_SIZE = 10_000
EVENT_TYPES = {
    "b:enter": "quiz_opened",
    "b:submit": "quiz_submitted",
    "q:respond": "quiz_answered",
    "l:enter": "video_started",
    "l:quit": "video_stopped",
    "e:enter": "explanation_opened",
    "e:quit": "explanation_closed",
}


def has_expected_checksum(path: Path) -> bool:
    if not path.is_file():
        return False
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest() == SHARD_SHA256


def download_shard() -> None:
    if has_expected_checksum(SHARD_PATH):
        return
    SHARD_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading EdNet KT3 shard from Hugging Face to {SHARD_PATH}", flush=True)
    with TemporaryDirectory(dir=SHARD_PATH.parent) as staging:
        staged_path = Path(staging) / SHARD_PATH.name
        with urlopen(SHARD_URL, timeout=60) as response, staged_path.open("wb") as output:
            shutil.copyfileobj(response, output)
        if not has_expected_checksum(staged_path):
            raise ValueError("The EdNet shard did not match the pinned SHA-256 checksum.")
        staged_path.replace(SHARD_PATH)


def phase1_events(raw: DataFrame) -> DataFrame:
    item_type = F.substring("item_id", 1, 1)
    action_key = F.concat_ws(":", item_type, F.col("action_type"))
    event_map = F.create_map(*[F.lit(value) for pair in EVENT_TYPES.items() for value in pair])
    event_type = F.element_at(event_map, action_key)
    event_id = F.sha2(F.to_json(F.struct(*raw.columns)), 256)
    week = raw.where(
        (F.col("timestamp") >= int(WEEK_START.timestamp() * 1000))
        & (F.col("timestamp") < int(WEEK_END.timestamp() * 1000))
        & F.col("subject_id").isNotNull()
    )
    scored_answer = (
        (event_type == "quiz_answered")
        & F.col("user_answer").isNotNull()
        & F.col("correct_answer").isNotNull()
    )
    return (
        week.where(event_type.isNotNull())
        .select(
            F.concat(F.lit("ednet_"), event_id).alias("event_id"),
            F.concat(F.lit("ednet_"), F.col("subject_id")).alias("student_id"),
            event_type.alias("event_type"),
            F.date_format(F.timestamp_millis("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("event_time"),
            F.lit(None).cast("string").alias("ingest_time"),
            F.when(item_type == "l", F.col("item_id")).alias("video_id"),
            F.lit(None).cast("int").alias("duration_sec"),
            F.when(scored_answer, F.col("is_correct").cast("double")).alias("quiz_score"),
            F.struct(
                F.lit("ednet_kt3").alias("dataset"),
                F.col("item_id"),
                F.col("action_type"),
                F.col("source").alias("study_mode"),
                F.col("platform"),
            ).alias("metadata"),
        )
        .dropDuplicates(["event_id"])
        .orderBy("event_id")
        .limit(SAMPLE_SIZE)
    )


def main() -> None:
    try:
        download_shard()
    except (OSError, ValueError) as exc:
        raise SystemExit(f"Could not download EdNet KT3: {exc}") from exc

    spark = (
        SparkSession.builder.appName("EduPulse - Import EdNet KT3")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    try:
        raw = spark.read.parquet(str(SHARD_PATH))
        rows = phase1_events(raw).collect()
        if len(rows) != SAMPLE_SIZE:
            raise SystemExit(f"Expected {SAMPLE_SIZE:,} EdNet events, found {len(rows):,}.")
        write_events((row.asDict(recursive=True) for row in rows), LOCAL_DATA_DIR)
        print(f"Prepared {len(rows):,} EdNet KT3 events in JSON and CSV at {LOCAL_DATA_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
