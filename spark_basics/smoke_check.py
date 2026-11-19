"""Verify that the local JVM, Spark, Pandas and generated files work together."""

import pandas as pd

from _local import EVENTS_CSV, EVENTS_JSON, create_spark


def main() -> None:
    if not EVENTS_JSON.is_file() or not EVENTS_CSV.is_file():
        raise SystemExit("Missing learning events. Run make generate-data first.")

    spark = create_spark("EduPulse Phase 1 - Smoke Check")
    try:
        spark.sparkContext.setLogLevel("WARN")
        json_count = spark.read.json(str(EVENTS_JSON)).count()
        csv_count = spark.read.option("header", True).csv(str(EVENTS_CSV)).count()
        pandas_count = len(pd.read_csv(EVENTS_CSV, usecols=["event_id"]))
        if json_count == 0 or json_count != csv_count or csv_count != pandas_count:
            raise SystemExit(
                f"Input counts must be positive and match: JSON={json_count}, "
                f"CSV={csv_count}, pandas={pandas_count}."
            )

        print(f"OK: Spark {spark.version} and pandas {pd.__version__} read {json_count:,} events.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
