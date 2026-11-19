"""Exercise 2: Compare Spark SQL, Spark DataFrame and Pandas."""

from _local import EVENTS_CSV, EVENTS_JSON, create_spark


def main() -> None:
    spark = create_spark("EduPulse 02 - Spark SQL")
    try:
        print(f"Input files: {EVENTS_JSON} and {EVENTS_CSV}")
        # TODO: Register the learning events as a temporary SQL view.
        # TODO: Reproduce the daily DataFrame counts in SQL and Pandas.
        # TODO: Compare the three results using the same UTC day and sort order.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
