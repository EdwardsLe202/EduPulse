"""Exercise 1: Read learning events and explore the DataFrame API."""

from _local import EVENTS_CSV, EVENTS_JSON, create_spark


def main() -> None:
    spark = create_spark("EduPulse 01 - DataFrame")
    try:
        print(f"Input files: {EVENTS_JSON} and {EVENTS_CSV}")
        # TODO: Read both formats and compare their inferred schemas.
        # TODO: Select useful columns, filter an event type and derive a UTC day.
        # TODO: Count events by day across all event types.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
