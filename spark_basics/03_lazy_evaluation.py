"""Exercise 3: Follow lazy evaluation and shuffle in the Spark UI."""

from _local import create_spark


def main() -> None:
    spark = create_spark("EduPulse 03 - Lazy Evaluation")
    try:
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        # TODO: Start with spark.range(100, numPartitions=4) and add a filter.
        # TODO: Inspect the plan with explain() and compare Jobs before and after an action.
        # TODO: Compare with a groupBy that requires a shuffle and inspect its stages.
        input("Press Enter to stop Spark after inspecting the UI. ")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
