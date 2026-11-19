"""Exercise 3: Follow lazy evaluation and shuffle in the Spark UI."""

from pyspark.sql import functions as F

from _local import create_spark


def main() -> None:
    spark = create_spark("EduPulse 03 - Lazy Evaluation")
    try:
        sc = spark.sparkContext
        print(f"Spark UI: {sc.uiWebUrl}")

        print("\n1. Build a plan from four input partitions")
        numbers = spark.range(100, numPartitions=4)
        evens = numbers.filter(F.col("id") % 2 == 0)
        evens.explain(mode="extended")
        print(f"Jobs submitted so far: {sc.statusTracker().getJobIdsForGroup(None)}")
        input("Open the Jobs tab, then press Enter to run the count. ")

        print("\n2. Execute the filter and a global count")
        sc.setJobGroup("filter-count", "Count even IDs")
        print(f"Even IDs: {evens.count()}")

        print("\n3. Plan a grouped count that redistributes data by key")
        grouped = numbers.withColumn("bucket", F.col("id") % 4).groupBy("bucket").count()
        grouped.explain(mode="formatted")
        input(
            "Check Jobs for the even-ID count and look for Exchange above. "
            "Press Enter to run the grouped count. "
        )

        sc.setJobGroup("group-count", "Count IDs by bucket")
        # The grouped result has only four rows. Sort those rows on the driver.
        buckets = sorted(grouped.collect(), key=lambda row: row.bucket)
        for row in buckets:
            print(f"Bucket {row.bucket}: {row['count']} IDs")

        input("Inspect the grouped count in SQL and Stages, then press Enter to stop Spark. ")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
