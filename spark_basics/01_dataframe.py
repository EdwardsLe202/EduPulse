"""Exercise 1: Read learning events and explore the DataFrame API."""

from pyspark.sql import functions as F

from _local import EVENTS_CSV, EVENTS_JSON, create_spark, daily_event_counts


def main() -> None:
    spark = create_spark("EduPulse 01 - DataFrame")
    try:
        json_events = spark.read.json(str(EVENTS_JSON))
        csv_events = spark.read.csv(str(EVENTS_CSV), header=True, inferSchema=True)

        print("Inferred JSON schema")
        json_events.printSchema()
        print("Inferred CSV schema")
        csv_events.printSchema()

        print("Sample quiz answers with their UTC day")
        quiz_answers = (
            json_events.filter(F.col("event_type") == "quiz_answered")
            .withColumn("event_day", F.to_date(F.to_timestamp("event_time")))
            .select("student_id", "event_time", "event_day", "quiz_score")
        )
        quiz_answers.show(5, truncate=False)

        print("Daily counts across all event types")
        daily_event_counts(json_events).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
