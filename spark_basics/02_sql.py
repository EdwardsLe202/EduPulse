"""Exercise 2: Compare Spark SQL, Spark DataFrame and Pandas."""

import pandas as pd

from _local import EVENTS_CSV, EVENTS_JSON, create_spark, daily_event_counts


def main() -> None:
    spark = create_spark("EduPulse 02 - Spark SQL")
    try:
        events = spark.read.json(str(EVENTS_JSON))
        events.createOrReplaceTempView("learning_events")

        sql_counts = spark.sql(
            """
            SELECT TO_DATE(TO_TIMESTAMP(event_time)) AS event_day,
                   COUNT(*) AS event_count
            FROM learning_events
            GROUP BY event_day
            ORDER BY event_day
            """
        )

        csv_events = pd.read_csv(EVENTS_CSV, usecols=["event_time"])
        csv_events["event_day"] = pd.to_datetime(
            csv_events["event_time"], format="ISO8601", utc=True
        ).dt.date
        pandas_counts = (
            csv_events.groupby("event_day", sort=True, dropna=False)
            .size()
            .reset_index(name="event_count")
        )

        # Bring only the daily summaries back to the driver for the comparison.
        dataframe_result = daily_event_counts(events).toPandas()
        sql_result = sql_counts.toPandas()
        pd.testing.assert_frame_equal(dataframe_result, sql_result)
        pd.testing.assert_frame_equal(sql_result, pandas_counts)

        print(sql_result.to_string(index=False))
        print(
            f"Spark DataFrame, Spark SQL and pandas agree on {len(sql_result)} UTC days "
            f"and {sql_result['event_count'].sum():,} events."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
