"""Spark-native derived features for cleaned NYC yellow taxi trips."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    month,
    round as spark_round,
    unix_timestamp,
    when,
    year,
)

FEATURE_INPUT_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "tip_amount",
)

FEATURE_COLUMNS = (
    "trip_duration_minutes",
    "pickup_year",
    "pickup_month",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_is_weekend",
    "average_speed_mph",
    "fare_per_mile",
    "tip_percentage",
)


def engineer_taxi_features(df: DataFrame) -> DataFrame:
    """Add documented trip, pickup-time, fare, and tip features.

    Existing input columns are preserved. Invalid-duration and nonpositive-
    distance rows are not removed; undefined ratios are set to null.
    """
    missing = sorted(set(FEATURE_INPUT_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(f"Taxi feature input is missing required columns: {', '.join(missing)}")

    duration_seconds = (
        unix_timestamp(col("tpep_dropoff_datetime"))
        - unix_timestamp(col("tpep_pickup_datetime"))
    )
    pickup_weekday = dayofweek(col("tpep_pickup_datetime"))

    return (
        df.withColumn("trip_duration_minutes", spark_round(duration_seconds / 60.0, 2))
        .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", pickup_weekday)
        .withColumn("pickup_is_weekend", pickup_weekday.isin(1, 7))
        .withColumn(
            "average_speed_mph",
            when(
                (duration_seconds > 0) & (col("trip_distance") >= 0),
                spark_round(col("trip_distance") / (duration_seconds / 3600.0), 2),
            ),
        )
        .withColumn(
            "fare_per_mile",
            when(
                col("trip_distance") > 0,
                spark_round(col("fare_amount") / col("trip_distance"), 2),
            ),
        )
        .withColumn(
            "tip_percentage",
            when(
                col("fare_amount") > 0,
                spark_round(col("tip_amount") / col("fare_amount") * 100.0, 2),
            ),
        )
    )
