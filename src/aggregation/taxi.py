"""Spark-native aggregation tables for NYC yellow taxi trips."""

from dataclasses import dataclass
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

REQUIRED_COLUMNS = (
    "tpep_pickup_datetime",
    "pickup_year",
    "pickup_month",
    "pickup_hour",
    "pickup_day_of_week",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "payment_type",
)
OPTIONAL_ZONE_COLUMNS = {
    "pickup": ("pickup_zone", "pickup_borough"),
    "dropoff": ("dropoff_zone", "dropoff_borough"),
}
COMPARISON_METRICS = (
    "trip_count",
    "total_fare_amount",
    "avg_fare_amount",
    "total_tip_amount",
    "avg_tip_amount",
    "avg_trip_distance",
)


@dataclass(frozen=True)
class TaxiAggregationTables:
    """The eight queryable Gold aggregation tables for taxi trips."""

    trips_by_month: DataFrame
    trips_by_weekday: DataFrame
    trips_by_hour: DataFrame
    pickup_zones: DataFrame
    dropoff_zones: DataFrame
    trip_metric_stats: DataFrame
    payment_mix: DataFrame
    same_month_comparison: DataFrame


def _zone_table(df: DataFrame, direction: str) -> DataFrame:
    location_column = "PULocationID" if direction == "pickup" else "DOLocationID"
    label_column, borough_column = OPTIONAL_ZONE_COLUMNS[direction]
    source_location = F.col(location_column)
    normalized_location = F.when(
        source_location.isNull() | (source_location <= 0),
        F.lit(None).cast(df.schema[location_column].dataType),
    ).otherwise(source_location)

    label = (
        F.col(label_column).cast(StringType())
        if label_column in df.columns
        else F.lit(None).cast(StringType())
    )
    borough = (
        F.col(borough_column).cast(StringType())
        if borough_column in df.columns
        else F.lit(None).cast(StringType())
    )

    grouped = (
        df.select(
            normalized_location.alias("location_id"),
            label.alias("_zone_label"),
            borough.alias("_borough"),
        )
        .groupBy("location_id")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.first("_zone_label", ignorenulls=True).alias("zone_label"),
            F.first("_borough", ignorenulls=True).alias("borough"),
        )
        .withColumn(
            "zone_label",
            F.when(F.col("location_id").isNull(), F.lit("Unknown/Invalid"))
            .otherwise(F.col("zone_label")),
        )
    )
    rank_window = Window.orderBy(
        F.col("trip_count").desc(), F.col("location_id").asc_nulls_last()
    )
    return (
        grouped.withColumn("trip_rank", F.row_number().over(rank_window))
        .select("location_id", "zone_label", "borough", "trip_count", "trip_rank")
    )


def _metric_stats(df: DataFrame) -> DataFrame:
    metric_columns = (
        ("trip_distance", "trip_distance"),
        ("fare_amount", "fare_amount"),
        ("tip_amount", "tip_amount"),
    )
    tables: Sequence[DataFrame] = tuple(
        df.agg(
            F.count(F.col(column)).alias("observation_count"),
            F.min(F.col(column)).alias("min_value"),
            F.avg(F.col(column)).alias("mean_value"),
            F.percentile_approx(F.col(column), 0.5, 10000).alias("median_approx"),
            F.max(F.col(column)).alias("max_value"),
            F.stddev_samp(F.col(column)).alias("stddev"),
        ).select(
            F.lit(metric_name).alias("metric_name"),
            "observation_count",
            "min_value",
            "mean_value",
            "median_approx",
            "max_value",
            "stddev",
        )
        for metric_name, column in metric_columns
    )
    result = tables[0]
    for table in tables[1:]:
        result = result.unionByName(table)
    return result


def _same_month_comparison(monthly: DataFrame) -> DataFrame:
    year_2019 = monthly.filter(
        (F.col("pickup_year") == 2019)
        & F.col("pickup_month").between(1, 6)
    ).select(
        "pickup_month",
        *[F.col(metric).alias(f"{metric}_2019") for metric in COMPARISON_METRICS],
    )
    year_2020 = monthly.filter(
        (F.col("pickup_year") == 2020)
        & F.col("pickup_month").between(1, 6)
    ).select(
        "pickup_month",
        *[F.col(metric).alias(f"{metric}_2020") for metric in COMPARISON_METRICS],
    )
    overlapping = year_2019.join(year_2020, "pickup_month", "inner")
    projected = [F.col("pickup_month")]
    for metric in COMPARISON_METRICS:
        value_2019 = F.col(f"{metric}_2019")
        value_2020 = F.col(f"{metric}_2020")
        delta = value_2020 - value_2019
        projected.extend(
            [
                value_2019,
                value_2020,
                delta.alias(f"{metric}_delta"),
                F.when(value_2019 != 0, delta / value_2019 * 100.0)
                .otherwise(F.lit(None).cast("double"))
                .alias(f"{metric}_pct_delta"),
            ]
        )
    return overlapping.select(*projected)


def aggregate_taxi_trips(df: DataFrame) -> TaxiAggregationTables:
    """Build Spark DataFrames for time, zone, metric, payment, and comparison views.

    Pickup time columns are supplied by taxi feature engineering, which uses the
    Spark session timezone. Negative distance/fare/tip values are retained;
    null measurements are excluded only from their own descriptive statistics.
    """
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            "Taxi aggregation input is missing required columns: "
            + ", ".join(missing)
        )

    monthly_internal = df.groupBy("pickup_year", "pickup_month").agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum("fare_amount").alias("total_fare_amount"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.sum("tip_amount").alias("total_tip_amount"),
        F.avg("tip_amount").alias("avg_tip_amount"),
        F.avg("trip_distance").alias("avg_trip_distance"),
    )
    trips_by_month = monthly_internal.select(
        "pickup_year",
        "pickup_month",
        "trip_count",
        "total_fare_amount",
        "avg_fare_amount",
        "total_tip_amount",
        "avg_tip_amount",
    )
    trips_by_weekday = df.groupBy("pickup_day_of_week").agg(
        F.count(F.lit(1)).alias("trip_count")
    )
    trips_by_hour = df.groupBy("pickup_hour").agg(
        F.count(F.lit(1)).alias("trip_count")
    )

    payment_counts = (
        df.select(
            F.coalesce(F.col("payment_type").cast(StringType()), F.lit("unknown"))
            .alias("payment_type")
        )
        .groupBy("payment_type")
        .agg(F.count(F.lit(1)).alias("trip_count"))
    )
    total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    payment_mix = payment_counts.withColumn(
        "share_percent",
        F.col("trip_count") * F.lit(100.0) / F.sum("trip_count").over(total_window),
    ).select("payment_type", "trip_count", "share_percent")

    return TaxiAggregationTables(
        trips_by_month=trips_by_month,
        trips_by_weekday=trips_by_weekday,
        trips_by_hour=trips_by_hour,
        pickup_zones=_zone_table(df, "pickup"),
        dropoff_zones=_zone_table(df, "dropoff"),
        trip_metric_stats=_metric_stats(df),
        payment_mix=payment_mix,
        same_month_comparison=_same_month_comparison(monthly_internal),
    )
