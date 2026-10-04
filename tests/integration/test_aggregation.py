"""Contract tests for the Spark taxi aggregation tables."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)


NY = ZoneInfo("America/New_York")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-aggregation-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def aggregate_taxi_trips(df):
    try:
        from src.aggregation.taxi import aggregate_taxi_trips as aggregate
    except ModuleNotFoundError:
        pytest.fail("aggregate_taxi_trips is not implemented", pytrace=False)
    return aggregate(df)


def aggregate_taxi_trips(df):
    try:
        from src.aggregation.taxi import aggregate_taxi_trips as aggregate
    except ModuleNotFoundError:
        pytest.fail("aggregate_taxi_trips is not implemented", pytrace=False)
    return aggregate(df)

def taxi_frame(spark):
    schema = StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), False),
            StructField("pickup_year", IntegerType(), True),
            StructField("pickup_month", IntegerType(), True),
            StructField("pickup_hour", IntegerType(), True),
            StructField("pickup_day_of_week", IntegerType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("payment_type", IntegerType(), True),
        ]
    )
    rows = [
        (datetime(2019, 1, 15, 10, tzinfo=NY), 2019, 1, 10, 3, 1, 2, 1.0, 10.0, 2.0, 1),
        (datetime(2019, 1, 15, 11, tzinfo=NY), 2019, 1, 11, 3, 1, 3, 2.0, 20.0, None, None),
        (datetime(2019, 2, 15, 12, tzinfo=NY), 2019, 2, 12, 6, -1, None, -1.0, -5.0, -2.0, 2),
        (datetime(2020, 1, 6, 8, tzinfo=NY), 2020, 1, 8, 2, 2, 4, 2.0, 30.0, 3.0, 2),
        # DST starts in New York: this pickup is still in hour 1 on Sunday.
        (datetime(2020, 3, 8, 1, 50, tzinfo=NY), 2020, 3, 1, 1, None, 0, None, 40.0, 4.0, 1),
        (datetime(2020, 6, 10, 16, tzinfo=NY), 2020, 6, 16, 4, 0, 5, 5.0, 50.0, 5.0, 1),
        (datetime(2019, 5, 7, 9, tzinfo=NY), 2019, 5, 9, 3, 0, -1, 0.0, 0.0, 0.0, None),
        (datetime(2020, 5, 8, 10, tzinfo=NY), 2020, 5, 10, 6, 3, 0, 1.0, 5.0, 1.0, 1),
    ]
    return spark.createDataFrame(rows, schema)


def test_aggregate_taxi_trips_builds_time_tables_and_reconciles_counts(spark):
    tables = aggregate_taxi_trips(taxi_frame(spark))

    assert type(tables).__name__ == "TaxiAggregationTables"
    assert tables.trips_by_month.columns == [
        "pickup_year", "pickup_month", "trip_count", "total_fare_amount",
        "avg_fare_amount", "total_tip_amount", "avg_tip_amount",
    ]
    assert tables.trips_by_weekday.columns == ["pickup_day_of_week", "trip_count"]
    assert tables.trips_by_hour.columns == ["pickup_hour", "trip_count"]

    month_rows = {
        (row.pickup_year, row.pickup_month): row
        for row in tables.trips_by_month.collect()
    }
    assert {key: row.trip_count for key, row in month_rows.items()} == {
        (2019, 1): 2, (2019, 2): 1, (2019, 5): 1,
        (2020, 1): 1, (2020, 3): 1, (2020, 5): 1, (2020, 6): 1,
    }
    assert month_rows[(2019, 1)].total_fare_amount == 30.0
    assert month_rows[(2019, 1)].avg_fare_amount == 15.0
    assert month_rows[(2019, 1)].total_tip_amount == 2.0
    assert month_rows[(2019, 1)].avg_tip_amount == 2.0

    assert {
        row.pickup_day_of_week: row.trip_count
        for row in tables.trips_by_weekday.collect()
    } == {1: 1, 2: 1, 3: 3, 4: 1, 6: 2}
    assert {
        row.pickup_hour: row.trip_count for row in tables.trips_by_hour.collect()
    } == {1: 1, 8: 1, 9: 1, 10: 2, 11: 1, 12: 1, 16: 1}
    assert tables.trips_by_month.agg({"trip_count": "sum"}).first()[0] == 8
    assert tables.trips_by_weekday.agg({"trip_count": "sum"}).first()[0] == 8
    assert tables.trips_by_hour.agg({"trip_count": "sum"}).first()[0] == 8


def test_aggregate_taxi_trips_requires_feature_columns(spark):
    incomplete = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError, match="missing required columns.*PULocationID"):
        aggregate_taxi_trips(incomplete)


def test_zone_tables_handle_optional_labels_and_unknown_ids(spark):
    source = taxi_frame(spark)
    without_labels = aggregate_taxi_trips(source)

    assert without_labels.pickup_zones.columns == [
        "location_id", "zone_label", "borough", "trip_count", "trip_rank",
    ]
    pickup = {row.location_id: row for row in without_labels.pickup_zones.collect()}
    assert pickup[None].trip_count == 4
    assert pickup[None].zone_label == "Unknown/Invalid"
    assert pickup[None].trip_rank == 1
    assert pickup[1].trip_count == 2
    assert pickup[1].zone_label is None
    assert pickup[1].borough is None
    assert sum(row.trip_count for row in pickup.values()) == 8

    labeled = (
        source.withColumn(
            "pickup_zone", when(col("PULocationID") == 1, "Zone 1")
        )
        .withColumn(
            "pickup_borough", when(col("PULocationID") == 1, "Manhattan")
        )
        .withColumn(
            "dropoff_zone", when(col("DOLocationID") == 2, "Zone 2")
        )
        .withColumn(
            "dropoff_borough", when(col("DOLocationID") == 2, "Brooklyn")
        )
    )
    with_labels = aggregate_taxi_trips(labeled)
    labeled_pickup = {
        row.location_id: row for row in with_labels.pickup_zones.collect()
    }
    assert labeled_pickup[1].zone_label == "Zone 1"
    assert labeled_pickup[1].borough == "Manhattan"
    assert sum(row.trip_count for row in with_labels.dropoff_zones.collect()) == 8
    assert with_labels.dropoff_zones.filter(col("location_id").isNull()).first().zone_label == "Unknown/Invalid"


def test_metric_stats_keep_negative_values_and_exclude_nulls(spark):
    tables = aggregate_taxi_trips(taxi_frame(spark))
    assert tables.trip_metric_stats.columns == [
        "metric_name", "observation_count", "min_value", "mean_value",
        "median_approx", "max_value", "stddev",
    ]
    stats = {row.metric_name: row for row in tables.trip_metric_stats.collect()}

    assert stats["trip_distance"].observation_count == 7
    assert stats["trip_distance"].min_value == -1.0
    assert stats["trip_distance"].mean_value == pytest.approx(10 / 7)
    assert stats["trip_distance"].median_approx == 1.0
    assert stats["trip_distance"].max_value == 5.0
    assert stats["fare_amount"].observation_count == 8
    assert stats["fare_amount"].min_value == -5.0
    assert stats["fare_amount"].mean_value == 18.75
    assert stats["fare_amount"].median_approx == 10.0
    assert stats["tip_amount"].observation_count == 7
    assert stats["tip_amount"].min_value == -2.0
    assert stats["tip_amount"].median_approx == 2.0


def test_payment_mix_includes_nulls_in_denominator(spark):
    tables = aggregate_taxi_trips(taxi_frame(spark))
    assert tables.payment_mix.columns == ["payment_type", "trip_count", "share_percent"]
    payment = {row.payment_type: row for row in tables.payment_mix.collect()}

    assert payment["1"].trip_count == 4
    assert payment["1"].share_percent == 50.0
    assert payment["2"].trip_count == 2
    assert payment["2"].share_percent == 25.0
    assert payment["unknown"].trip_count == 2
    assert payment["unknown"].share_percent == 25.0
    assert sum(row.trip_count for row in payment.values()) == 8
    assert sum(row.share_percent for row in payment.values()) == pytest.approx(100.0)


def test_same_month_comparison_uses_only_overlapping_jan_to_jun_months(spark):
    tables = aggregate_taxi_trips(taxi_frame(spark))
    metrics = (
        "trip_count", "total_fare_amount", "avg_fare_amount", "total_tip_amount",
        "avg_tip_amount", "avg_trip_distance",
    )
    expected_columns = ["pickup_month"] + [
        f"{metric}_{suffix}"
        for metric in metrics
        for suffix in ("2019", "2020", "delta", "pct_delta")
    ]
    assert tables.same_month_comparison.columns == expected_columns
    rows = {row.pickup_month: row for row in tables.same_month_comparison.collect()}
    assert set(rows) == {1, 5}

    january = rows[1]
    assert january.trip_count_2019 == 2
    assert january.trip_count_2020 == 1
    assert january.trip_count_delta == -1
    assert january.trip_count_pct_delta == -50.0
    assert january.total_fare_amount_2019 == 30.0
    assert january.total_fare_amount_2020 == 30.0
    assert january.total_fare_amount_delta == 0.0
    assert january.avg_fare_amount_pct_delta == 100.0
    assert january.avg_trip_distance_2019 == 1.5
    assert january.avg_trip_distance_2020 == 2.0
    assert january.avg_trip_distance_pct_delta == pytest.approx(100 / 3)

    may = rows[5]
    assert may.total_fare_amount_2019 == 0.0
    assert may.total_fare_amount_2020 == 5.0
    assert may.total_fare_amount_delta == 5.0
    assert may.total_fare_amount_pct_delta is None
    assert may.avg_trip_distance_pct_delta is None



