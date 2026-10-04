"""Feature-engineering checks for the NYC yellow taxi Silver data."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession

from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.transformation.taxi import FEATURE_COLUMNS, engineer_taxi_features

NYC_TIMEZONE = ZoneInfo("America/New_York")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-feature-tests")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    yield session
    session.stop()


def taxi_row(**changes):
    fields = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2020, 4, 1, 10, 0, tzinfo=NYC_TIMEZONE),
        "tpep_dropoff_datetime": datetime(2020, 4, 1, 10, 15, tzinfo=NYC_TIMEZONE),
        "passenger_count": 1,
        "trip_distance": 2.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": "n",
        "PULocationID": 10,
        "DOLocationID": 20,
        "payment_type": 1,
        "fare_amount": 8.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 1.6,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": 10.4,
        "congestion_surcharge": None,
    }
    fields.update(changes)
    return tuple(fields[name] for name in YELLOW_TAXI_SCHEMA.fieldNames())


def test_engineer_taxi_features_adds_documented_columns(spark):
    source = spark.createDataFrame([taxi_row()], YELLOW_TAXI_SCHEMA)

    featured = engineer_taxi_features(source)
    row = featured.first()

    assert featured.columns[: len(YELLOW_TAXI_SCHEMA.fieldNames())] == YELLOW_TAXI_SCHEMA.fieldNames()
    assert featured.columns[-len(FEATURE_COLUMNS) :] == list(FEATURE_COLUMNS)
    assert featured.count() == source.count()
    assert row.trip_duration_minutes == 15.0
    assert row.pickup_year == 2020
    assert row.pickup_month == 4
    assert row.pickup_hour == 10
    assert row.pickup_day_of_week == 4  # Spark numbers Sunday as 1.
    assert row.pickup_is_weekend is False
    assert row.average_speed_mph == 10.0
    assert row.fare_per_mile == 3.2
    assert row.tip_percentage == 20.0
    assert row.total_amount == 10.4


def test_engineer_taxi_features_handles_zero_denominators_and_invalid_duration(spark):
    source = spark.createDataFrame(
        [
            taxi_row(
                VendorID=2,
                tpep_pickup_datetime=datetime(2020, 4, 4, 23, 50, tzinfo=NYC_TIMEZONE),
                tpep_dropoff_datetime=datetime(2020, 4, 4, 23, 45, tzinfo=NYC_TIMEZONE),
                trip_distance=0.0,
                fare_amount=0.0,
                tip_amount=None,
            )
        ],
        YELLOW_TAXI_SCHEMA,
    )

    row = engineer_taxi_features(source).first()

    assert row.trip_duration_minutes == -5.0
    assert row.pickup_is_weekend is True
    assert row.average_speed_mph is None
    assert row.fare_per_mile is None
    assert row.tip_percentage is None


def test_engineer_taxi_features_uses_new_york_daylight_saving_time(spark):
    source = spark.createDataFrame(
        [
            taxi_row(
                tpep_pickup_datetime=datetime(2020, 3, 8, 1, 50, tzinfo=NYC_TIMEZONE),
                tpep_dropoff_datetime=datetime(2020, 3, 8, 3, 10, tzinfo=NYC_TIMEZONE),
            )
        ],
        YELLOW_TAXI_SCHEMA,
    )

    row = engineer_taxi_features(source).first()

    assert row.trip_duration_minutes == 20.0


def test_engineer_taxi_features_requires_taxi_columns(spark):
    source = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError, match="missing required columns"):
        engineer_taxi_features(source)
