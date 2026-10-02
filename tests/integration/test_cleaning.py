"""NYC taxi cleaning and quality-report integration checks."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.cleaning.quality import null_counts
from src.cleaning.silver import clean_to_silver
from src.cleaning.taxi import normalize_taxi, remove_exact_duplicates, remove_missing_required
from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.pipeline.main import run


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("cleaning-tests").getOrCreate()
    yield session
    session.stop()


def taxi_row(**changes):
    fields = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2020, 4, 1, 10),
        "tpep_dropoff_datetime": datetime(2020, 4, 1, 10, 15),
        "passenger_count": None,
        "trip_distance": 2.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": " N ",
        "PULocationID": 10,
        "DOLocationID": 20,
        "payment_type": 1,
        "fare_amount": -5.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 0.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": -4.2,
        "congestion_surcharge": None,
    }
    fields.update(changes)
    return tuple(fields[name] for name in YELLOW_TAXI_SCHEMA.fieldNames())


def test_cleaning_rules_keep_optional_nulls_and_negative_fares(spark):
    rows = [taxi_row(), taxi_row(), taxi_row(PULocationID=None)]
    source = spark.createDataFrame(rows, YELLOW_TAXI_SCHEMA)
    assert null_counts(source)["passenger_count"] == 3
    required = remove_missing_required(source)
    assert required.count() == 2
    deduped = remove_exact_duplicates(required)
    assert deduped.count() == 1
    normalized = normalize_taxi(deduped)
    row = normalized.first()
    assert row.store_and_fwd_flag == "n"
    assert row.passenger_count is None
    assert row.fare_amount == -5.0
    assert normalized.schema == YELLOW_TAXI_SCHEMA


def test_silver_report_and_readback(tmp_path, spark):
    source = spark.createDataFrame(
        [taxi_row(), taxi_row(), taxi_row(tpep_pickup_datetime=None)],
        YELLOW_TAXI_SCHEMA,
    )
    silver_path = str(tmp_path / "silver")
    reports = str(tmp_path / "reports")
    report = clean_to_silver(source, spark, silver_path, reports)
    assert report["stage_counts"] == {
        "bronze": 3, "after_missing": 2, "after_deduplication": 1, "silver": 1,
    }
    assert report["null_counts_before"]["passenger_count"] == 3
    assert report["null_counts_after"]["passenger_count"] == 1
    assert report["warnings"]["negative_fare_amount"] == 1
    assert report["warnings"]["negative_total_amount"] == 1
    assert spark.read.parquet(silver_path).first().store_and_fwd_flag == "n"
    assert (tmp_path / "reports" / "taxi_quality.json").is_file()
    assert (tmp_path / "reports" / "taxi_quality.md").is_file()


def test_pipeline_rejects_other_schema_before_spark(tmp_path):
    config = tmp_path / "other.yaml"
    config.write_text(
        "input:\n  path: example.json\n  format: json\n"
        "paths:\n  bronze: bronze\n  silver: silver\n  reports: reports\n"
        "spark:\n  master: local[1]\n", encoding="utf-8",
    )
    with pytest.raises(ValueError, match="only input.schema: yellow_taxi"):
        run(config)


def test_cli_runs_ingestion_through_silver(tmp_path, spark):
    csv = tmp_path / "taxi.csv"
    csv.write_text(
        ",".join(YELLOW_TAXI_SCHEMA.fieldNames()) + "\n"
        "1,2020-04-01 10:00:00,2020-04-01 10:15:00,,2.5,1, N ,10,20,1,-5,0,0.5,0,0,0.3,-4.2,\n",
        encoding="utf-8",
    )
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "input:\n"
        f"  path: {csv.as_posix()}\n"
        "  format: csv\n  schema: yellow_taxi\n  header: true\n"
        "paths:\n"
        f"  bronze: {(tmp_path / 'bronze').as_posix()}\n"
        f"  silver: {(tmp_path / 'silver').as_posix()}\n"
        f"  reports: {(tmp_path / 'reports').as_posix()}\n"
        "spark:\n  master: local[1]\n  app_name: cleaning-cli-test\n",
        encoding="utf-8",
    )
    assert run(config) == 1
    assert (tmp_path / "reports" / "taxi_quality.json").is_file()
    assert SparkSession.getActiveSession() is None
