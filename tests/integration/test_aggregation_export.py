"""Gold table and report export contracts for taxi aggregations."""

import csv
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from src.aggregation.taxi import aggregate_taxi_trips

NY = ZoneInfo("America/New_York")
TABLE_NAMES = [
    "trips_by_month", "trips_by_weekday", "trips_by_hour", "pickup_zones",
    "dropoff_zones", "trip_metric_stats", "payment_mix", "same_month_comparison",
]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-aggregation-export-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def taxi_frame(spark, years=(2019, 2020, 2021)):
    schema = StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
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
    rows = []
    for index, year in enumerate(years):
        month = 1 if year in (2019, 2020) else index + 1
        rows.append(
            (
                datetime(year, month, 10, 9 + index, tzinfo=NY),
                year,
                month,
                9 + index,
                5,
                (10 + index) if index != 2 else 0,
                20 + index,
                1.5 + index,
                12.0 + index,
                None if index == 1 else 2.0,
                None if index == 1 else 1,
            )
        )
    return spark.createDataFrame(rows, schema)


def export_taxi_aggregations(*args, **kwargs):
    try:
        from src.aggregation.export import export_taxi_aggregations as export
    except ModuleNotFoundError:
        pytest.fail("export_taxi_aggregations is not implemented", pytrace=False)
    return export(*args, **kwargs)


@pytest.fixture
def parquet_round_trip(monkeypatch):
    """Keep schema/count verification real while isolating unavailable winutils I/O."""
    written_frames = {}

    def write_parquet(writer, path):
        written_frames[str(path)] = writer._df

    def read_parquet(_reader, path, *args, **kwargs):
        return written_frames[str(path)]

    monkeypatch.setattr(DataFrameWriter, "parquet", write_parquet)
    monkeypatch.setattr(DataFrameReader, "parquet", read_parquet)
    return written_frames


def test_export_writes_each_gold_table_and_csv(spark, tmp_path, parquet_round_trip):
    tables = aggregate_taxi_trips(taxi_frame(spark))
    gold = tmp_path / "gold"
    reports = tmp_path / "reports"

    with pytest.raises(ValueError, match="top_n must be greater than zero"):
        export_taxi_aggregations(tables, str(gold), str(reports), top_n=0)

    report = export_taxi_aggregations(tables, str(gold), str(reports), top_n=1)

    assert list(report.table_row_counts) == TABLE_NAMES
    assert set(report.parquet_paths) == set(TABLE_NAMES)
    assert set(report.csv_paths) == set(TABLE_NAMES)
    assert set(parquet_round_trip) == set(report.parquet_paths.values())
    for name in TABLE_NAMES:
        restored = spark.read.parquet(report.parquet_paths[name])
        assert restored.columns == getattr(tables, name).columns
        assert restored.count() == report.table_row_counts[name]
        with open(report.csv_paths[name], newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            rows = list(reader)
        assert len(rows) == report.table_row_counts[name]
        assert reader.fieldnames == getattr(tables, name).columns

    with open(report.csv_paths["payment_mix"], newline="", encoding="utf-8") as stream:
        payment_rows = list(csv.DictReader(stream))
    assert {row["payment_type"] for row in payment_rows} == {"1", "unknown"}
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")
    assert "2019-01 to 2021-03" in markdown
    assert "approximate median" in markdown.lower()
    assert "America/New_York" in markdown
    assert set(report.chart_paths) == {
        "monthly_trips", "top_pickup_zones", "top_dropoff_zones", "payment_mix",
    }
    assert all(path.endswith(".png") and Path(path).stat().st_size > 0 for path in report.chart_paths.values())
    assert report.table_row_counts["pickup_zones"] == 3


def test_report_handles_empty_period_comparison(spark, tmp_path, parquet_round_trip):
    tables = aggregate_taxi_trips(taxi_frame(spark, years=(2021,)))

    report = export_taxi_aggregations(
        tables, str(tmp_path / "gold"), str(tmp_path / "reports")
    )

    assert report.table_row_counts["same_month_comparison"] == 0
    with open(report.csv_paths["same_month_comparison"], newline="", encoding="utf-8") as stream:
        comparison_rows = list(csv.DictReader(stream))
    assert comparison_rows == []
    markdown = Path(report.markdown_path).read_text(encoding="utf-8").lower()
    assert "insufficient overlapping months" in markdown

