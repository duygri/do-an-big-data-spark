"""Pipeline integration for taxi aggregations and export defaults."""

import csv
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
from pyspark.sql import DataFrame

from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.pipeline.main import run


@pytest.fixture
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-aggregation-pipeline-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def write_taxi_csv(path: Path) -> None:
    fieldnames = YELLOW_TAXI_SCHEMA.fieldNames()
    rows = []
    for index in range(4):
        day = index + 1
        rows.append(
            {
                "VendorID": 1,
                "tpep_pickup_datetime": f"2020-04-{day:02d} 10:00:00",
                "tpep_dropoff_datetime": f"2020-04-{day:02d} 10:15:00",
                "passenger_count": 1,
                "trip_distance": 1.5 + index,
                "RatecodeID": 1,
                "store_and_fwd_flag": "N",
                "PULocationID": 10 + index,
                "DOLocationID": 20 + index,
                "payment_type": 1 if index % 2 == 0 else 2,
                "fare_amount": 8.0 + index,
                "extra": 0.0,
                "mta_tax": 0.5,
                "tip_amount": 1.0,
                "tolls_amount": 0.0,
                "improvement_surcharge": 0.3,
                "total_amount": 9.8 + index,
                "congestion_surcharge": "",
            }
        )
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)



def write_header_only_taxi_csv(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=YELLOW_TAXI_SCHEMA.fieldNames())
        writer.writeheader()


def test_pipeline_writes_aggregation_outputs_from_six_months(tmp_path, monkeypatch, spark):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for month in range(1, 7):
        csv_path = raw_dir / f"yellow_tripdata_2020-{month:02d}.csv"
        if month == 4:
            write_taxi_csv(csv_path)
        else:
            write_header_only_taxi_csv(csv_path)
    input_pattern = (raw_dir / "yellow_tripdata_2020-0[1-6].csv").as_posix()
    gold_path = tmp_path / "gold"
    report_path = tmp_path / "reports"
    processed_path = gold_path / "taxi_trips"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""input:
  path: '{input_pattern}'
  format: csv
  schema: yellow_taxi
  header: true
paths:
  bronze: {tmp_path / 'bronze'}
  silver: {tmp_path / 'silver'}
  gold: {gold_path}
  processed_taxi: {processed_path}
  reports: {report_path}
spark:
  app_name: aggregation-pipeline-test
  master: local[1]
  session_timezone: America/New_York
aggregation:
  top_n: 2
""",
        encoding="utf-8",
    )

    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    has_winutils = bool(
        hadoop_home and (Path(hadoop_home) / "bin" / "winutils.exe").is_file()
    )
    written_frames = {}

    def write_parquet(writer, path):
        written_frames[str(path)] = writer._df

    def read_parquet(_reader, path, *args, **kwargs):
        return written_frames[str(path)]

    if os.name == "nt" and not has_winutils:
        monkeypatch.setattr(DataFrameWriter, "parquet", write_parquet)
        monkeypatch.setattr(DataFrameReader, "parquet", read_parquet)

    persisted_frames = []
    unpersisted_frames = []
    original_persist = DataFrame.persist
    original_unpersist = DataFrame.unpersist

    def track_persist(frame, *args, **kwargs):
        result = original_persist(frame, *args, **kwargs)
        persisted_frames.append(result)
        return result

    def track_unpersist(frame, *args, **kwargs):
        unpersisted_frames.append(frame)
        return original_unpersist(frame, *args, **kwargs)

    monkeypatch.setattr(DataFrame, "persist", track_persist)
    monkeypatch.setattr(DataFrame, "unpersist", track_unpersist)

    row_count = run(config_path)

    aggregate_root = gold_path / "aggregations"
    analysis_root = report_path / "taxi_aggregations"
    expected_tables = {
        "trips_by_month", "trips_by_weekday", "trips_by_hour", "pickup_zones",
        "dropoff_zones", "trip_metric_stats", "payment_mix",
    }
    assert row_count == 4
    if os.name == "nt" and not has_winutils:
        assert all(str(aggregate_root / name) in written_frames for name in expected_tables)
    else:
        assert all((aggregate_root / name).is_dir() for name in expected_tables)
    assert (analysis_root / "taxi_aggregation_report.md").is_file()
    assert (analysis_root / "csv" / "trips_by_month.csv").is_file()
    assert (analysis_root / "charts" / "monthly_trips.png").is_file()
    assert {id(frame) for frame in persisted_frames} == {
        id(frame) for frame in unpersisted_frames
    }
    assert len(persisted_frames) == len(unpersisted_frames)
    assert len(persisted_frames) == 5
