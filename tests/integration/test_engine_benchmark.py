"""Spark/Pandas equivalence contract for the taxi benchmark workload."""

import csv
import json
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.ingestion.schema import YELLOW_TAXI_SCHEMA


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-engine-benchmark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def taxi_row(pickup, dropoff, pickup_zone, dropoff_zone, distance, fare, tip, payment):
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff,
        "passenger_count": 1,
        "trip_distance": distance,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": pickup_zone,
        "DOLocationID": dropoff_zone,
        "payment_type": payment,
        "fare_amount": fare,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": tip,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "total_amount": fare + tip + 0.8,
        "congestion_surcharge": 0.0,
    }


def test_spark_and_pandas_benchmarks_return_same_aggregate_fingerprint(spark, tmp_path):
    try:
        from src.aggregation.engine_benchmark import benchmark_spark_vs_pandas
    except ModuleNotFoundError:
        pytest.fail("Spark/Pandas engine benchmark is not implemented", pytrace=False)

    csv_path = tmp_path / "taxi.csv"
    duplicate = taxi_row("2019-01-03 10:00:00", "2019-01-03 10:10:00", 10, 20, 1.0, 8.0, 1.0, 1)
    rows = [
        duplicate,
        duplicate.copy(),
        taxi_row("2019-01-04 11:00:00", "2019-01-04 11:15:00", 11, 21, 2.0, 12.0, 2.0, 2),
        taxi_row("2019-01-05 12:00:00", "2019-01-05 12:10:00", "", 22, 3.0, 14.0, 1.0, 1),
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=YELLOW_TAXI_SCHEMA.fieldNames())
        writer.writeheader()
        writer.writerows(rows)

    results = benchmark_spark_vs_pandas([str(csv_path)], spark, "tiny")

    assert [result.engine for result in results] == ["spark", "pandas"]
    assert results[0].fingerprint == results[1].fingerprint
    assert all(result.dataset_label == "tiny" for result in results)
    assert all(result.input_bytes == csv_path.stat().st_size for result in results)
    assert all(result.input_rows == 2 for result in results)
    assert all(result.spark_startup_seconds >= 0 for result in results)
    assert all(result.read_seconds >= 0 for result in results)
    assert all(result.aggregation_seconds >= 0 for result in results)
    assert all(result.total_seconds >= 0 for result in results)
    assert all(result.rows_per_second >= 0 for result in results)
    assert all(result.peak_rss_mb > 0 for result in results)
    assert all(result.python_version for result in results)
    assert all(result.spark_version == spark.version for result in results)


def test_cli_accepts_labeled_datasets_and_writes_results(tmp_path, monkeypatch):
    try:
        import scripts.benchmark_spark_vs_pandas as cli
    except ModuleNotFoundError:
        pytest.fail("Spark/Pandas benchmark CLI is not implemented", pytrace=False)
    from src.aggregation.engine_benchmark import EngineBenchmarkResult

    output_path = tmp_path / "results"

    class FakeSpark:
        version = "3.5.9"
        stopped = False

        def stop(self):
            self.stopped = True

    fake_spark = FakeSpark()

    class FakeBuilder:
        def appName(self, _name):
            return self

        def config(self, _key, _value):
            return self

        def getOrCreate(self):
            return fake_spark

    calls = []

    def fake_benchmark(input_paths, spark, dataset_label, *, spark_startup_seconds):
        calls.append((input_paths, spark, dataset_label, spark_startup_seconds))
        return [
            EngineBenchmarkResult(
                engine=engine,
                dataset_label=dataset_label,
                input_bytes=123,
                input_rows=2,
                spark_startup_seconds=spark_startup_seconds if engine == "spark" else 0.0,
                read_seconds=0.1,
                aggregation_seconds=0.2,
                total_seconds=0.3,
                rows_per_second=2 / 0.3,
                peak_rss_mb=32.0,
                python_version="3.12.0",
                spark_version=spark.version,
                pandas_version="2.3.3",
                fingerprint="same",
            )
            for engine in ("spark", "pandas")
        ]

    monkeypatch.setattr(cli.SparkSession, "builder", FakeBuilder())
    monkeypatch.setattr(cli, "benchmark_spark_vs_pandas", fake_benchmark)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_spark_vs_pandas",
            "--dataset", "small=small.csv",
            "--dataset", "medium=medium.csv",
            "--output", str(output_path),
        ],
    )

    cli.main()

    records = json.loads((output_path / "engine_benchmark.json").read_text(encoding="utf-8"))
    with (output_path / "engine_benchmark.csv").open(newline="", encoding="utf-8") as stream:
        csv_rows = list(csv.DictReader(stream))
    assert [call[2] for call in calls] == ["small", "medium"]
    assert [record["dataset_label"] for record in records] == ["small", "small", "medium", "medium"]
    assert len(csv_rows) == 4
    assert fake_spark.stopped is True
