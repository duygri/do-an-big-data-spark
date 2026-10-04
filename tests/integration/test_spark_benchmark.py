"""Equivalence contract for the standalone persistence benchmark."""

import json
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)
from datetime import datetime


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("taxi-spark-benchmark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_persistence_benchmark_returns_equivalent_results_for_both_modes(spark):
    from src.aggregation.spark_benchmark import compare_aggregation_persistence

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
    source = spark.createDataFrame(
        [
            (datetime(2019, 1, 10, 9), 2019, 1, 9, 5, 10, 20, 1.5, 12.0, 2.0, 1),
            (datetime(2020, 1, 10, 10), 2020, 1, 10, 6, 10, 30, 2.0, 15.0, None, None),
            (datetime(2020, 1, 10, 11), 2020, 1, 11, 6, -1, None, -1.0, -2.0, 0.0, 2),
        ],
        schema,
    )

    results = compare_aggregation_persistence(source)

    assert [result.mode for result in results] == ["baseline", "persisted"]
    assert results[0].fingerprint == results[1].fingerprint
    assert all(result.input_rows == 3 for result in results)
    assert results[0].table_row_counts == results[1].table_row_counts
    assert all(result.elapsed_seconds > 0 for result in results)
    assert all(result.rows_per_second > 0 for result in results)
    assert all(result.spark_version == spark.version for result in results)
    assert all(result.shuffle_partitions == 2 for result in results)
    assert all(result.storage_level for result in results)
    assert all("Scan" in result.physical_plan or "scan" in result.physical_plan.lower() for result in results)


def test_cli_writes_results_and_physical_plan_evidence(tmp_path, monkeypatch):
    try:
        import scripts.benchmark_spark_aggregation as cli
    except ModuleNotFoundError:
        pytest.fail("benchmark CLI is not implemented", pytrace=False)
    from src.aggregation.spark_benchmark import AggregationBenchmarkResult

    main = cli.main
    output_path = tmp_path / "benchmark"

    class FakeReader:
        def parquet(self, _path):
            return object()

    class FakeSpark:
        read = FakeReader()
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

    monkeypatch.setattr(cli.SparkSession, "builder", FakeBuilder())
    monkeypatch.setattr(
        cli,
        "compare_aggregation_persistence",
        lambda _trips, repetitions: [
            AggregationBenchmarkResult(
                mode=mode,
                elapsed_seconds=0.1,
                input_rows=1,
                table_row_counts={"trips_by_month": 1},
                fingerprint="same-fingerprint",
                rows_per_second=10.0,
                spark_version="3.5.9",
                shuffle_partitions=2,
                storage_level=storage,
                physical_plan="Scan ExistingRDD [test]\n",
            )
            for mode, storage in (("baseline", "NONE"), ("persisted", "DISK_ONLY"))
        ],
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_spark_aggregation",
            "--input",
            str(tmp_path / "processed"),
            "--output",
            str(output_path),
        ],
    )
    main()

    json_path = output_path / "aggregation_benchmark.json"
    csv_path = output_path / "aggregation_benchmark.csv"
    records = json.loads(json_path.read_text(encoding="utf-8"))
    assert [record["mode"] for record in records] == ["baseline", "persisted"]
    assert csv_path.is_file()
    for record in records:
        plan_path = output_path / record["physical_plan_file"]
        assert plan_path.is_file()
        assert "scan" in plan_path.read_text(encoding="utf-8").lower()
    assert fake_spark.stopped is True

