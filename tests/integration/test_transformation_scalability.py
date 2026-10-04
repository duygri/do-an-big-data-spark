"""Scalability regression tests for taxi transformation output."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

from src.transformation.joins import TAXI_ZONE_LOOKUP_SCHEMA, join_taxi_zones
from src.transformation.processed import write_processed_taxi


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-transformation-scalability-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_processed_writer_spreads_one_month_across_multiple_tasks(spark, tmp_path):
    row_count = 128
    source = spark.createDataFrame(
        [(row_id, 2020, 4) for row_id in range(row_count)],
        ["trip_id", "pickup_year", "pickup_month"],
    )
    destination = Path(tmp_path) / "processed"

    report = write_processed_taxi(
        source,
        spark,
        str(destination),
        max_records_per_file=row_count + 1,
    )

    month_dir = destination / "pickup_year=2020" / "pickup_month=4"
    parquet_files = list(month_dir.glob("*.parquet"))
    restored = spark.read.parquet(str(destination))

    assert len(parquet_files) > 1
    assert report.row_count == row_count
    assert restored.count() == row_count
    assert restored.columns == source.columns


def test_zone_join_result_is_reused_by_processed_writer(spark, tmp_path):
    row_count = 32
    trip_schema = StructType(
        [
            StructField("PULocationID", IntegerType(), False),
            StructField("DOLocationID", IntegerType(), False),
            StructField("pickup_year", IntegerType(), False),
            StructField("pickup_month", IntegerType(), False),
        ]
    )
    rows = [(1, 2, 2020, 4) for _ in range(row_count)]
    evaluations = spark.sparkContext.accumulator(0)

    def count_source_evaluation(row):
        evaluations.add(1)
        return row

    trips = spark.createDataFrame(
        spark.sparkContext.parallelize(rows, 2).map(count_source_evaluation),
        trip_schema,
    )
    lookup = spark.createDataFrame(
        [
            (1, "Manhattan", "Zone 1", "Yellow Zone"),
            (2, "Queens", "Zone 2", "Boro Zone"),
        ],
        TAXI_ZONE_LOOKUP_SCHEMA,
    )
    join_result = join_taxi_zones(trips, lookup)
    evaluations_after_join_metrics = evaluations.value

    try:
        report = write_processed_taxi(
            join_result.dataframe,
            spark,
            str(Path(tmp_path) / "joined-processed"),
            max_records_per_file=row_count + 1,
        )

        assert join_result.input_rows == row_count
        assert join_result.output_rows == row_count
        assert report.row_count == row_count
        assert evaluations.value == evaluations_after_join_metrics
    finally:
        if hasattr(join_result, "unpersist"):
            join_result.unpersist()
