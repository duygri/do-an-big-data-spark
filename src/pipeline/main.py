"""CLI entry point for the NYC taxi raw-to-processed pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import yaml
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from src.aggregation.export import export_taxi_aggregations
from src.aggregation.taxi import aggregate_taxi_trips
from src.ingestion.bronze import write_bronze
from src.ingestion.reader import read_raw
from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.cleaning.silver import clean_to_silver
from src.pipeline.taxi_inputs import validate_taxi_inputs
from src.transformation.joins import join_taxi_zones, read_taxi_zone_lookup
from src.transformation.processed import write_processed_taxi
from src.transformation.taxi import FEATURE_COLUMNS, engineer_taxi_features

LOG = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Big Data Spark pipeline")
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    return parser.parse_args()


def run(config_path: Path) -> int:
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with config_path.open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping")
    try:
        input_config = config["input"]
        paths = config["paths"]
        spark_config = config["spark"]
        raw_path = str(input_config["path"])
        file_format = str(input_config["format"]).lower()
        bronze_path = str(paths["bronze"])
        silver_path = str(paths["silver"])
        report_path = str(paths["reports"])
        gold_path = str(paths.get("gold", Path(silver_path).parent / "gold"))
        processed_path = str(paths.get("processed_taxi", Path(gold_path) / "taxi_trips"))
        aggregations_path = str(
            paths.get("aggregations", Path(gold_path) / "aggregations")
        )
        analysis_reports_path = str(
            paths.get("analysis_reports", Path(report_path) / "taxi_aggregations")
        )
        aggregation_config = config.get("aggregation", {})
        if not isinstance(aggregation_config, dict):
            raise TypeError("aggregation must be a YAML mapping")
        aggregation_top_n = int(aggregation_config.get("top_n", 10))
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Missing or invalid configuration field: {exc}") from exc
    if not all(
        (raw_path, bronze_path, silver_path, report_path, processed_path,
         aggregations_path, analysis_reports_path)
    ):
        raise ValueError("Input, Bronze, Silver, processed, and report paths are required")
    if aggregation_top_n <= 0:
        raise ValueError("aggregation.top_n must be greater than zero")
    if input_config.get("schema") != "yellow_taxi":
        raise ValueError("Cleaning currently supports only input.schema: yellow_taxi")
    input_files = validate_taxi_inputs(raw_path, file_format)
    spark = None
    join_result = None
    cached_featured = None
    try:
        builder = SparkSession.builder.appName(spark_config.get("app_name", "big-data-spark-pipeline"))
        builder = builder.master(spark_config.get("master", "local[*]"))
        builder = builder.config(
            "spark.sql.session.timeZone",
            spark_config.get("session_timezone", "America/New_York"),
        )
        if "max_partition_bytes" in spark_config:
            builder = builder.config("spark.sql.files.maxPartitionBytes", spark_config["max_partition_bytes"])
        spark = builder.getOrCreate()
        LOG.info("Stage ingest: %s (%s)", raw_path, file_format)
        LOG.info("Selected taxi files: %s", [path.name for path in input_files])
        schema = YELLOW_TAXI_SCHEMA if input_config.get("schema") == "yellow_taxi" else None
        df = read_raw(
            spark, raw_path, file_format=file_format, schema=schema,
            header=input_config.get("header", True), options=input_config.get("options"),
        )
        LOG.info("Input schema: %s", df.schema.simpleString())
        LOG.info("Input sample: %s", df.limit(2).toJSON().collect())
        LOG.info("Stage bronze: %s", bronze_path)
        count = write_bronze(df, spark, bronze_path)
        LOG.info("Stage bronze complete: %d rows verified", count)
        bronze = spark.read.parquet(bronze_path)
        report = clean_to_silver(bronze, spark, silver_path, report_path)
        silver = spark.read.parquet(silver_path)
        LOG.info("Stage feature engineering")
        featured = engineer_taxi_features(silver)
        LOG.info("Feature schema: %s", featured.schema.simpleString())
        LOG.info(
            "Feature sample: %s",
            featured.select(*FEATURE_COLUMNS).limit(2).toJSON().collect(),
        )

        zone_lookup_path = input_config.get("zone_lookup_path")
        if not zone_lookup_path:
            conventional_lookup = Path(raw_path).parent / "taxi+_zone_lookup.csv"
            if conventional_lookup.is_file():
                zone_lookup_path = str(conventional_lookup)
        if zone_lookup_path:
            LOG.info("Stage zone enrichment: %s", zone_lookup_path)
            zone_lookup = read_taxi_zone_lookup(spark, zone_lookup_path)
            join_result = join_taxi_zones(featured, zone_lookup)
            featured = join_result.dataframe
            LOG.info(
                "Zone join rows: %d before, %d after; unmatched pickup=%d, "
                "unmatched dropoff=%d; duplicate lookup keys=%d",
                join_result.input_rows,
                join_result.output_rows,
                join_result.unmatched_pickup_rows,
                join_result.unmatched_dropoff_rows,
                join_result.duplicate_lookup_keys,
            )
        else:
            LOG.warning(
                "Taxi zone enrichment skipped: set input.zone_lookup_path or place "
                "taxi+_zone_lookup.csv beside the raw taxi CSV files"
            )

        if join_result is None:
            LOG.info("Persisting featured taxi trips for processed and aggregation outputs")
            cached_featured = featured.persist(StorageLevel.DISK_ONLY)
        else:
            LOG.info("Reusing disk cache owned by the taxi zone join")

        LOG.info("Stage processed Parquet write: %s", processed_path)
        write_report = write_processed_taxi(featured, spark, processed_path)
        LOG.info(
            "Processed Parquet verified: %d rows; schema=%s; partitions=%s",
            write_report.row_count,
            write_report.schema,
            write_report.partition_columns,
        )
        LOG.info("Stage taxi aggregation: %s", aggregations_path)
        aggregation_tables = aggregate_taxi_trips(featured)
        aggregation_report = export_taxi_aggregations(
            aggregation_tables,
            aggregations_path,
            analysis_reports_path,
            top_n=aggregation_top_n,
        )
        LOG.info(
            "Taxi aggregations complete: tables=%s; report=%s",
            aggregation_report.table_row_counts,
            aggregation_report.markdown_path,
        )
        return write_report.row_count
    finally:
        try:
            if cached_featured is not None:
                cached_featured.unpersist()
        finally:
            try:
                if join_result is not None:
                    join_result.unpersist()
            finally:
                if spark is not None:
                    spark.stop()
                    LOG.info("Spark session stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run(parse_args().config)
    except Exception:
        LOG.exception("Pipeline failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
