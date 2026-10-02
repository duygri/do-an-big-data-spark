"""CLI entry point for NYC taxi ingestion, Bronze, and Silver cleaning."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from src.ingestion.bronze import write_bronze
from src.ingestion.reader import read_raw
from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.cleaning.silver import clean_to_silver

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
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Missing or invalid configuration field: {exc}") from exc
    if not raw_path or not bronze_path or not silver_path or not report_path:
        raise ValueError("Input, Bronze, Silver, and report paths are required")
    if input_config.get("schema") != "yellow_taxi":
        raise ValueError("Cleaning currently supports only input.schema: yellow_taxi")
    spark = None
    try:
        builder = SparkSession.builder.appName(spark_config.get("app_name", "big-data-spark-pipeline"))
        builder = builder.master(spark_config.get("master", "local[*]"))
        if "max_partition_bytes" in spark_config:
            builder = builder.config("spark.sql.files.maxPartitionBytes", spark_config["max_partition_bytes"])
        spark = builder.getOrCreate()
        LOG.info("Stage ingest: %s (%s)", raw_path, file_format)
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
        LOG.info("Transformation and aggregation await their team modules")
        return report["stage_counts"]["silver"]
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
