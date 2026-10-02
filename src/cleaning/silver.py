"""Run the NYC taxi cleaning stages and persist the verified Silver layer."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.storagelevel import StorageLevel

from src.cleaning.quality import null_counts, quality_report, write_report
from src.cleaning.taxi import normalize_taxi, remove_exact_duplicates, remove_missing_required, validate_schema

LOG = logging.getLogger(__name__)


def clean_to_silver(
    bronze: DataFrame,
    spark: SparkSession,
    silver_path: str,
    report_path: str,
) -> dict:
    """Clean, verify, and report the 18-column taxi data."""
    validate_schema(bronze)
    cached = []
    try:
        bronze = bronze.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(bronze)
        counts = {"bronze": bronze.count()}
        if counts["bronze"] == 0:
            raise ValueError("Bronze contains no rows")
        before_nulls = null_counts(bronze)
        LOG.info("Stage missing values: %d input rows", counts["bronze"])
        required = remove_missing_required(bronze).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(required)
        counts["after_missing"] = required.count()
        LOG.info("Stage missing values: %d rows retained", counts["after_missing"])
        LOG.info("Stage deduplication")
        deduped = remove_exact_duplicates(required).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(deduped)
        counts["after_deduplication"] = deduped.count()
        LOG.info("Stage deduplication: %d rows retained", counts["after_deduplication"])
        LOG.info("Stage normalization")
        normalized = normalize_taxi(deduped).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(normalized)
        counts["silver"] = normalized.count()
        LOG.info("Stage Silver write: %s", silver_path)
        normalized.write.mode("overwrite").option("compression", "snappy").parquet(silver_path)
        silver = spark.read.parquet(silver_path)
        if silver.schema != normalized.schema or silver.count() != counts["silver"]:
            raise RuntimeError("Silver read-back does not match the cleaned data")
        LOG.info("Stage quality report: %s", report_path)
        report = quality_report(silver, counts, before_nulls)
        write_report(report, report_path)
        LOG.info("Silver complete: %d rows; warnings: %s", counts["silver"], report["warnings"])
        return report
    finally:
        for frame in reversed(cached):
            frame.unpersist()
