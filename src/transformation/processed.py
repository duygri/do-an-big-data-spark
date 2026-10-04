"""Persist and verify the enriched taxi trips in the processed Parquet layer."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

DEFAULT_PARTITION_COLUMNS = ("pickup_year", "pickup_month")
DEFAULT_MAX_RECORDS_PER_FILE = 1_000_000


@dataclass(frozen=True)
class ProcessedWriteReport:
    """Read-back checks recorded after writing the processed data."""

    path: str
    row_count: int
    schema: str
    partition_columns: tuple[str, ...]


def _schema_signature(df: DataFrame) -> tuple[tuple[str, str], ...]:
    """Compare column names and Spark data types, ignoring nullability metadata."""
    return tuple((field.name, field.dataType.json()) for field in df.schema.fields)


def write_processed_taxi(
    df: DataFrame,
    spark: SparkSession,
    path: str,
    partition_columns: tuple[str, ...] = DEFAULT_PARTITION_COLUMNS,
    max_records_per_file: int = DEFAULT_MAX_RECORDS_PER_FILE,
) -> ProcessedWriteReport:
    """Overwrite Snappy Parquet, partition by pickup month, and verify read-back.

    Repartitioning on the low-cardinality partition keys co-locates each month
    before writing. ``maxRecordsPerFile`` caps large monthly files without
    creating partitions for individual taxi zones or trips.
    """
    destination = str(path).strip()
    if not destination:
        raise ValueError("Processed Parquet output path cannot be empty")
    if not partition_columns:
        raise ValueError("At least one processed Parquet partition column is required")
    if max_records_per_file < 1:
        raise ValueError("max_records_per_file must be a positive integer")

    missing = sorted(set(partition_columns) - set(df.columns))
    if missing:
        raise ValueError(
            "Processed DataFrame is missing partition column(s): " + ", ".join(missing)
        )

    expected_rows = df.count()
    if expected_rows == 0:
        raise ValueError("Refusing to write an empty processed taxi dataset")

    output = df.repartition(*partition_columns)
    (
        output.write.mode("overwrite")
        .option("compression", "snappy")
        .option("maxRecordsPerFile", max_records_per_file)
        .partitionBy(*partition_columns)
        .parquet(destination)
    )

    restored = spark.read.parquet(destination).select(*df.columns)
    if _schema_signature(restored) != _schema_signature(df):
        raise RuntimeError(
            "Processed Parquet read-back schema does not match the input schema: "
            f"expected {_schema_signature(df)}, got {_schema_signature(restored)}"
        )
    restored_rows = restored.count()
    if restored_rows != expected_rows:
        raise RuntimeError(
            "Processed Parquet read-back row count does not match the input: "
            f"expected {expected_rows}, got {restored_rows}"
        )

    return ProcessedWriteReport(
        path=destination,
        row_count=restored_rows,
        schema=restored.schema.simpleString(),
        partition_columns=partition_columns,
    )
