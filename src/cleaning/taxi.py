"""Cleaning rules for the 18-column NYC yellow taxi dataset."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, when

from src.ingestion.schema import YELLOW_TAXI_SCHEMA

REQUIRED_FIELDS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
)


def validate_schema(df: DataFrame) -> None:
    """Refuse unrelated inputs before applying taxi-specific rules."""
    if df.schema != YELLOW_TAXI_SCHEMA:
        raise ValueError(
            "Yellow taxi schema mismatch: expected the 18 fields and types "
            "defined in src.ingestion.schema.YELLOW_TAXI_SCHEMA"
        )


def remove_missing_required(df: DataFrame) -> DataFrame:
    """Keep optional nulls for downstream analysis and quality reporting."""
    validate_schema(df)
    return df.na.drop(subset=list(REQUIRED_FIELDS))


def remove_exact_duplicates(df: DataFrame) -> DataFrame:
    """The source has no stable trip ID, so compare all 18 source fields."""
    validate_schema(df)
    return df.dropDuplicates()


def normalize_taxi(df: DataFrame) -> DataFrame:
    """Normalize the only text field; retain timestamp/mile/USD units."""
    validate_schema(df)
    flag = trim(col("store_and_fwd_flag"))
    return df.withColumn(
        "store_and_fwd_flag",
        when(flag == "", None).otherwise(lower(flag)),
    )
