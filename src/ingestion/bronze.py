"""Persist and verify the raw Bronze Parquet layer."""

from pyspark.sql import DataFrame, SparkSession


def write_bronze(df: DataFrame, spark: SparkSession, path: str) -> int:
    """Overwrite Bronze with Snappy Parquet and verify its row count/schema."""
    source_count = df.count()
    if source_count == 0:
        raise ValueError("Input contains no rows")
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    restored = spark.read.parquet(path)
    result_count = restored.count()
    if result_count != source_count or restored.schema != df.schema:
        raise RuntimeError("Bronze read-back does not match the source")
    return result_count
