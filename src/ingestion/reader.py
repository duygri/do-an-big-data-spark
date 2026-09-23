"""Reusable Spark readers for local CSV and JSON input."""

import glob

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_raw(
    spark: SparkSession,
    path: str,
    *,
    file_format: str,
    schema: StructType | None = None,
    header: bool = True,
    options: dict[str, str] | None = None,
) -> DataFrame:
    """Read a local file, directory, or glob with consistent options."""
    fmt = file_format.lower()
    if fmt not in {"csv", "json"}:
        raise ValueError(f"Unsupported input format: {file_format}")
    if not path:
        raise ValueError("Input path is required")
    if not glob.glob(path):
        raise FileNotFoundError(f"Input path has no matches: {path}")
    reader = spark.read.format(fmt)
    if schema is not None:
        reader = reader.schema(schema)
    if fmt == "csv":
        reader = reader.option("header", str(header).lower()).option("mode", "FAILFAST")
        reader = reader.option("enforceSchema", "false")
        reader = reader.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    if options:
        reader = reader.options(**options)
    return reader.load(path)
