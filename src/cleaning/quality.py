"""Bounded, machine-readable quality metrics for NYC yellow taxi data."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, isnan, max as spark_max, min as spark_min, sum as spark_sum, when
from pyspark.sql.types import DoubleType

from src.cleaning.taxi import REQUIRED_FIELDS, validate_schema

NUMERIC_FIELDS = (
    "passenger_count", "trip_distance", "fare_amount", "total_amount",
    "tip_amount", "congestion_surcharge",
)


def null_counts(df: DataFrame) -> dict[str, int]:
    """Count nulls (and NaN in doubles) in one Spark aggregation."""
    validate_schema(df)
    expressions = []
    for field in df.schema.fields:
        missing = col(field.name).isNull()
        if isinstance(field.dataType, DoubleType):
            missing = missing | isnan(col(field.name))
        expressions.append(spark_sum(when(missing, 1).otherwise(0)).alias(field.name))
    row = df.agg(*expressions).first()
    return {name: int(row[name] or 0) for name in df.columns}


def quality_report(df: DataFrame, stage_counts: dict[str, int], before_nulls: dict[str, int]) -> dict:
    """Summarize schema, nulls, statistics, and warning conditions."""
    validate_schema(df)
    after_nulls = null_counts(df)
    stats = df.agg(*[
        function(col(name)).alias(f"{name}__{label}")
        for name in NUMERIC_FIELDS
        for label, function in (("min", spark_min), ("max", spark_max), ("avg", avg))
    ]).first().asDict()
    numeric_stats = {
        name: {label: stats[f"{name}__{label}"] for label in ("min", "max", "avg")}
        for name in NUMERIC_FIELDS
    }
    checks = {
        "pickup_after_dropoff": col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"),
        "nonpositive_pickup_location": col("PULocationID") <= 0,
        "nonpositive_dropoff_location": col("DOLocationID") <= 0,
        "negative_passenger_count": col("passenger_count") < 0,
        "negative_trip_distance": col("trip_distance") < 0,
        "negative_fare_amount": col("fare_amount") < 0,
        "negative_total_amount": col("total_amount") < 0,
        "unexpected_store_and_fwd_flag": (
            col("store_and_fwd_flag").isNotNull()
            & ~col("store_and_fwd_flag").isin("y", "n")
        ),
    }
    warning_row = df.agg(*[
        count(when(condition, 1)).alias(name) for name, condition in checks.items()
    ]).first()
    warnings = {name: int(warning_row[name]) for name in checks}
    examples = {}
    for name, condition in checks.items():
        if warnings[name]:
            examples[name] = [
                row.asDict(recursive=True)
                for row in df.where(condition).select(
                    "tpep_pickup_datetime", "PULocationID", "DOLocationID",
                    "trip_distance", "fare_amount", "total_amount",
                    "store_and_fwd_flag",
                ).limit(2).collect()
            ]
    return {
        "schema": df.schema.simpleString(),
        "stage_counts": stage_counts,
        "removed_missing_required": stage_counts["bronze"] - stage_counts["after_missing"],
        "removed_exact_duplicates": stage_counts["after_missing"] - stage_counts["after_deduplication"],
        "required_fields": list(REQUIRED_FIELDS),
        "null_counts_before": before_nulls,
        "null_counts_after": after_nulls,
        "numeric_statistics": numeric_stats,
        "warnings": warnings,
        "warning_examples": examples,
        "key_uniqueness": "No stable trip ID; only exact full-row duplicates are removed.",
    }


def write_report(report: dict, directory: str) -> None:
    """Write a JSON artifact and concise Markdown summary outside tracked data."""
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    (target / "taxi_quality.json").write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
    )
    counts = report["stage_counts"]
    lines = [
        "# NYC yellow taxi data quality",
        "",
        f"Rows in Bronze: {counts['bronze']}",
        f"Rows after required-field cleaning: {counts['after_missing']}",
        f"Rows after exact deduplication: {counts['after_deduplication']}",
        f"Rows in Silver: {counts['silver']}",
        "",
        "## Warnings retained in Silver",
        "",
    ]
    lines.extend(f"- {name}: {value}" for name, value in report["warnings"].items())
    lines.extend(["", "## Null counts after cleaning", ""])
    lines.extend(f"- {name}: {value}" for name, value in report["null_counts_after"].items())
    (target / "taxi_quality.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
