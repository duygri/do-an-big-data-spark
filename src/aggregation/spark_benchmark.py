"""Standalone benchmark for reused Spark taxi aggregations."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, fields
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame

from src.aggregation.taxi import TaxiAggregationTables, aggregate_taxi_trips


@dataclass(frozen=True)
class AggregationBenchmarkResult:
    """Measurements and equivalence evidence for one benchmark mode."""

    mode: str
    elapsed_seconds: float
    input_rows: int
    table_row_counts: dict[str, int]
    fingerprint: str
    rows_per_second: float
    spark_version: str
    shuffle_partitions: int
    storage_level: str
    physical_plan: str


def _canonical_table_rows(frame: DataFrame) -> list[str]:
    return sorted(
        json.dumps(row.asDict(recursive=True), sort_keys=True, separators=(",", ":"), default=str)
        for row in frame.collect()
    )


def _fingerprint(tables: TaxiAggregationTables) -> tuple[dict[str, int], str]:
    canonical_tables: dict[str, list[str]] = {}
    row_counts: dict[str, int] = {}
    for field in fields(tables):
        name = field.name
        rows = _canonical_table_rows(getattr(tables, name))
        canonical_tables[name] = rows
        row_counts[name] = len(rows)
    encoded = json.dumps(canonical_tables, sort_keys=True, separators=(",", ":"))
    return row_counts, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def compare_aggregation_persistence(
    df: DataFrame, repetitions: int = 1
) -> list[AggregationBenchmarkResult]:
    """Compare uncached and DISK_ONLY-cached runs over the same taxi input.

    Cache materialization is included in the persisted elapsed time. Both modes
    execute the row-count and all eight aggregation actions in each repetition.
    """
    if repetitions < 1:
        raise ValueError("repetitions must be at least 1")

    spark = df.sparkSession
    modes = (("baseline", None), ("persisted", StorageLevel.DISK_ONLY))
    results: list[AggregationBenchmarkResult] = []
    fingerprints_by_mode: dict[str, str] = {}

    for repetition in range(repetitions):
        fingerprints_by_mode.clear()
        for mode, storage in modes:
            spark.catalog.clearCache()
            cached_df: DataFrame | None = None
            started = time.perf_counter()
            try:
                if storage is None:
                    measured_df = df
                    input_rows = measured_df.count()
                    storage_level = "NONE"
                else:
                    cached_df = df.persist(storage)
                    measured_df = cached_df
                    input_rows = measured_df.count()
                    storage_level = "DISK_ONLY"

                tables = aggregate_taxi_trips(measured_df)
                table_row_counts, fingerprint = _fingerprint(tables)
                physical_plan = (
                    tables.trips_by_month._jdf.queryExecution().executedPlan().toString()
                )
                elapsed = time.perf_counter() - started
                results.append(
                    AggregationBenchmarkResult(
                        mode=mode,
                        elapsed_seconds=elapsed,
                        input_rows=input_rows,
                        table_row_counts=table_row_counts,
                        fingerprint=fingerprint,
                        rows_per_second=(input_rows / elapsed if elapsed > 0 else 0.0),
                        spark_version=spark.version,
                        shuffle_partitions=int(
                            spark.conf.get("spark.sql.shuffle.partitions")
                        ),
                        storage_level=storage_level,
                        physical_plan=physical_plan,
                    )
                )
                fingerprints_by_mode[mode] = fingerprint
            finally:
                if cached_df is not None:
                    cached_df.unpersist(blocking=True)
                spark.catalog.clearCache()

        if fingerprints_by_mode["baseline"] != fingerprints_by_mode["persisted"]:
            raise RuntimeError(
                "Taxi aggregation results differ between baseline and persisted modes "
                f"in repetition {repetition + 1}"
            )

    return results
