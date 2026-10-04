"""Reproducible Spark/Pandas comparison for the taxi Gold workload."""

from __future__ import annotations

import glob
import hashlib
import json
import os
import platform
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import psutil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

from src.aggregation.taxi import TaxiAggregationTables, aggregate_taxi_trips
from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.transformation.taxi import engineer_taxi_features

REQUIRED_FIELDS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
)
FINGERPRINT_TABLES = (
    "trips_by_month",
    "pickup_zones",
    "dropoff_zones",
    "trip_metric_stats",
    "payment_mix",
)


@dataclass(frozen=True)
class EngineBenchmarkResult:
    """Timing, resource, environment, and equivalence evidence for one engine."""

    engine: str
    dataset_label: str
    input_bytes: int
    input_rows: int
    spark_startup_seconds: float
    read_seconds: float
    aggregation_seconds: float
    total_seconds: float
    rows_per_second: float
    peak_rss_mb: float
    python_version: str
    spark_version: str
    pandas_version: str
    fingerprint: str


class _PeakRssSampler:
    """Sample process-tree RSS; optionally omit the already-running Spark JVM."""

    def __init__(self, *, exclude_java: bool = False, interval_seconds: float = 0.05):
        self.exclude_java = exclude_java
        self.interval_seconds = interval_seconds
        self.peak_bytes = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _sample(self) -> None:
        try:
            root = psutil.Process(os.getpid())
            pending = [root]
            total = 0
            while pending:
                process = pending.pop()
                try:
                    name = process.name().lower()
                    if self.exclude_java and name.startswith("java"):
                        continue
                    total += process.memory_info().rss
                    pending.extend(process.children())
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            with self._lock:
                self.peak_bytes = max(self.peak_bytes, total)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def __enter__(self) -> "_PeakRssSampler":
        self._sample()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self._sample()

    @property
    def peak_mb(self) -> float:
        return self.peak_bytes / (1024.0 * 1024.0)


def _expand_input_paths(input_paths: Sequence[str]) -> list[str]:
    matches: list[str] = []
    for input_path in input_paths:
        path = Path(input_path)
        if path.is_dir():
            matches.extend(str(item) for item in sorted(path.rglob("*.csv")) if item.is_file())
        else:
            expanded = glob.glob(input_path)
            matches.extend(expanded or ([str(path)] if path.is_file() else []))
    files = sorted({str(Path(item).resolve()) for item in matches if Path(item).is_file()})
    if not files:
        raise FileNotFoundError("No taxi CSV inputs matched the provided paths")
    return files


def _canonical_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except ValueError:
            pass
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return round(value, 8)
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ")
    return value


def _fingerprint_records(tables: dict[str, list[dict[str, Any]]]) -> str:
    canonical: dict[str, list[dict[str, Any]]] = {}
    for table_name, rows in tables.items():
        normalized = [
            {key: _canonical_value(value) for key, value in row.items()}
            for row in rows
        ]
        canonical[table_name] = sorted(
            normalized,
            key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
        )
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _spark_fingerprint(tables: TaxiAggregationTables) -> str:
    rows: dict[str, list[dict[str, Any]]] = {}
    for name in FINGERPRINT_TABLES:
        rows[name] = [row.asDict(recursive=True) for row in getattr(tables, name).collect()]
    return _fingerprint_records(rows)


def _pandas_clean(frame: pd.DataFrame) -> pd.DataFrame:
    expected = YELLOW_TAXI_SCHEMA.fieldNames()
    missing = sorted(set(expected) - set(frame.columns))
    if missing:
        raise ValueError("Taxi CSV is missing required schema columns: " + ", ".join(missing))
    frame = frame[expected].copy()
    for field in YELLOW_TAXI_SCHEMA.fields:
        name = field.name
        if isinstance(field.dataType, TimestampType):
            frame[name] = pd.to_datetime(frame[name], errors="coerce")
        elif isinstance(field.dataType, IntegerType):
            frame[name] = pd.to_numeric(frame[name], errors="coerce").astype("Int64")
        elif isinstance(field.dataType, DoubleType):
            frame[name] = pd.to_numeric(frame[name], errors="coerce").astype("float64")
        elif isinstance(field.dataType, StringType):
            frame[name] = frame[name].astype("string")
    frame = frame.dropna(subset=list(REQUIRED_FIELDS))
    return frame.drop_duplicates().reset_index(drop=True)


def _pandas_monthly(frame: pd.DataFrame) -> list[dict[str, Any]]:
    pickup = frame["tpep_pickup_datetime"]
    data = frame.assign(pickup_year=pickup.dt.year, pickup_month=pickup.dt.month)
    grouped = data.groupby(["pickup_year", "pickup_month"], dropna=False, sort=True)
    rows = []
    for (year, month), group in grouped:
        rows.append(
            {
                "pickup_year": year,
                "pickup_month": month,
                "trip_count": len(group),
                "total_fare_amount": group["fare_amount"].sum(min_count=1),
                "avg_fare_amount": group["fare_amount"].mean(),
                "total_tip_amount": group["tip_amount"].sum(min_count=1),
                "avg_tip_amount": group["tip_amount"].mean(),
            }
        )
    return rows


def _pandas_zones(frame: pd.DataFrame, source_column: str) -> list[dict[str, Any]]:
    counts: dict[int | None, int] = {}
    for value in pd.to_numeric(frame[source_column], errors="coerce"):
        location_id = None if pd.isna(value) or value <= 0 else int(value)
        counts[location_id] = counts.get(location_id, 0) + 1
    ordered = sorted(
        counts.items(),
        key=lambda item: (-item[1], item[0] is None, item[0] if item[0] is not None else 0),
    )
    return [
        {
            "location_id": location_id,
            "zone_label": "Unknown/Invalid" if location_id is None else None,
            "borough": None,
            "trip_count": count,
            "trip_rank": rank,
        }
        for rank, (location_id, count) in enumerate(ordered, start=1)
    ]


def _pandas_metric_stats(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for metric in ("trip_distance", "fare_amount", "tip_amount"):
        values = frame[metric].dropna()
        rows.append(
            {
                "metric_name": metric,
                "observation_count": int(values.count()),
                "min_value": values.min() if len(values) else None,
                "mean_value": values.mean() if len(values) else None,
                "median_approx": values.quantile(0.5, interpolation="lower") if len(values) else None,
                "max_value": values.max() if len(values) else None,
                "stddev": values.std(ddof=1) if len(values) > 1 else None,
            }
        )
    return rows


def _pandas_payment_mix(frame: pd.DataFrame) -> list[dict[str, Any]]:
    payment = [
        "unknown" if pd.isna(value) else str(int(value))
        for value in frame["payment_type"]
    ]
    counts = pd.Series(payment, dtype="string").value_counts(dropna=False).to_dict()
    total = len(frame)
    return [
        {
            "payment_type": payment_type,
            "trip_count": int(count),
            "share_percent": count * 100.0 / total if total else 0.0,
        }
        for payment_type, count in sorted(counts.items(), key=lambda item: item[0])
    ]


def _pandas_fingerprint(frame: pd.DataFrame) -> str:
    return _fingerprint_records(
        {
            "trips_by_month": _pandas_monthly(frame),
            "pickup_zones": _pandas_zones(frame, "PULocationID"),
            "dropoff_zones": _pandas_zones(frame, "DOLocationID"),
            "trip_metric_stats": _pandas_metric_stats(frame),
            "payment_mix": _pandas_payment_mix(frame),
        }
    )


def _result(
    *,
    engine: str,
    dataset_label: str,
    input_bytes: int,
    input_rows: int,
    spark_startup_seconds: float,
    read_seconds: float,
    aggregation_seconds: float,
    total_seconds: float,
    peak_rss_mb: float,
    spark_version: str,
    fingerprint: str,
) -> EngineBenchmarkResult:
    return EngineBenchmarkResult(
        engine=engine,
        dataset_label=dataset_label,
        input_bytes=input_bytes,
        input_rows=input_rows,
        spark_startup_seconds=spark_startup_seconds,
        read_seconds=read_seconds,
        aggregation_seconds=aggregation_seconds,
        total_seconds=total_seconds,
        rows_per_second=(input_rows / total_seconds if total_seconds > 0 else 0.0),
        peak_rss_mb=peak_rss_mb,
        python_version=platform.python_version(),
        spark_version=spark_version,
        pandas_version=pd.__version__,
        fingerprint=fingerprint,
    )


def benchmark_spark_vs_pandas(
    input_paths: Sequence[str],
    spark: SparkSession,
    dataset_label: str,
    *,
    spark_startup_seconds: float = 0.0,
) -> list[EngineBenchmarkResult]:
    """Measure equivalent taxi aggregations in Spark and Pandas.

    Required-null filtering and exact-row deduplication match the pipeline's
    cleaning rules. Spark table definitions come directly from Task 1. The
    Spark RSS sample includes its JVM; the Pandas sample excludes that idle JVM.
    """
    if not dataset_label:
        raise ValueError("dataset_label is required")
    files = _expand_input_paths(input_paths)
    input_bytes = sum(Path(path).stat().st_size for path in files)
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")

    spark_total_started = time.perf_counter()
    with _PeakRssSampler() as spark_memory:
        read_started = time.perf_counter()
        raw = (
            spark.read.option("header", True)
            .option("mode", "FAILFAST")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(YELLOW_TAXI_SCHEMA)
            .csv(files)
        )
        cleaned = raw.na.drop(subset=list(REQUIRED_FIELDS)).dropDuplicates()
        input_rows = cleaned.count()
        spark_read_seconds = time.perf_counter() - read_started

        aggregation_started = time.perf_counter()
        featured = engineer_taxi_features(cleaned)
        tables = aggregate_taxi_trips(featured)
        spark_fingerprint = _spark_fingerprint(tables)
        spark_aggregation_seconds = time.perf_counter() - aggregation_started
        spark_total_seconds = time.perf_counter() - spark_total_started
    spark_result = _result(
        engine="spark",
        dataset_label=dataset_label,
        input_bytes=input_bytes,
        input_rows=input_rows,
        spark_startup_seconds=spark_startup_seconds,
        read_seconds=spark_read_seconds,
        aggregation_seconds=spark_aggregation_seconds,
        total_seconds=spark_total_seconds,
        peak_rss_mb=spark_memory.peak_mb,
        spark_version=spark.version,
        fingerprint=spark_fingerprint,
    )

    pandas_total_started = time.perf_counter()
    with _PeakRssSampler(exclude_java=True) as pandas_memory:
        read_started = time.perf_counter()
        raw_frames = [
            pd.read_csv(path, keep_default_na=True, low_memory=False) for path in files
        ]
        pandas_raw = pd.concat(raw_frames, ignore_index=True)
        pandas_cleaned = _pandas_clean(pandas_raw)
        pandas_rows = len(pandas_cleaned)
        pandas_read_seconds = time.perf_counter() - read_started

        aggregation_started = time.perf_counter()
        pandas_fingerprint = _pandas_fingerprint(pandas_cleaned)
        pandas_aggregation_seconds = time.perf_counter() - aggregation_started
        pandas_total_seconds = time.perf_counter() - pandas_total_started
    pandas_result = _result(
        engine="pandas",
        dataset_label=dataset_label,
        input_bytes=input_bytes,
        input_rows=pandas_rows,
        spark_startup_seconds=0.0,
        read_seconds=pandas_read_seconds,
        aggregation_seconds=pandas_aggregation_seconds,
        total_seconds=pandas_total_seconds,
        peak_rss_mb=pandas_memory.peak_mb,
        spark_version=spark.version,
        fingerprint=pandas_fingerprint,
    )

    if spark_result.input_rows != pandas_result.input_rows:
        raise RuntimeError(
            "Spark and Pandas retained different taxi row counts: "
            f"Spark={spark_result.input_rows}, Pandas={pandas_result.input_rows}"
        )
    if spark_result.fingerprint != pandas_result.fingerprint:
        raise RuntimeError(
            "Spark and Pandas taxi aggregation fingerprints differ for "
            f"{dataset_label}: Spark={spark_result.fingerprint}, "
            f"Pandas={pandas_result.fingerprint}"
        )
    return [spark_result, pandas_result]
