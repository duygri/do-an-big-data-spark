"""CLI for running equivalent taxi workloads on Spark and Pandas."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

from pyspark.sql import SparkSession

# Direct ``python scripts/...`` execution puts only ``scripts/`` on sys.path.
repository_root = str(Path(__file__).resolve().parents[1])
if repository_root not in sys.path:
    sys.path.insert(0, repository_root)

from src.aggregation.engine_benchmark import benchmark_spark_vs_pandas


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare NYC taxi aggregations on Spark and Pandas"
    )
    parser.add_argument(
        "--dataset",
        action="append",
        required=True,
        metavar="LABEL=PATH",
        help="Labeled CSV file, directory, or glob; repeat for each dataset size",
    )
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def _parse_dataset_argument(argument: str) -> tuple[str, str]:
    label, separator, path = argument.partition("=")
    if not separator or not label.strip() or not path.strip():
        raise ValueError(f"Dataset must use LABEL=PATH syntax: {argument!r}")
    return label.strip(), path.strip()


def main() -> None:
    args = parse_args()
    try:
        datasets = [_parse_dataset_argument(item) for item in args.dataset]
    except ValueError as error:
        raise SystemExit(str(error)) from error
    labels = [label for label, _ in datasets]
    if len(labels) != len(set(labels)):
        raise SystemExit("Dataset labels must be unique")

    args.output.mkdir(parents=True, exist_ok=True)
    startup_started = time.perf_counter()
    spark = (
        SparkSession.builder.appName("taxi-spark-vs-pandas-benchmark")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark_startup_seconds = time.perf_counter() - startup_started
    try:
        results = []
        for label, path in datasets:
            results.extend(
                benchmark_spark_vs_pandas(
                    [path],
                    spark,
                    label,
                    spark_startup_seconds=spark_startup_seconds,
                )
            )

        records = [asdict(result) for result in results]
        json_path = args.output / "engine_benchmark.json"
        json_path.write_text(
            json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        csv_path = args.output / "engine_benchmark.csv"
        columns = list(records[0]) if records else []
        with csv_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(records)
        print(f"Wrote {csv_path}")
        print(f"Wrote {json_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
