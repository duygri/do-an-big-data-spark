"""Run the standalone Spark aggregation persistence benchmark."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path

from pyspark.sql import SparkSession

# Direct ``python scripts/...`` execution puts only ``scripts/`` on sys.path.
repository_root = str(Path(__file__).resolve().parents[1])
if repository_root not in sys.path:
    sys.path.insert(0, repository_root)

from src.aggregation.spark_benchmark import compare_aggregation_persistence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare baseline and DISK_ONLY-cached taxi aggregations"
    )
    parser.add_argument("--input", required=True, type=Path, help="Processed taxi Parquet path")
    parser.add_argument("--output", required=True, type=Path, help="Benchmark output directory")
    parser.add_argument("--repetitions", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.repetitions < 1:
        raise SystemExit("--repetitions must be at least 1")
    args.output.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName("taxi-aggregation-persistence-benchmark")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    try:
        trips = spark.read.parquet(str(args.input))
        results = compare_aggregation_persistence(trips, repetitions=args.repetitions)
        records = []
        for index, result in enumerate(results):
            record = asdict(result)
            physical_plan = record.pop("physical_plan")
            repetition = index // 2 + 1
            plan_name = f"physical_plan_{result.mode}_run_{repetition}.txt"
            (args.output / plan_name).write_text(physical_plan + "\n", encoding="utf-8")
            record["repetition"] = repetition
            record["physical_plan_file"] = plan_name
            records.append(record)

        json_path = args.output / "aggregation_benchmark.json"
        json_path.write_text(
            json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        csv_path = args.output / "aggregation_benchmark.csv"
        columns = [
            "repetition", "mode", "elapsed_seconds", "input_rows",
            "table_row_counts", "fingerprint", "rows_per_second", "spark_version",
            "shuffle_partitions", "storage_level", "physical_plan_file",
        ]
        with csv_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            for record in records:
                row = dict(record)
                row["table_row_counts"] = json.dumps(row["table_row_counts"], sort_keys=True)
                writer.writerow(row)
        print(f"Wrote {csv_path}")
        print(f"Wrote {json_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
