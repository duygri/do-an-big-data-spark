"""Verified Gold and human-readable exports for taxi aggregations."""

from __future__ import annotations

import csv
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame

from src.aggregation.taxi import TaxiAggregationTables


@dataclass(frozen=True)
class AggregationExportReport:
    """Paths and verified row counts for exported aggregation assets."""

    table_row_counts: dict[str, int]
    parquet_paths: dict[str, str]
    csv_paths: dict[str, str]
    markdown_path: str
    chart_paths: dict[str, str]


def _collect_rows(frame: DataFrame) -> list[dict[str, Any]]:
    """Collect only the already-aggregated, report-sized DataFrame."""
    return [row.asDict(recursive=True) for row in frame.collect()]


def _write_csv(frame: DataFrame, rows: list[dict[str, Any]], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(frame.columns)
        writer.writerows([[row.get(column) for column in frame.columns] for row in rows])


def _display_zone(row: dict[str, Any]) -> str:
    if row.get("zone_label"):
        return str(row["zone_label"])
    location_id = row.get("location_id")
    return "Unknown/Invalid" if location_id is None else str(location_id)


def _save_monthly_chart(rows: list[dict[str, Any]], destination: Path) -> None:
    ordered = sorted(rows, key=lambda row: (row.get("pickup_year") or 0, row.get("pickup_month") or 0))
    labels = [f"{row['pickup_year']}-{row['pickup_month']:02d}" for row in ordered]
    values = [row["trip_count"] for row in ordered]
    fig, axis = plt.subplots(figsize=(10, 4.5))
    if rows:
        axis.plot(labels, values, marker="o")
        axis.tick_params(axis="x", rotation=45)
        axis.set_ylabel("Trips")
        axis.set_xlabel("Pickup month")
        axis.set_title("Taxi trips by month")
    else:
        axis.text(0.5, 0.5, "No monthly trip data", ha="center", va="center")
        axis.set_axis_off()
    fig.tight_layout()
    fig.savefig(destination, dpi=140)
    plt.close(fig)


def _save_zone_chart(
    rows: list[dict[str, Any]], destination: Path, title: str, top_n: int
) -> None:
    ordered = sorted(
        rows,
        key=lambda row: (
            row.get("trip_rank") if row.get("trip_rank") is not None else 2**31,
            _display_zone(row),
        ),
    )[:top_n]
    labels = [_display_zone(row) for row in reversed(ordered)]
    values = [row["trip_count"] for row in reversed(ordered)]
    fig, axis = plt.subplots(figsize=(9, max(3, min(8, 0.35 * len(labels) + 1.5))))
    if ordered:
        axis.barh(labels, values)
        axis.set_xlabel("Trips")
        axis.set_title(title)
    else:
        axis.text(0.5, 0.5, "No zone data", ha="center", va="center")
        axis.set_axis_off()
    fig.tight_layout()
    fig.savefig(destination, dpi=140)
    plt.close(fig)


def _save_payment_chart(rows: list[dict[str, Any]], destination: Path) -> None:
    ordered = sorted(rows, key=lambda row: str(row.get("payment_type") or "unknown"))
    labels = [str(row["payment_type"]) for row in ordered]
    values = [row["share_percent"] for row in ordered]
    fig, axis = plt.subplots(figsize=(7, 4.5))
    if rows:
        axis.bar(labels, values)
        axis.set_ylabel("Share of trips (%)")
        axis.set_title("Payment mix")
    else:
        axis.text(0.5, 0.5, "No payment data", ha="center", va="center")
        axis.set_axis_off()
    fig.tight_layout()
    fig.savefig(destination, dpi=140)
    plt.close(fig)


def _date_coverage(month_rows: list[dict[str, Any]]) -> str:
    periods = sorted(
        (row["pickup_year"], row["pickup_month"])
        for row in month_rows
        if row.get("pickup_year") is not None and row.get("pickup_month") is not None
    )
    if not periods:
        return "No pickup year/month values available"
    first, last = periods[0], periods[-1]
    return f"{first[0]}-{first[1]:02d} to {last[0]}-{last[1]:02d}"


def _write_markdown(
    rows_by_table: dict[str, list[dict[str, Any]]], destination: Path
) -> None:
    month_rows = rows_by_table["trips_by_month"]
    total_trips = sum(row["trip_count"] for row in month_rows)
    comparison = rows_by_table["same_month_comparison"]
    zone_rows = rows_by_table["pickup_zones"] + rows_by_table["dropoff_zones"]
    has_zone_labels = any(
        row.get("zone_label") not in (None, "", "Unknown/Invalid")
        for row in zone_rows
    )
    lines = [
        "# Taxi aggregation report",
        "",
        f"- Pickup date coverage: {_date_coverage(month_rows)}",
        f"- Trips represented in monthly table: {total_trips:,}",
        "- Pickup time zone: `America/New_York` (Spark session time zone).",
        "- Negative distance, fare, and tip values are retained; null metric values are excluded only from that metric's statistic.",
        "- Null or nonpositive location IDs are grouped as `Unknown/Invalid`; trips remain in the other summaries.",
        "",
        "## Table grains and metric definitions",
        "",
        "- `trips_by_month`: one row per pickup year/month; trip count, fare/tip totals and means.",
        "- `trips_by_weekday`: one row per Spark weekday (Sunday = 1); trip count.",
        "- `trips_by_hour`: one row per local pickup hour; trip count.",
        "- `pickup_zones` and `dropoff_zones`: one row per location ID with available labels, trip count, and deterministic rank.",
        "- `trip_metric_stats`: non-null observation count, min, mean, approximate median (Spark accuracy 10,000), max, and sample standard deviation for distance, fare, and tip.",
        "- `payment_mix`: trip count and share of all trips; null payment type is included as `unknown` in the denominator.",
    ]
    if not has_zone_labels:
        lines.append(
            "- Zone lookup labels are unavailable; results show location IDs instead of names."
        )
    if comparison:
        included_months = sorted({row["pickup_month"] for row in comparison})
        first_month, last_month = included_months[0], included_months[-1]
        month_unit = "month" if len(included_months) == 1 else "months"
        lines.extend(
            [
                f"- 2019 period: 2019-{first_month:02d} to 2019-{last_month:02d} ({len(included_months)} {month_unit}).",
                f"- 2020 period: 2020-{first_month:02d} to 2020-{last_month:02d} ({len(included_months)} {month_unit}).",
                f"- Months included in comparison: {', '.join(f'{month:02d}' for month in included_months)} ({len(included_months)} {month_unit}).",
                "- `same_month_comparison`: only overlapping months 1–6 in 2019 and 2020; deltas are 2020 minus 2019, and percent deltas are null when the 2019 value is zero.",
                "- Year-over-year values are descriptive comparisons, not causal conclusions.",
            ]
        )
    else:
        lines.extend(
            [
                "- `same_month_comparison`: insufficient overlapping months from January through June in both 2019 and 2020; the comparison table is empty.",
                "- Year-over-year values, when available, are descriptive comparisons, not causal conclusions.",
            ]
        )
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _same_schema(left: DataFrame, right: DataFrame) -> bool:
    return [(field.name, field.dataType) for field in left.schema.fields] == [
        (field.name, field.dataType) for field in right.schema.fields
    ]


def export_taxi_aggregations(
    tables: TaxiAggregationTables,
    gold_path: str,
    reports_path: str,
    top_n: int = 10,
) -> AggregationExportReport:
    """Write each Gold table, verify Parquet read-back, and render small reports."""
    if top_n <= 0:
        raise ValueError("top_n must be greater than zero")

    gold_root = Path(gold_path)
    reports_root = Path(reports_path)
    csv_root = reports_root / "csv"
    chart_root = reports_root / "charts"
    csv_root.mkdir(parents=True, exist_ok=True)
    chart_root.mkdir(parents=True, exist_ok=True)

    table_row_counts: dict[str, int] = {}
    parquet_paths: dict[str, str] = {}
    csv_paths: dict[str, str] = {}
    rows_by_table: dict[str, list[dict[str, Any]]] = {}

    for field in fields(tables):
        name = field.name
        frame = getattr(tables, name)
        path = gold_root / name
        rows = _collect_rows(frame)
        source_count = len(rows)
        frame.write.mode("overwrite").option("compression", "snappy").parquet(str(path))
        restored = frame.sparkSession.read.parquet(str(path))
        restored_count = restored.count()
        if restored_count != source_count or not _same_schema(frame, restored):
            raise RuntimeError(
                f"Gold table {name} failed Parquet read-back verification: "
                f"source rows={source_count}, restored rows={restored_count}, "
                f"same schema={_same_schema(frame, restored)}"
            )

        table_row_counts[name] = source_count
        parquet_paths[name] = str(path)
        csv_path = csv_root / f"{name}.csv"
        _write_csv(frame, rows, csv_path)
        csv_paths[name] = str(csv_path)
        rows_by_table[name] = rows

    markdown_path = reports_root / "taxi_aggregation_report.md"
    _write_markdown(rows_by_table, markdown_path)

    chart_files = {
        "monthly_trips": chart_root / "monthly_trips.png",
        "top_pickup_zones": chart_root / "top_pickup_zones.png",
        "top_dropoff_zones": chart_root / "top_dropoff_zones.png",
        "payment_mix": chart_root / "payment_mix.png",
    }
    _save_monthly_chart(rows_by_table["trips_by_month"], chart_files["monthly_trips"])
    _save_zone_chart(
        rows_by_table["pickup_zones"], chart_files["top_pickup_zones"],
        "Top pickup zones", top_n,
    )
    _save_zone_chart(
        rows_by_table["dropoff_zones"], chart_files["top_dropoff_zones"],
        "Top dropoff zones", top_n,
    )
    _save_payment_chart(rows_by_table["payment_mix"], chart_files["payment_mix"])

    return AggregationExportReport(
        table_row_counts=table_row_counts,
        parquet_paths=parquet_paths,
        csv_paths=csv_paths,
        markdown_path=str(markdown_path),
        chart_paths={name: str(path) for name, path in chart_files.items()},
    )
