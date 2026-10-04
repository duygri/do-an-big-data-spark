# Taxi aggregations and report outputs

The normal pipeline creates these outputs after feature engineering and the processed taxi Parquet write:

- Gold Parquet tables: `paths.aggregations` (default `data/gold/aggregations`). Each table is written under its own directory and read back to verify column names, Spark data types, and row count.
- Headered CSV files: `paths.analysis_reports/csv/` (default `reports/generated/taxi_aggregations/csv/`), one file per Gold table.
- Human-readable summary: `paths.analysis_reports/taxi_aggregation_report.md`.
- Charts: `paths.analysis_reports/charts/`.

Set `aggregation.top_n` to control the number of pickup/dropoff zones shown in charts. Gold and CSV retain every zone row. To rerun all stages, prepare the input files and config, then run from the repository root:

```powershell
python -m src.pipeline.main --config configs/config.yaml
```

The sample config sets the output paths explicitly. If omitted, aggregation output defaults to `{paths.gold}/aggregations`, the analysis report directory defaults to `{paths.reports}/taxi_aggregations`, and `aggregation.top_n` defaults to `10`.

## Gold table contract

| Table | One row per | Output columns |
| --- | --- | --- |
| `trips_by_month` | pickup year and month | `pickup_year`, `pickup_month`, `trip_count`, `total_fare_amount`, `avg_fare_amount`, `total_tip_amount`, `avg_tip_amount` |
| `trips_by_weekday` | Spark weekday | `pickup_day_of_week`, `trip_count` |
| `trips_by_hour` | local pickup hour | `pickup_hour`, `trip_count` |
| `pickup_zones`, `dropoff_zones` | normalized location ID | `location_id`, `zone_label`, `borough`, `trip_count`, `trip_rank` |
| `trip_metric_stats` | metric (`trip_distance`, `fare_amount`, `tip_amount`) | `metric_name`, `observation_count`, `min_value`, `mean_value`, `median_approx`, `max_value`, `stddev` |
| `payment_mix` | payment code, including unknown | `payment_type`, `trip_count`, `share_percent` |
| `same_month_comparison` | overlapping pickup month from 1 through 6 | `pickup_month`, followed by each of `trip_count`, `total_fare_amount`, `avg_fare_amount`, `total_tip_amount`, `avg_tip_amount`, `avg_trip_distance` with `_2019`, `_2020`, `_delta`, and `_pct_delta` columns |

The comparison delta is 2020 minus 2019. A percentage delta is null when the 2019 value is zero. Only months present in both years are emitted; an empty comparison is valid and is called out in the Markdown report. The comparison is descriptive and does not establish causality.

## Data handling rules

- Pickup year/month/hour/weekday are the feature-engineered fields under Spark session timezone `America/New_York`; Spark weekday numbering is Sunday = 1.
- Negative distance, fare, and tip values remain in the tables. Each metric's descriptive-statistic observation count excludes only nulls for that metric.
- Median is Spark `percentile_approx` at accuracy `10,000`; standard deviation is the sample standard deviation.
- Null and nonpositive location IDs share an `Unknown/Invalid` zone row. These trips remain in time, metric, and payment summaries.
- Zone labels and boroughs are included when lookup enrichment is available. Without lookup labels, the report states that zone results show IDs.
- Null `payment_type` is counted as `unknown`; payment shares use all trips, including that bucket, as the denominator.
- Parquet is the queryable Gold source. The report collects only aggregate rows, never trip-scale data.

## Generated charts

The four PNG charts use aggregated rows only:

- `monthly_trips.png`
- `top_pickup_zones.png`
- `top_dropoff_zones.png`
- `payment_mix.png`

The two zone charts use `aggregation.top_n`, sorted by descending trip count and then location ID. This limit affects chart display only.

## Spark persistence benchmark

The persistence benchmark is a standalone experiment, not part of a normal pipeline run. It compares uncached aggregations with one `DISK_ONLY` input cache reused across all table actions. Cache materialization is included in the persisted elapsed time; a deterministic output fingerprint must match between modes. The command writes CSV/JSON measurements and a physical-plan text file for each mode and repetition:

```powershell
python scripts/benchmark_spark_aggregation.py `
  --input data/gold/taxi_trips `
  --output reports/generated/aggregation_benchmark `
  --repetitions 3
```

The measurement is machine- and dataset-dependent. CI checks result equivalence on a small fixture and does not require a runtime speedup.
