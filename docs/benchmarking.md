# Reproducible taxi benchmark guide

These standalone experiments are separate from the normal pipeline. They use the same NYC Yellow Taxi CSV files and the same taxi cleaning rules: remove rows missing pickup/dropoff timestamps or either location ID, then remove exact duplicates across the 18 source fields. Both engines interpret the source's local wall-clock timestamps in `America/New_York`. Pandas shifts nonexistent spring-forward times forward by the one-hour DST gap, matching Spark's CSV timestamp parsing before exact-row deduplication; ambiguous fall-back times retain their source wall-clock value.

## Spark versus Pandas (#15)

The command accepts one or more labeled datasets. Use a file, a directory containing CSV files, or a glob for each label. Example with representative sizes:

```powershell
python scripts/benchmark_spark_vs_pandas.py `
  --dataset 100mb=data/raw/nyc_taxi/yellow_tripdata_2020-01.csv `
  --dataset 1gb=data/raw/nyc_taxi/sample_1gb `
  --dataset 5gb=data/raw/nyc_taxi/sample_5gb `
  --output reports/generated/engine_benchmark
```

The same files run through both engines. The workload produces monthly fare/tip totals and averages, pickup/dropoff zone counts, descriptive distance/fare/tip statistics, and payment counts/shares. Spark uses the aggregation definitions from `src/aggregation/taxi.py`. Before writing `engine_benchmark.csv` and `engine_benchmark.json`, the command compares row keys and counts exactly, ordinary numeric outputs with relative tolerance `1e-10` and absolute tolerance `1e-8`, and approximate medians by rank around Pandas' exact lower middle value. The shared fingerprint identifies the pair of outputs that passed those checks; it does not claim bit-for-bit identical floating-point results.

The results include input bytes and retained rows, Spark session startup, read/clean time, aggregation time, total engine time, rows per second, peak RSS, Python/Spark/Pandas versions, and the comparison fingerprint. Spark read/clean timing includes CSV scan, required-field filtering, exact deduplication, and materializing the cleaned rows in a `DISK_ONLY` cache. Aggregation actions reuse that materialized input; the cache is released after collection. Pandas read/clean timing includes reading and materializing its cleaned frame. Spark startup is reported separately and is excluded from `total_seconds`. The Spark RSS sample includes the Python process tree and JVM. The Pandas sample omits the already-running idle Spark JVM, but includes the Python benchmark process tree.

Spark `percentile_approx` uses accuracy `10,000`; its median is accepted when its value's rank interval falls within `max(1, ceil(observation_count / 10,000))` positions of Pandas' exact lower-middle rank. Other numeric results use the documented relative and absolute tolerances to account for accumulation order.

## Spark persistence experiment (#13)

This command compares a baseline that does not persist the shared taxi input with a `DISK_ONLY` persisted run. It verifies matching output fingerprints and writes physical-plan evidence for each mode:

```powershell
python scripts/benchmark_spark_aggregation.py `
  --input data/gold/taxi_trips `
  --output reports/generated/aggregation_benchmark `
  --repetitions 3
```

Cache materialization is included in the persisted run's elapsed time. The result files contain timing, throughput, row counts, Spark version, shuffle partitions, storage level, and fingerprint; plan text is saved separately.

## Choosing datasets and reading results

Try roughly 100 MB, 1 GB, and 5 GB inputs, or the largest files that fit the machine. Keep the CSV files unchanged between engine runs. Do not treat one machine's timing or memory values as a CI threshold or universal speedup: Spark session startup, available cores, JVM heap, disk, concurrent load, and Pandas' in-memory representation all affect the result. Read startup separately from engine read/aggregation time, and compare fingerprints before comparing speeds.

CI uses small fixtures to check equivalent results and real Parquet round trips on Linux. Large benchmark data and result files should stay out of Git.
