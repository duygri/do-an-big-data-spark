# Reproducible taxi benchmark guide

These standalone experiments are separate from the normal pipeline. They use the same NYC Yellow Taxi CSV files and the same taxi cleaning rules: remove rows missing pickup/dropoff timestamps or either location ID, then remove exact duplicates across the 18 source fields. The Spark session timezone and Pandas pickup-time interpretation are `America/New_York`.

## Spark versus Pandas (#15)

The command accepts one or more labeled datasets. Use a file, a directory containing CSV files, or a glob for each label. Example with representative sizes:

```powershell
python scripts/benchmark_spark_vs_pandas.py `
  --dataset 100mb=data/raw/nyc_taxi/yellow_tripdata_2019-01.csv `
  --dataset 1gb=data/raw/nyc_taxi/sample_1gb `
  --dataset 5gb=data/raw/nyc_taxi/sample_5gb `
  --output reports/generated/engine_benchmark
```

The same files run through both engines. The workload produces monthly fare/tip totals and averages, pickup/dropoff zone counts, descriptive distance/fare/tip statistics, and payment counts/shares. Spark uses the aggregation definitions from `src/aggregation/taxi.py`. The command verifies a canonical fingerprint before writing `engine_benchmark.csv` and `engine_benchmark.json`.

The results include input bytes and retained rows, Spark session startup, read/clean time, aggregation time, total engine time, rows per second, peak RSS, Python/Spark/Pandas versions, and the result fingerprint. Spark startup is reported separately and is excluded from `total_seconds`. The Spark RSS sample includes the Python process tree and JVM. The Pandas sample omits the already-running idle Spark JVM, but includes the Python benchmark process tree.

Floating-point values are rounded to eight decimal places only when fingerprints are formed, so harmless accumulation-order differences do not count as workload mismatches. Spark and Pandas use the same lower-middle percentile convention for even-sized samples, matching Spark `percentile_approx` on this fixture.

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

CI uses only small fixtures to check equivalent results. Large benchmark data and result files should stay out of Git.
