# Scripts

Run scripts from the repository root with the project virtual environment active. Large benchmarks are separate from the normal data pipeline.

## Spark persistence benchmark

Compare the repeated aggregation workload with no shared input cache and with a `DISK_ONLY` cache:

```powershell
python scripts/benchmark_spark_aggregation.py --input data/gold/taxi_trips --output reports/generated/aggregation_benchmark --repetitions 3
```

Outputs: `aggregation_benchmark.csv`, `aggregation_benchmark.json`, and per-mode physical-plan text files.

## Spark versus Pandas benchmark

Pass repeated `--dataset LABEL=PATH` values for CSV files, directories, or globs:

```powershell
python scripts/benchmark_spark_vs_pandas.py --dataset 100mb=data/raw/sample_100mb --dataset 1gb=data/raw/sample_1gb --output reports/generated/engine_benchmark
```

Outputs: `engine_benchmark.csv` and `engine_benchmark.json`. See [docs/benchmarking.md](../docs/benchmarking.md) for cleaning rules, timing definitions, memory measurements, and interpretation guidance.
