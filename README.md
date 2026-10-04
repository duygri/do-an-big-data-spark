# Big Data Spark Project

Distributed data pipeline and analytics project using PySpark, Spark SQL, and Python.

## Pipeline

```text
data/raw/nyc_taxi -> data/bronze/raw -> data/silver -> data/gold -> reports
```

## Project structure

```text
data/
├── raw/             # Source files; do not commit large datasets
├── bronze/          # Raw data persisted by the ingestion stage
├── silver/          # Cleaned and validated data
├── gold/            # Transformed and processed data
└── sample/          # Small datasets for local tests and demos

src/
├── ingestion/       # Read CSV/JSON and write the bronze layer
├── cleaning/        # Missing values, duplicates, and normalization
├── transformation/  # Derived columns and joins
├── aggregation/     # Spark SQL and DataFrame aggregations
└── pipeline/        # End-to-end entry point

tests/
├── unit/            # Focused transformation tests
└── integration/     # End-to-end pipeline tests

configs/             # Example configuration files
docs/                # Architecture and technical documentation
notebooks/           # Optional exploratory and demo notebooks
reports/             # Generated summaries and charts
scripts/             # Reproducible helper scripts
```

## Local setup

1. Install Python 3.10+ and Java 11+ or Java 17+.
2. Create and activate a virtual environment.
3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Place the NYC yellow taxi CSV files in `data/raw/nyc_taxi/`. The local dataset and generated Parquet are excluded from Git.
5. Copy `configs/config.example.yaml` to `configs/config.yaml` and adjust local paths. The example reads all 18 monthly CSVs from 2019-01 through 2020-06. Use a single file path to run a smaller trial.
6. Run ingestion, Bronze, cleaning, taxi feature engineering, zone enrichment, and processed Parquet output:

   ```bash
   python -m src.pipeline.main --config configs/config.yaml
   ```

   The command logs schemas, sample rows, and join/read-back counts. Bronze, Silver, and `data/gold/taxi_trips/` are overwritten on each run. The pipeline joins pickup/dropoff IDs to the NYC taxi-zone lookup and writes the enriched feature data as Snappy Parquet partitioned by pickup year and month. Set `input.zone_lookup_path` if the lookup is stored elsewhere. Quality reports appear in `reports/generated/`. See [ingestion details](docs/ingestion.md), [cleaning policy](docs/cleaning-quality.md), [feature definitions](docs/transformation-features.md), [zone joins](docs/transformation-joins.md), and [processed output](docs/processed-output.md).

## Contribution workflow

- Work from an assigned GitHub Issue.
- Create one branch per Issue.
- Push the branch and open a Pull Request to `main`.
- At least one teammate must approve the Pull Request.
- Resolve all review conversations before merging.
- Do not push directly to `main`.

The complete task list is tracked in the [GitHub Issues](https://github.com/duygri/do-an-big-data-spark/issues).
