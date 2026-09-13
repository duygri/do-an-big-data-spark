# Big Data Spark Project

Distributed data pipeline and analytics project using PySpark, Spark SQL, and Python.

## Pipeline

```text
raw input -> bronze/raw -> silver/cleaned -> gold/processed -> reports
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

4. Copy `configs/config.example.yaml` to `configs/config.yaml` and adjust local paths.
5. Run the pipeline entry point after the team modules are implemented:

   ```bash
   python -m src.pipeline.main --config configs/config.yaml
   ```

## Contribution workflow

- Work from an assigned GitHub Issue.
- Create one branch per Issue.
- Push the branch and open a Pull Request to `main`.
- At least one teammate must approve the Pull Request.
- Resolve all review conversations before merging.
- Do not push directly to `main`.

The complete task list is tracked in the [GitHub Issues](https://github.com/duygri/do-an-big-data-spark/issues).
