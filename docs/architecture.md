# Pipeline architecture

The project follows a simple Medallion architecture:

1. **Raw**: original CSV or JSON files received from the source.
2. **Bronze**: the raw data persisted in a Spark-friendly format such as Parquet.
3. **Silver**: data after missing-value handling, deduplication, normalization, and quality checks.
4. **Gold**: transformed, joined, and aggregated data prepared for analysis.
5. **Reports**: small, presentation-ready summaries and charts generated from Gold data.

The pipeline stages should be implemented as independent modules so that each stage can be tested and rerun without duplicating business logic.
