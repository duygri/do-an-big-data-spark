# Processed taxi Parquet output

The pipeline writes the feature-engineered and zone-enriched trip data to `data/gold/taxi_trips/` by default (`paths.processed_taxi` in YAML). It overwrites the previous output with Snappy-compressed Parquet.

The output is partitioned by `pickup_year` and `pickup_month`. This matches the source's monthly time grain and supports common time-range analysis without creating high-cardinality partitions for taxi locations or individual trips. Rows are shuffled with a temporary salt so each month can be written by multiple tasks; only year and month appear in the directory layout. `maxRecordsPerFile` limits large files while keeping the number of partition keys small.

After writing, the module reads the dataset back, restores the original column order, and checks every column name and Spark data type plus the total row count. A mismatch raises an error and fails the pipeline. The verified row count, schema, output path, and partition columns are logged.
