# NYC yellow taxi cleaning and quality policy

The pipeline reads verified Bronze Parquet from the six January–June 2020 monthly CSVs, then performs missing-value handling, exact-row deduplication, text normalization, validation, and Silver Parquet export. Run `python -m src.pipeline.main --config configs/config.yaml` from the repository root. The default Silver path is `data/silver/cleaned/` and reports are written to `reports/generated/taxi_quality.json` and `taxi_quality.md`. Generated data and reports are excluded from Git.

| Field group | Policy |
| --- | --- |
| Pickup and dropoff timestamps; pickup and dropoff location IDs | Required; remove rows with null values. |
| Vendor, passenger count, rate code, payment type, distance, amounts, surcharges, and store-and-forward flag | Optional; keep null and report counts before and after cleaning. No arbitrary zero or mean imputation. |
| Exact duplicate records | Remove only when all 18 fields match. There is no stable trip ID; distinct rows with the same time and locations are retained. |
| Store-and-forward flag | Trim whitespace and lowercase; empty strings become null. Unexpected non-null values are reported. |
| Timestamps, distance, currency | Keep schema timestamps, miles, and USD. No unit conversion is appropriate for this source. |

The report includes stage row counts, schema, before and after null counts, numeric minimum/maximum/average, and warning counts with up to two examples per warning. It checks pickup after dropoff, nonpositive location IDs, negative passenger count, distance, fare and total amount, and unexpected store-and-forward flags. Warnings remain in Silver, including negative fares that may represent adjustments. A reviewer can decide whether later analytical stages should filter them. The report records the lack of a unique business key.

## April 2020 sample evidence

The checked-in code was run against the local `yellow_tripdata_2020-04.csv` (237,993 records). Required-field cleaning removed 0 records and exact deduplication removed 0 records. The report found 19,513 nulls in each of `VendorID`, `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, and `payment_type`; these optional nulls remained. It flagged 1,295 negative `fare_amount` values and 1,294 negative `total_amount` values. All other configured warning counts were 0. This sample run exercised cleaning and quality metrics; Parquet writing is checked by the Ubuntu integration tests.
