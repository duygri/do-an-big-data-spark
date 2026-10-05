# NYC yellow taxi feature definitions

`engineer_taxi_features()` accepts the cleaned 18-column Silver DataFrame and returns a DataFrame with the source columns preserved plus these Spark-native features. NYC taxi timestamps are local New York time, so the pipeline defaults `spark.sql.session.timeZone` to `America/New_York` (overridable with `spark.session_timezone` in YAML). This also makes duration calculations correct across daylight-saving transitions. The function does not filter rows or write storage; the pipeline subsequently enriches trips with zone labels and persists processed Parquet. See [zone joins](transformation-joins.md) and [processed output](processed-output.md).

| Feature | Definition | Unit and edge handling |
| --- | --- | --- |
| `trip_duration_minutes` | `(dropoff timestamp - pickup timestamp) / 60` | Minutes, rounded to 2 decimals. Null timestamps yield null. Negative durations are retained to expose invalid-time records. |
| `pickup_year` | Year component of `tpep_pickup_datetime` | Integer year; null when pickup timestamp is null. |
| `pickup_month` | Month component of `tpep_pickup_datetime` | Integer 1–12; null when pickup timestamp is null. |
| `pickup_hour` | Hour component of `tpep_pickup_datetime` | Integer 0–23; null when pickup timestamp is null. |
| `pickup_day_of_week` | Spark `dayofweek` of pickup timestamp | Integer 1–7, where 1 is Sunday and 7 is Saturday. |
| `pickup_is_weekend` | Pickup day is Sunday or Saturday | Boolean; null when pickup timestamp is null. |
| `average_speed_mph` | `trip_distance / (trip duration in hours)` | Miles per hour, rounded to 2 decimals. Null for nonpositive duration, negative distance, or null inputs. |
| `fare_per_mile` | `fare_amount / trip_distance` | USD per mile, rounded to 2 decimals. Null for distance at or below zero or null inputs. Negative source fares remain visible as negative ratios. |
| `tip_percentage` | `tip_amount / fare_amount * 100` | Percent, rounded to 2 decimals. Null when fare is at or below zero or either amount is null. |

`total_amount` is already supplied by the NYC Taxi source schema, so it is preserved unchanged instead of being duplicated or recomputed. The source does not contain passenger age, so age-group features are not applicable.

## Sample

For a trip picked up at `2020-04-01 10:00:00`, dropped off at `2020-04-01 10:15:00`, with distance `2.5` miles, fare `$8.00`, and tip `$1.60`, the derived values are:

```text
trip_duration_minutes = 15.0
pickup_year = 2020
pickup_month = 4
pickup_hour = 10
pickup_day_of_week = 4
pickup_is_weekend = false
average_speed_mph = 10.0
fare_per_mile = 3.2
tip_percentage = 20.0
```

The CLI applies these features to Silver, logs the output schema and a two-row feature sample, and leaves the resulting DataFrame available for the subsequent transformation/aggregation stages.
