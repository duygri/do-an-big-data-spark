"""Join NYC taxi trips with the official taxi-zone lookup dimension."""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, count, lit, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

TAXI_ZONE_LOOKUP_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

TRIP_LOCATION_COLUMNS = ("PULocationID", "DOLocationID")
ZONE_OUTPUT_COLUMNS = (
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",
    "dropoff_borough",
    "dropoff_zone",
    "dropoff_service_zone",
)


@dataclass(frozen=True)
class TaxiZoneJoinResult:
    """Enriched trips and join-quality counts for pipeline logging."""

    dataframe: DataFrame
    input_rows: int
    output_rows: int
    unmatched_pickup_rows: int
    unmatched_dropoff_rows: int
    duplicate_lookup_keys: int


def read_taxi_zone_lookup(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read the NYC TLC ``taxi+_zone_lookup.csv`` using an explicit schema."""
    source = str(path).strip()
    if not source:
        raise ValueError("Taxi zone lookup path cannot be empty")
    if "://" not in source and not Path(source).is_file():
        raise FileNotFoundError(f"Taxi zone lookup CSV not found: {source}")

    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(TAXI_ZONE_LOOKUP_SCHEMA)
        .csv(source)
    )


def join_taxi_zones(trips: DataFrame, zone_lookup: DataFrame) -> TaxiZoneJoinResult:
    """Enrich trips with pickup/dropoff zone labels using validated left joins.

    Left joins retain every trip. LocationID is a dimension key and must be
    unique; duplicate keys are rejected before joining so they cannot multiply
    trip rows. Unknown non-null trip IDs are counted in the returned report.
    """
    missing_trip_columns = sorted(set(TRIP_LOCATION_COLUMNS) - set(trips.columns))
    if missing_trip_columns:
        raise ValueError(
            "Taxi zone join input is missing required columns: "
            + ", ".join(missing_trip_columns)
        )

    required_lookup_columns = set(TAXI_ZONE_LOOKUP_SCHEMA.fieldNames())
    missing_lookup_columns = sorted(required_lookup_columns - set(zone_lookup.columns))
    if missing_lookup_columns:
        raise ValueError(
            "Taxi zone lookup is missing required columns: "
            + ", ".join(missing_lookup_columns)
        )

    collisions = sorted(set(ZONE_OUTPUT_COLUMNS).intersection(trips.columns))
    if collisions:
        raise ValueError(
            "Taxi zone join output columns already exist: " + ", ".join(collisions)
        )
    internal_collisions = sorted(
        {"_pickup_lookup_matched", "_dropoff_lookup_matched"}.intersection(trips.columns)
    )
    if internal_collisions:
        raise ValueError(
            "Taxi zone join uses reserved internal column(s): "
            + ", ".join(internal_collisions)
        )

    if not isinstance(zone_lookup.schema["LocationID"].dataType, IntegerType):
        raise ValueError("Taxi zone lookup LocationID must have Spark IntegerType")

    lookup = zone_lookup.select("LocationID", "Borough", "Zone", "service_zone")
    null_key_count = lookup.filter(col("LocationID").isNull()).count()
    if null_key_count:
        raise ValueError(
            f"Taxi zone lookup has {null_key_count} row(s) with a null LocationID key"
        )

    duplicate_keys = (
        lookup.groupBy("LocationID")
        .agg(count(lit(1)).alias("key_rows"))
        .filter(col("key_rows") > 1)
    )
    duplicate_key_count = duplicate_keys.count()
    if duplicate_key_count:
        sample_keys = [row["LocationID"] for row in duplicate_keys.orderBy("LocationID").limit(5).collect()]
        raise ValueError(
            "Taxi zone lookup has "
            f"{duplicate_key_count} duplicate LocationID key(s); sample IDs: {sample_keys}"
        )

    input_rows = trips.count()
    pickup_lookup = broadcast(
        lookup.select(
            col("LocationID").alias("_pickup_location_id"),
            lit(True).alias("_pickup_lookup_matched"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone"),
            col("service_zone").alias("pickup_service_zone"),
        )
    )
    enriched = trips.join(
        pickup_lookup,
        trips["PULocationID"] == pickup_lookup["_pickup_location_id"],
        "left",
    ).drop("_pickup_location_id")

    dropoff_lookup = broadcast(
        lookup.select(
            col("LocationID").alias("_dropoff_location_id"),
            lit(True).alias("_dropoff_lookup_matched"),
            col("Borough").alias("dropoff_borough"),
            col("Zone").alias("dropoff_zone"),
            col("service_zone").alias("dropoff_service_zone"),
        )
    )
    enriched = enriched.join(
        dropoff_lookup,
        enriched["DOLocationID"] == dropoff_lookup["_dropoff_location_id"],
        "left",
    ).drop("_dropoff_location_id")

    pickup_unmatched = col("PULocationID").isNotNull() & col("_pickup_lookup_matched").isNull()
    dropoff_unmatched = col("DOLocationID").isNotNull() & col("_dropoff_lookup_matched").isNull()
    metrics = enriched.agg(
        count(lit(1)).alias("output_rows"),
        count(when(pickup_unmatched, lit(1))).alias("unmatched_pickup_rows"),
        count(when(dropoff_unmatched, lit(1))).alias("unmatched_dropoff_rows"),
    ).first()
    output_rows = metrics["output_rows"]
    if output_rows != input_rows:
        raise RuntimeError(
            "Taxi zone join changed the trip row count: "
            f"{input_rows} input rows, {output_rows} output rows"
        )

    enriched = enriched.drop("_pickup_lookup_matched", "_dropoff_lookup_matched")

    return TaxiZoneJoinResult(
        dataframe=enriched,
        input_rows=input_rows,
        output_rows=output_rows,
        unmatched_pickup_rows=metrics["unmatched_pickup_rows"],
        unmatched_dropoff_rows=metrics["unmatched_dropoff_rows"],
        duplicate_lookup_keys=duplicate_key_count,
    )
