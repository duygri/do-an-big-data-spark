"""Small Spark integration checks for the ingestion and Bronze contract."""

import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from src.ingestion.bronze import write_bronze
from src.ingestion.reader import read_raw
from src.ingestion.schema import YELLOW_TAXI_SCHEMA
from src.pipeline.main import run


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("ingestion-tests").getOrCreate()
    yield session
    session.stop()


def test_csv_schema_and_bronze(tmp_path, spark):
    csv = tmp_path / "taxi.csv"
    csv.write_text(
        ",".join(YELLOW_TAXI_SCHEMA.fieldNames()) + "\n"
        "1,2019-01-01 00:00:00,2019-01-01 00:10:00,2,1.5,1,N,10,20,1,8,0.5,0.5,1,0,0.3,10.3,\n",
        encoding="utf-8",
    )
    df = read_raw(spark, str(csv), file_format="csv", schema=YELLOW_TAXI_SCHEMA)
    assert isinstance(df.schema["VendorID"].dataType, IntegerType)
    assert df.first().congestion_surcharge is None
    assert write_bronze(df, spark, str(tmp_path / "bronze")) == 1


def test_json_and_missing_path(tmp_path, spark):
    path = tmp_path / "input.json"
    path.write_text(json.dumps({"id": 7, "name": "sample"}) + "\n", encoding="utf-8")
    assert read_raw(spark, str(path), file_format="json").first().id == 7
    with pytest.raises(FileNotFoundError):
        read_raw(spark, str(tmp_path / "missing.csv"), file_format="csv")
    with pytest.raises(ValueError, match="Unsupported"):
        read_raw(spark, str(path), file_format="xml")


def test_empty_input_rejected(tmp_path, spark):
    path = tmp_path / "empty.csv"
    path.write_text(",".join(YELLOW_TAXI_SCHEMA.fieldNames()) + "\n", encoding="utf-8")
    df = read_raw(spark, str(path), file_format="csv", schema=YELLOW_TAXI_SCHEMA)
    with pytest.raises(ValueError, match="no rows"):
        write_bronze(df, spark, str(tmp_path / "bronze"))


def test_cli_missing_input_does_not_change_spark_session(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        "input:\n  path: does-not-exist.csv\n  format: csv\n"
        "  schema: yellow_taxi\n"
        "paths:\n  bronze: unused-bronze\n  silver: unused-silver\n"
        "  reports: unused-reports\n"
        "spark:\n  app_name: failure-test\n  master: local[1]\n",
        encoding="utf-8",
    )
    active_before = SparkSession.getActiveSession()
    with pytest.raises(FileNotFoundError):
        run(config)
    assert SparkSession.getActiveSession() is active_before
