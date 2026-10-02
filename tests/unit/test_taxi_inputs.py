"""Preflight file selection without creating a Spark session."""

import pytest

from src.pipeline import main as pipeline_main
from src.pipeline.taxi_inputs import EXPECTED_TAXI_FILES, validate_taxi_inputs


def make_months(directory, names):
    for name in names:
        (directory / name).write_text("header\n", encoding="utf-8")


def test_exact_six_months_selected(tmp_path):
    make_months(tmp_path, EXPECTED_TAXI_FILES)
    make_months(tmp_path, ("yellow_tripdata_2019-12.csv",))
    pattern = str(tmp_path / "yellow_tripdata_2020-0[1-6].csv")
    assert [path.name for path in validate_taxi_inputs(pattern, "csv")] == list(EXPECTED_TAXI_FILES)


def test_missing_month_rejected(tmp_path):
    make_months(tmp_path, EXPECTED_TAXI_FILES[:-1])
    with pytest.raises(ValueError, match="2020-06"):
        validate_taxi_inputs(str(tmp_path / "yellow_tripdata_2020-*.csv"), "csv")


def test_2019_or_later_month_rejected(tmp_path):
    make_months(tmp_path, EXPECTED_TAXI_FILES)
    make_months(tmp_path, ("yellow_tripdata_2019-12.csv", "yellow_tripdata_2020-07.csv"))
    with pytest.raises(ValueError, match="unexpected=.*2019-12"):
        validate_taxi_inputs(str(tmp_path / "yellow_tripdata_*.csv"), "csv")


def test_format_and_empty_glob_rejected(tmp_path):
    make_months(tmp_path, EXPECTED_TAXI_FILES)
    with pytest.raises(ValueError, match="format: csv"):
        validate_taxi_inputs(str(tmp_path / "*.csv"), "json")
    with pytest.raises(FileNotFoundError, match="No NYC taxi files"):
        validate_taxi_inputs(str(tmp_path / "missing*.csv"), "csv")


def test_invalid_selection_fails_before_spark(tmp_path, monkeypatch):
    make_months(tmp_path, EXPECTED_TAXI_FILES)
    make_months(tmp_path, ("yellow_tripdata_2019-12.csv",))
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "input:\n"
        f"  path: {(tmp_path / 'yellow_tripdata_*.csv').as_posix()}\n"
        "  format: csv\n  schema: yellow_taxi\n"
        "paths:\n  bronze: bronze\n  silver: silver\n  reports: reports\n"
        "spark:\n  master: local[1]\n",
        encoding="utf-8",
    )

    class NoSpark:
        @property
        def builder(self):
            raise AssertionError("Spark must not start for invalid taxi inputs")

    monkeypatch.setattr(pipeline_main, "SparkSession", NoSpark())
    with pytest.raises(ValueError, match="2019-12"):
        pipeline_main.run(config)
