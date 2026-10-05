"""Preflight the fixed January–June 2020 NYC yellow taxi input window."""

import glob
from pathlib import Path

EXPECTED_TAXI_FILES = tuple(
    f"yellow_tripdata_2020-{month:02d}.csv" for month in range(1, 7)
)


def validate_taxi_inputs(path_pattern: str, file_format: str) -> list[Path]:
    """Require exactly one CSV for each month before Spark starts."""
    if file_format != "csv":
        raise ValueError("NYC yellow taxi pipeline requires input.format: csv")
    matches = sorted(Path(path) for path in glob.glob(path_pattern))
    if not matches:
        raise FileNotFoundError(f"No NYC taxi files matched input.path: {path_pattern}")
    names = [path.name for path in matches if path.is_file()]
    expected = set(EXPECTED_TAXI_FILES)
    missing = sorted(expected - set(names))
    unexpected = sorted(set(names) - expected)
    duplicates = sorted(name for name in expected if names.count(name) > 1)
    non_files = sorted(str(path) for path in matches if not path.is_file())
    if missing or unexpected or duplicates or non_files:
        raise ValueError(
            "NYC taxi input must contain exactly the six monthly CSVs "
            "2020-01 through 2020-06; "
            f"missing={missing}; unexpected={unexpected}; "
            f"duplicates={duplicates}; non_files={non_files}"
        )
    return matches
