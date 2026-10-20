"""Preflight selected NYC yellow taxi months from January through June 2020."""

import fnmatch
import glob
from pathlib import Path

EXPECTED_TAXI_FILES = tuple(
    f"yellow_tripdata_2020-{month:02d}.csv" for month in range(1, 7)
)
SUPPORTED_TAXI_FILES = frozenset(EXPECTED_TAXI_FILES)


def validate_taxi_inputs(path_pattern: str, file_format: str) -> list[Path]:
    """Accept one or more supported monthly CSVs before Spark starts.

    A caller may select a single month, a subset of months, or the default
    January–June window. This keeps accidental 2019 and out-of-window files
    out of the pipeline while allowing smaller local runs.
    """
    if file_format != "csv":
        raise ValueError("NYC yellow taxi pipeline requires input.format: csv")
    matches = sorted(Path(path) for path in glob.glob(path_pattern))
    if not matches:
        raise FileNotFoundError(f"No NYC taxi files matched input.path: {path_pattern}")
    names = [path.name for path in matches if path.is_file()]
    requested = {
        name
        for name in EXPECTED_TAXI_FILES
        if fnmatch.fnmatchcase(name, Path(path_pattern).name)
    }
    missing = sorted(requested - set(names))
    unexpected = sorted(set(names) - SUPPORTED_TAXI_FILES)
    duplicates = sorted(name for name in set(names) if names.count(name) > 1)
    non_files = sorted(str(path) for path in matches if not path.is_file())
    if missing or unexpected or duplicates or non_files:
        raise ValueError(
            "NYC taxi input must select one or more distinct monthly CSVs "
            "from 2020-01 through 2020-06, with every month selected by the "
            "input pattern present; "
            f"missing={missing}; "
            f"unexpected={unexpected}; "
            f"duplicates={duplicates}; non_files={non_files}"
        )
    return matches
