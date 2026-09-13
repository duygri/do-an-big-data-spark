"""Command-line entry point for the end-to-end Spark pipeline.

The individual stages will be connected here as TASK-01 through TASK-14 are
implemented. This first version validates the command-line contract only.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Big Data Spark pipeline")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config.yaml"),
        help="Path to the local YAML configuration file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.config.exists():
        raise SystemExit(
            f"Configuration file not found: {args.config}. "
            "Copy configs/config.example.yaml to configs/config.yaml first."
        )
    print(f"Pipeline configuration found: {args.config}")
    print("Pipeline stages will be connected as the assigned Issues are implemented.")


if __name__ == "__main__":
    main()
