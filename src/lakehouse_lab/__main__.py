"""Command line entry point."""

import argparse
import json
from pathlib import Path

from .pipeline import ingest


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the mini lakehouse quality lab")
    parser.add_argument("source", type=Path, help="Input CSV with order events")
    parser.add_argument("--output", type=Path, default=Path("build"))
    parser.add_argument(
        "--fail-on-rejected",
        action="store_true",
        help="Exit with status 1 when any input row is quarantined",
    )
    args = parser.parse_args()
    report = ingest(args.source, args.output)
    print(json.dumps(report, indent=2))
    if args.fail_on_rejected and report["rejected_rows"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
