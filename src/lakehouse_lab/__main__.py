"""Command line entry point."""

import argparse
import json
from pathlib import Path

from .pipeline import ingest


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the mini lakehouse quality lab")
    parser.add_argument("source", type=Path, help="Input CSV with order events")
    parser.add_argument("--output", type=Path, default=Path("build"))
    args = parser.parse_args()
    print(json.dumps(ingest(args.source, args.output), indent=2))


if __name__ == "__main__":
    main()
