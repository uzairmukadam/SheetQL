"""
SheetQL — primary entry when running from a git clone.

Typical use (from the repo root, with a venv activated)::

    pip install -r requirements.txt
    python sheet_ql.py                    # interactive SQL shell
    python sheet_ql.py -r pipeline.yml  # YAML batch
    python sheet_ql.py query -i data.csv -q "SELECT 1"  # subcommands (same as sheetql CLI)

Architecture:
- CSV/Parquet/JSON: DuckDB zero-copy views where possible.
- Excel: Pandas bridges into DuckDB (in-memory for loaded sheets).
"""

import argparse
from sheetql.engine import SheetQL
from sheetql.cli import main as sheetql_main
from sheetql.logging import setup_logging


def main() -> None:
    # Top-level flags for the app entry; remaining args go to the shared CLI (query, run, …).
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-r", "--run", dest="config_path", help="Run batch config")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-h", "--help", action="store_true", help="Show help")
    args, rest = parser.parse_known_args()

    if args.help:
        raise SystemExit(sheetql_main(["--help"]))

    if args.config_path:
        logger = setup_logging(args.debug)
        tool = SheetQL(logger)
        tool.run_batch(args.config_path)
        return

    if rest:
        raise SystemExit(sheetql_main(rest))

    logger = setup_logging(args.debug)
    tool = SheetQL(logger)
    tool.run_interactive()


if __name__ == "__main__":
    main()
