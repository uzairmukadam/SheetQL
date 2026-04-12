"""
SheetQL: Professional Data Analysis & ETL Tool

This module implements an interactive Command Line Interface (CLI) for querying
flat files (CSV, Excel, Parquet, JSON) using SQL.

Architecture:
- CSV/Parquet/JSON: Uses DuckDB's "Zero-Copy" views for high performance on large files.
- Excel: Uses Pandas to bridge data into DuckDB (requires memory for loading).
"""

import argparse
from sheetql.engine import SheetQL
from sheetql.cli import main as sheetql_main
from sheetql.logging import setup_logging


def main() -> None:
    # Backwards-compatible entrypoint:
    # - supports legacy `python sheet_ql.py --debug` and `python sheet_ql.py -r script.yaml`
    # - supports richer subcommand CLI via `sheetql.cli`
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
