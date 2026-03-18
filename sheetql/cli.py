import argparse
import json
import sys
from typing import Dict, List, Optional

import pandas as pd

from sheetql.engine import SheetQL
from sheetql.logging import setup_logging


def _parse_aliases(items: Optional[List[str]]) -> Dict[str, str]:
    """
    Parse repeated --alias values of the form name=table.
    """
    aliases: Dict[str, str] = {}
    if not items:
        return aliases
    for item in items:
        if "=" not in item:
            raise argparse.ArgumentTypeError("--alias must be in the form name=table")
        name, table = item.split("=", 1)
        name = name.strip()
        table = table.strip()
        if not name or not table:
            raise argparse.ArgumentTypeError("--alias must be in the form name=table")
        aliases[name] = table
    return aliases


def _format_df_as_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records")


def _format_df_as_csv(df: pd.DataFrame) -> str:
    return df.to_csv(index=False)


def _cmd_interactive(args: argparse.Namespace) -> int:
    logger = setup_logging(args.debug, log_file=args.log_file)
    tool = SheetQL(logger)
    tool.run_interactive()
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    logger = setup_logging(args.debug, log_file=args.log_file)
    tool = SheetQL(logger)
    tool.run_batch(args.config)
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    logger = setup_logging(args.debug, log_file=args.log_file)
    tool = SheetQL(logger)
    tool._init_db()
    tool._load_data(args.input)

    if not tool.db_connection:
        logger.error("DuckDB not initialized.")
        return 2

    try:
        tables = tool.db_connection.execute("SHOW TABLES").fetchdf()["name"].tolist()
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        return 3

    if args.format == "json":
        out = {"tables": []}
        for t in tables:
            try:
                schema_df = tool.db_connection.execute(f"DESCRIBE {t}").fetchdf()
                out["tables"].append(
                    {"name": t, "columns": schema_df["column_name"].tolist()}
                )
            except Exception:
                out["tables"].append({"name": t, "columns": []})
        sys.stdout.write(json.dumps(out))
    else:
        tool.console.print(f"\n[cyan]Tables ({len(tables)}):[/cyan]")
        for t in tables:
            tool.console.print(f" - {t}")
            if args.schema:
                try:
                    schema_df = tool.db_connection.execute(f"DESCRIBE {t}").fetchdf()
                    cols = schema_df["column_name"].tolist()
                    tool.console.print(f"   columns: {', '.join(cols)}")
                except Exception:
                    pass

    return 0


def _cmd_query(args: argparse.Namespace) -> int:
    logger = setup_logging(args.debug, log_file=args.log_file)
    tool = SheetQL(logger)
    tool._init_db()
    tool._load_data(args.input)

    if not tool.db_connection:
        logger.error("DuckDB not initialized.")
        return 2

    aliases = _parse_aliases(args.alias)
    for new_name, old_name in aliases.items():
        try:
            tool.db_connection.execute(f'ALTER VIEW "{old_name}" RENAME TO "{new_name}"')
        except Exception as e:
            logger.error(f"Alias failed {old_name} -> {new_name}: {e}")
            return 3

    sql = args.query
    if args.query_file:
        try:
            with open(args.query_file, "r", encoding="utf-8") as f:
                sql = f.read()
        except Exception as e:
            logger.error(f"Failed to read query file: {e}")
            return 4

    if not sql:
        logger.error("No query provided. Use --query or --query-file.")
        return 4

    try:
        df = tool.db_connection.execute(sql).fetchdf()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return 5

    if args.limit is not None:
        df = df.head(args.limit)

    if args.output:
        if args.output.lower().endswith(".xlsx"):
            tool.results_to_save.clear()
            tool.results_to_save[args.sheet_name or "result"] = df
            tool._save_to_excel(args.output)
            return 0
        if args.output.lower().endswith(".csv"):
            df.to_csv(args.output, index=False)
            return 0
        if args.output.lower().endswith(".json"):
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(_format_df_as_json(df))
            return 0

        logger.error("Unsupported output extension. Use .xlsx, .csv, or .json.")
        return 6

    if args.format == "json":
        sys.stdout.write(_format_df_as_json(df))
    elif args.format == "csv":
        sys.stdout.write(_format_df_as_csv(df))
    else:
        tool._display_results_table(df)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sheetql", description="SheetQL Professional")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", default="sheetql.log", help="Log file path")

    sub = parser.add_subparsers(dest="cmd")

    p_interactive = sub.add_parser("interactive", help="Start interactive shell")
    p_interactive.set_defaults(func=_cmd_interactive)

    p_run = sub.add_parser("run", help="Run a YAML batch script")
    p_run.add_argument("-c", "--config", required=True, help="Path to YAML config")
    p_run.set_defaults(func=_cmd_run)

    p_inspect = sub.add_parser("inspect", help="Inspect inferred tables/schemas")
    p_inspect.add_argument("-i", "--input", required=True, nargs="+", help="Input files")
    p_inspect.add_argument("--schema", action="store_true", help="Also show columns")
    p_inspect.add_argument("--format", choices=["table", "json"], default="table")
    p_inspect.set_defaults(func=_cmd_inspect)

    p_query = sub.add_parser("query", help="Run a one-off SQL query")
    p_query.add_argument("-i", "--input", required=True, nargs="+", help="Input files")
    p_query.add_argument("-q", "--query", default="", help="SQL to execute")
    p_query.add_argument("-f", "--query-file", help="Path to .sql file")
    p_query.add_argument("--alias", action="append", help="Rename view: new=old (repeatable)")
    p_query.add_argument("--limit", type=int, help="Limit result rows")
    p_query.add_argument("--format", choices=["table", "csv", "json"], default="table")
    p_query.add_argument("-o", "--output", help="Write to .xlsx/.csv/.json instead of stdout")
    p_query.add_argument("--sheet-name", help="Excel sheet name when writing .xlsx")
    p_query.set_defaults(func=_cmd_query)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Default behavior: no subcommand -> interactive
    if not getattr(args, "cmd", None):
        return _cmd_interactive(args)

    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return 130


__all__ = ["build_parser", "main"]

