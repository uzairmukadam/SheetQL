import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from rich import box
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.tree import Tree

from sheetql.constants import get_package_version
from sheetql.duckdb_util import fetch_columns_by_table, rename_relation
from sheetql.engine import SheetQL
from sheetql.logging import setup_logging

CLI_QUERY_PREVIEW_ROWS = 50

_JSON_INDENT = 2 if sys.stdout.isatty() else None


def _default_prog() -> str:
    """Usage banner: sheet_ql.py when run from clone, else entry name (e.g. sheetql.exe)."""
    if not sys.argv:
        return "sheetql"
    name = Path(sys.argv[0]).name
    if name in ("pytest", "py.test"):
        return "sheetql"
    if name == "__main__.py":
        return "python -m sheetql"
    if name.endswith(".py"):
        return name
    return name or "sheetql"


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
    return df.to_json(orient="records", indent=_JSON_INDENT)


def _format_df_as_csv(df: pd.DataFrame) -> str:
    return df.to_csv(index=False)


def _sql_with_server_limit(sql: str, limit: Optional[int]) -> Tuple[str, bool]:
    """
    For SELECT / WITH, push LIMIT into the engine so DuckDB does not materialize
    unbounded result sets. Returns (sql_to_run, limit_was_pushed).
    """
    if limit is None or limit <= 0:
        return sql, False
    stripped = sql.strip().rstrip(";").strip()
    low = stripped.lower()
    if not (low.startswith("select") or low.startswith("with")):
        return sql, False
    wrapped = f"SELECT * FROM ({stripped}) AS _sheetql_q LIMIT {int(limit)}"
    return wrapped, True


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

    if not tables:
        if args.format == "json":
            sys.stdout.write(json.dumps({"tables": []}, indent=_JSON_INDENT))
            if _JSON_INDENT is not None:
                sys.stdout.write("\n")
        else:
            tool.console.print(
                Panel(
                    "[dim]No tables loaded from the given inputs.[/]", title="inspect"
                )
            )
        return 0

    if args.format == "json":
        cols_map = fetch_columns_by_table(tool.db_connection, tables)
        out = {"tables": [{"name": t, "columns": cols_map.get(t, [])} for t in tables]}
        sys.stdout.write(json.dumps(out, indent=_JSON_INDENT))
        if _JSON_INDENT is not None:
            sys.stdout.write("\n")
    else:
        tool.console.print()
        tool.console.print(
            Rule(
                f"[bold cyan]inspect[/] · {len(tables)} table(s)",
                style="cyan",
            )
        )
        cols_map = (
            fetch_columns_by_table(tool.db_connection, tables) if args.schema else {}
        )
        if args.schema and tables:
            grid = Table(
                box=box.ROUNDED,
                show_header=True,
                header_style="bold magenta",
                title="[bold]Tables & columns[/]",
                expand=True,
            )
            grid.add_column("Table", style="cyan", no_wrap=True)
            grid.add_column("Columns", overflow="fold")
            for t in tables:
                cols = cols_map.get(t, [])
                grid.add_row(t, ", ".join(cols) if cols else "[dim]—[/]")
            tool.console.print(grid)
        else:
            tree = Tree(f"[bold]Tables[/] [dim]({len(tables)})[/]")
            for t in tables:
                tree.add(f"[cyan]{t}[/]")
            tool.console.print(tree)

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
            rename_relation(tool.db_connection, old_name, new_name)
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

    sql_to_run, limit_pushed = _sql_with_server_limit(sql, args.limit)
    try:
        df = tool.db_connection.execute(sql_to_run).fetchdf()
    except Exception as e:
        logger.error(f"SQL Error: {e}")
        return 5

    if args.limit is not None and args.limit > 0 and not limit_pushed:
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
                f.write(df.to_json(orient="records", indent=2))
            return 0

        logger.error("Unsupported output extension. Use .xlsx, .csv, or .json.")
        return 6

    if args.format == "json":
        sys.stdout.write(_format_df_as_json(df))
        if _JSON_INDENT is not None:
            sys.stdout.write("\n")
    elif args.format == "csv":
        sys.stdout.write(_format_df_as_csv(df))
    else:
        tool.console.print()
        tool.console.print(Rule("[bold]Query result[/]", style="dim"))
        meta = f"[dim]{len(df):,} row(s) × {len(df.columns)} column(s)" + (
            " · server LIMIT applied[/]" if limit_pushed else "[/]"
        )
        tool.console.print(meta)
        tool.console.print()
        tool._display_results_table(
            df,
            title="Result",
            preview_rows=min(CLI_QUERY_PREVIEW_ROWS, max(len(df), 1)),
            col_max_width=56,
        )

    return 0


def build_parser(*, prog: Optional[str] = None) -> argparse.ArgumentParser:
    prog = prog if prog is not None else _default_prog()
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Run SQL over spreadsheets and flat files (CSV, Excel, Parquet, JSON) "
            "using DuckDB in-process."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "With no subcommand, the interactive shell starts (same as "
            f"`{prog} interactive`).\n"
            f"Use `{prog} <command> --help` for subcommand options."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {get_package_version()}",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", default="sheetql.log", help="Log file path")

    sub = parser.add_subparsers(dest="cmd")

    p_interactive = sub.add_parser(
        "interactive",
        help="Start the interactive SQL shell (file picker, staging, export)",
    )
    p_interactive.set_defaults(func=_cmd_interactive)

    p_run = sub.add_parser(
        "run",
        help="Run a YAML batch script (inputs, tasks, export)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Example:\n  {prog} run -c pipeline.yaml\n  {prog} run -c pipeline.yml",
    )
    p_run.add_argument(
        "-c",
        "--config",
        required=True,
        help="Path to YAML script (.yaml or .yml)",
    )
    p_run.set_defaults(func=_cmd_run)

    p_inspect = sub.add_parser(
        "inspect",
        help="List inferred DuckDB table names (and optionally columns)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            f"  {prog} inspect -i sales.csv\n"
            f"  {prog} inspect -i data/*.parquet --schema"
        ),
    )
    p_inspect.add_argument(
        "-i", "--input", required=True, nargs="+", help="Input files"
    )
    p_inspect.add_argument("--schema", action="store_true", help="Also show columns")
    p_inspect.add_argument("--format", choices=["table", "json"], default="table")
    p_inspect.set_defaults(func=_cmd_inspect)

    p_query = sub.add_parser(
        "query",
        help="Run one SQL statement against loaded file(s)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            f'  {prog} query -i report.xlsx -q "SELECT * FROM report_sheet LIMIT 10"\n'
            f'  {prog} query -i a.csv b.csv --alias m=a_csv -q "SELECT * FROM m"\n'
            f'  {prog} query -i big.parquet -q "SELECT COUNT(*) FROM big_parquet" -o out.csv'
        ),
    )
    p_query.add_argument("-i", "--input", required=True, nargs="+", help="Input files")
    p_query.add_argument("-q", "--query", default="", help="SQL to execute")
    p_query.add_argument("-f", "--query-file", help="Path to .sql file")
    p_query.add_argument(
        "--alias", action="append", help="Rename view: new=old (repeatable)"
    )
    p_query.add_argument("--limit", type=int, help="Limit result rows")
    p_query.add_argument("--format", choices=["table", "csv", "json"], default="table")
    p_query.add_argument(
        "-o", "--output", help="Write to .xlsx/.csv/.json instead of stdout"
    )
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
