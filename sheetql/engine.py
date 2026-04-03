import os
import re
import logging
from collections import deque
from typing import Any, Optional, List, Tuple, Dict

import duckdb
import pandas as pd
from openpyxl.styles import Font, PatternFill
from rich.console import Console
from rich.table import Table

from sheetql.completion import SheetQLCompleter
from sheetql.deps import (
    CALAMINE_AVAILABLE,
    PROMPT_TOOLKIT_AVAILABLE,
    TKINTER_AVAILABLE,
    XLSXWRITER_AVAILABLE,
    YAML_AVAILABLE,
)
from sheetql.session import SessionRecorder
from sheetql.naming import normalize_name
from sheetql.scripting import ScriptConfigError, ScriptExport, parse_script_config, resolve_alias_targets

if PROMPT_TOOLKIT_AVAILABLE:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import InMemoryHistory
    from prompt_toolkit.lexers import PygmentsLexer
    from pygments.lexers.sql import SqlLexer
    from prompt_toolkit.styles import Style
else:
    PromptSession = None  # type: ignore[assignment]
    InMemoryHistory = None  # type: ignore[assignment]
    PygmentsLexer = None  # type: ignore[assignment]
    SqlLexer = None  # type: ignore[assignment]
    Style = None  # type: ignore[assignment]

if YAML_AVAILABLE:
    import yaml
else:
    yaml = None  # type: ignore[assignment]

if TKINTER_AVAILABLE:
    import tkinter as tk
    from tkinter import filedialog
else:
    tk = None  # type: ignore[assignment]
    filedialog = None  # type: ignore[assignment]


class SheetQL:
    """Main application controller."""

    PROMPT_SQL = "SQL> "
    PROMPT_CONTINUE = "  -> "
    DEFAULT_EXPORT_FILENAME = "query_result.xlsx"
    HISTORY_MAX_LEN = 50
    DEFAULT_MEMORY_LIMIT = "75%"

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.console = Console()
        self.db_connection: Optional[duckdb.DuckDBPyConnection] = None
        self.results_to_save: Dict[str, pd.DataFrame] = {}
        self.history: deque[str] = deque(maxlen=self.HISTORY_MAX_LEN)
        self.schema_cache: Dict[str, List[str]] = {}
        self.recorder = SessionRecorder()

        self.loaded_files_map: Dict[str, List[str]] = {}

        self.session = None
        if PROMPT_TOOLKIT_AVAILABLE and PromptSession is not None and InMemoryHistory is not None:
            try:
                # InMemoryHistory enables Ctrl+R reverse-search and Up/Down navigation.
                # Wrapped in try/except because prompt_toolkit probes the terminal at
                # construction time and raises in non-interactive environments (e.g. tests).
                self.session = PromptSession(history=InMemoryHistory())
            except Exception:
                self.session = None

    # --- Lifecycle / entrypoints -------------------------------------------------

    def run_interactive(self) -> None:
        try:
            self._display_welcome()
            self._init_db()

            if initial_paths := self._prompt_for_paths(
                title="Select Data Files",
                filetypes=[
                    (
                        "Supported Files",
                        "*.xlsx *.xls *.csv *.parquet *.json *.jsonl *.ndjson",
                    ),
                    ("All files", "*.*"),
                ],
                allow_multiple=True,
            ):
                self._load_data(initial_paths)
                self.logger.info("[bold green]--- 🦆 DuckDB is ready ---[/bold green]")
                self._list_tables()
                self._run_interactive_loop()

        except Exception as e:
            self.logger.critical(f"Fatal error in interactive loop: {e}", exc_info=True)
        finally:
            self.logger.info("[bold cyan]👋 Goodbye![/bold cyan]")

    def run_batch(self, config_path: str) -> None:
        self.logger.info(f"🚀 Batch mode: '{config_path}'")
        if not YAML_AVAILABLE or yaml is None:
            self.logger.error("PyYAML is not installed.")
            return

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            return

        self._init_db()
        self._execute_yaml_script(config)

    # --- DB / schema -------------------------------------------------------------

    def _init_db(self) -> None:
        self.db_connection = duckdb.connect(database=":memory:")
        try:
            self.db_connection.execute(f"SET memory_limit='{self.DEFAULT_MEMORY_LIMIT}';")
        except Exception:
            self.logger.debug("DuckDB memory limit config failed. Using defaults.")

    def _update_schema_cache(self, table_names: List[str]) -> None:
        if not self.db_connection:
            return
        for table in table_names:
            try:
                schema_df = self.db_connection.execute(f"DESCRIBE {table}").fetchdf()
                self.schema_cache[table] = schema_df["column_name"].tolist()
            except Exception:
                pass

    # --- UI helpers --------------------------------------------------------------

    def _display_welcome(self) -> None:
        self.console.print("[bold green]--- SheetQL Professional ---[/bold green]")
        self.console.print(
            "Commands: [yellow].help[/yellow], [yellow].load[/yellow], [yellow].dump <file>[/yellow]"
        )
        status = []
        status.append(
            "[green]Rust-Excel[/green]"
            if CALAMINE_AVAILABLE
            else "[red]Rust-Excel[/red]"
        )
        status.append(
            "[green]Stream-Write[/green]"
            if XLSXWRITER_AVAILABLE
            else "[red]Stream-Write[/red]"
        )
        status.append(
            "[green]Autocomplete[/green]"
            if PROMPT_TOOLKIT_AVAILABLE
            else "[red]Autocomplete[/red]"
        )
        self.console.print(f"Engine Status: {', '.join(status)}")

    def _display_results_table(self, df: pd.DataFrame) -> None:
        table = Table(show_header=True, header_style="bold magenta")
        for col in df.columns:
            table.add_column(str(col))
        for _, row in df.head(15).iterrows():
            table.add_row(*[str(x) for x in row])
        self.console.print(table)
        if len(df) > 15:
            self.console.print(f"... ({len(df)-15} more rows)")

    # --- Path / file prompts -----------------------------------------------------

    def _prompt_for_paths(
        self, title: str, filetypes: List[Tuple[str, str]], allow_multiple: bool
    ) -> Optional[List[str]]:
        if TKINTER_AVAILABLE and tk is not None and filedialog is not None:
            root = tk.Tk()
            root.withdraw()
            if allow_multiple:
                paths = filedialog.askopenfilenames(title=title, filetypes=filetypes)
            else:
                paths = [filedialog.askopenfilename(title=title, filetypes=filetypes)]
            root.destroy()
            return list(paths) if paths and paths[0] else None

        self.console.print(f"\n[cyan]Enter paths for: {title}[/cyan]")
        paths_input = self.console.input("[bold]Path(s): [/bold]")
        raw_paths = [p.strip().strip("'\"") for p in paths_input.split(",")]
        return [p for p in raw_paths if p and os.path.exists(p)]

    def _prompt_for_save_path(self) -> Optional[str]:
        """Prompts the user for a new file save path."""
        if TKINTER_AVAILABLE and tk is not None and filedialog is not None:
            root = tk.Tk()
            root.withdraw()

            root.lift()
            root.attributes("-topmost", True)

            save_path = filedialog.asksaveasfilename(
                title="Select Save Location",
                initialfile=self.DEFAULT_EXPORT_FILENAME,
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")],
            )
            root.destroy()
            return save_path if save_path else None

        self.console.print("\n[cyan]Please enter a save path for the export.[/cyan]")
        save_path_input = self.console.input(
            f"[bold]Save path (default: {self.DEFAULT_EXPORT_FILENAME}): [/bold]"
        )
        if not save_path_input:
            save_path_input = self.DEFAULT_EXPORT_FILENAME

        directory = os.path.dirname(save_path_input)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except OSError as e:
                self.console.print(
                    f"[red]Error: Could not create directory '{directory}'. {e}[/red]"
                )
                return None
        return save_path_input

    # --- Loading / zero-copy views ----------------------------------------------

    def _escape_sql_path(self, path: str) -> str:
        """Escapes single quotes in file paths to prevent SQL injection/errors."""
        return path.replace("'", "''")

    def _load_data(self, file_paths: List[str]) -> List[str]:
        if not self.db_connection:
            return []
        loaded_tables = []

        with self.console.status("[bold green]Linking files...[/bold green]"):
            for file_path in file_paths:
                try:
                    clean_path = str(file_path).replace("\\", "/")
                    sql_safe_path = self._escape_sql_path(clean_path)

                    ext = os.path.splitext(file_path)[1].lower()
                    raw_base = os.path.splitext(os.path.basename(file_path))[0]
                    base = normalize_name(raw_base)
                    table_name = ""

                    generated_tables = []

                    if ext == ".parquet":
                        table_name = f"{base}_parquet"
                        self.db_connection.execute(
                            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{sql_safe_path}'"
                        )
                        generated_tables.append(table_name)
                    elif ext == ".csv":
                        table_name = f"{base}_csv"
                        self.db_connection.execute(
                            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{sql_safe_path}')"
                        )
                        generated_tables.append(table_name)
                    elif ext in [".json", ".jsonl", ".ndjson"]:
                        table_name = f"{base}_json"
                        self.db_connection.execute(
                            f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_json_auto('{sql_safe_path}')"
                        )
                        generated_tables.append(table_name)

                    elif ext in [".xlsx", ".xls"]:
                        engine = "calamine" if CALAMINE_AVAILABLE else None
                        try:
                            context = pd.ExcelFile(file_path, engine=engine)
                        except Exception:
                            context = pd.ExcelFile(file_path)

                        with context as xls:
                            for sheet in xls.sheet_names:
                                df = pd.read_excel(xls, sheet_name=sheet)
                                # Use normalize_name() consistently — same logic as filename/alias normalization.
                                df.columns = [normalize_name(str(c)) for c in df.columns]
                                clean_sheet = normalize_name(sheet)

                                table_name = f"{base}_{clean_sheet}"
                                self.db_connection.register(table_name, df)

                                loaded_tables.append(table_name)
                                generated_tables.append(table_name)
                                self.recorder.record_load(file_path, table_name)

                        self.loaded_files_map[file_path] = generated_tables
                        # Schema cache is populated by the single call at the end of _load_data.
                        continue

                    else:
                        self.logger.warning(f"Skipping unsupported type: {ext}")
                        continue

                    if generated_tables:
                        loaded_tables.extend(generated_tables)
                        self.loaded_files_map[file_path] = generated_tables
                        for t in generated_tables:
                            self.recorder.record_load(file_path, t)

                except Exception as e:
                    self.logger.error(f"Failed to load '{file_path}': {e}")

        self._update_schema_cache(loaded_tables)
        self.logger.info(f"✔ Loaded {len(loaded_tables)} tables.")
        return loaded_tables

    # --- Interactive loop & meta-commands ---------------------------------------

    def _run_interactive_loop(self) -> None:
        query_buffer = ""
        completer = (
            SheetQLCompleter(self.schema_cache) if PROMPT_TOOLKIT_AVAILABLE else None
        )
        style = Style.from_dict({"prompt": "ansicyan bold"}) if Style else None

        while True:
            prompt_text = self.PROMPT_SQL if not query_buffer else self.PROMPT_CONTINUE
            try:
                if PROMPT_TOOLKIT_AVAILABLE and self.session and PygmentsLexer and SqlLexer and style:
                    line = self.session.prompt(
                        prompt_text,
                        completer=completer,
                        lexer=PygmentsLexer(SqlLexer),
                        style=style,
                    )
                else:
                    line = self.console.input(prompt_text)

                # Handle history re-run (!N) before touching the buffer.
                if line.strip().startswith("!"):
                    self._handle_history_rerun(line.strip())
                    query_buffer = ""
                    continue

                # Handle meta-commands BEFORE appending to the buffer so that
                # typing `.tables` mid-query does not corrupt the query buffer.
                if line.strip().lower().startswith("."):
                    if self._handle_meta_command(line.strip()):
                        break
                    query_buffer = ""
                    continue

                query_buffer += line + " "
            except (KeyboardInterrupt, EOFError):
                if self._handle_meta_command(".exit"):
                    break
                query_buffer = ""
                continue

            if query_buffer.strip().endswith(";"):
                query_to_run = query_buffer.strip()
                self.history.append(query_to_run)
                self._execute_query(query_to_run)
                query_buffer = ""

    def _execute_query(self, query: str) -> None:
        if not self.db_connection:
            return
        try:
            with self.console.status("[bold green]Executing...[/bold green]"):
                res = self.db_connection.execute(query).fetchdf()

            if res.empty:
                self.console.print("[yellow]No data returned.[/yellow]")
            else:
                self.logger.info("Query Successful")
                self._display_results_table(res)
                self._prompt_to_stage_results(res, query)
        except Exception as e:
            self.logger.error(f"SQL Error: {e}")

    def _prompt_to_stage_results(self, results: pd.DataFrame, query: str) -> None:
        if self.console.input("\nStage for export? (y/n): ").lower().startswith("y"):
            name = self.console.input("Sheet name: ")
            if name:
                self.results_to_save[name] = results
                self.recorder.record_query(name, query)
                self.logger.info(f"Staged '{name}'")

    # --- Exporting ---------------------------------------------------------------

    def _export_dataframe(self, df: pd.DataFrame, export: ScriptExport, default_sheet: str) -> None:
        """
        Export a single dataframe to the destination described by ScriptExport.

        Supports: .xlsx, .csv, .json (by extension). For xlsx, uses `export.sheet` or `default_sheet`.
        """
        # If no path is provided in the script, fall back to interactive selection.
        path = export.path
        if not path:
            path = self._prompt_for_save_path()
            if not path:
                self.logger.warning("Export cancelled (no destination selected).")
                return

        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        lower = path.lower()
        if lower.endswith(".xlsx"):
            sheet_name = export.sheet or default_sheet
            prev = dict(self.results_to_save)
            try:
                self.results_to_save.clear()
                self.results_to_save[sheet_name] = df
                self._save_to_excel(path)
            finally:
                self.results_to_save = prev
            return

        if lower.endswith(".csv"):
            df.to_csv(path, index=False)
            self.logger.info(f"Saved to '{os.path.basename(path)}' (csv)")
            return

        if lower.endswith(".json"):
            df.to_json(path, orient="records")
            self.logger.info(f"Saved to '{os.path.basename(path)}' (json)")
            return

        raise ValueError("Unsupported export extension. Use .xlsx, .csv, or .json.")

    @staticmethod
    def _calc_col_width(series: pd.Series, col_name: str, min_w: int = 8, max_w: int = 60) -> int:
        """Calculate column width from actual content, capped between min_w and max_w."""
        try:
            max_data = int(series.astype(str).str.len().max()) if len(series) else 0
        except Exception:
            max_data = 0
        return min(max_w, max(min_w, max_data, len(str(col_name))) + 2)

    def _save_to_excel(self, save_path: str) -> None:
        try:
            with self.console.status("[bold green]Saving Excel file...[/bold green]"):
                engine = "xlsxwriter" if XLSXWRITER_AVAILABLE else "openpyxl"
                with pd.ExcelWriter(save_path, engine=engine) as writer:
                    for sheet_name, df in self.results_to_save.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)

                        if engine == "xlsxwriter":
                            wb = writer.book
                            ws = writer.sheets[sheet_name]
                            header_fmt = wb.add_format(
                                {
                                    "bold": True,
                                    "fg_color": "#4F81BD",
                                    "font_color": "white",
                                }
                            )
                            for col_num, value in enumerate(df.columns.values):
                                ws.write(0, col_num, value, header_fmt)
                            for i, col in enumerate(df.columns):
                                ws.set_column(i, i, self._calc_col_width(df[col], col))
                            # Dropdown filter across all header columns.
                            ws.autofilter(0, 0, len(df), len(df.columns) - 1)
                        else:
                            # Operate on the specific sheet — not all worksheets — to avoid
                            # re-applying styling to previous sheets on each loop iteration.
                            ws = writer.sheets[sheet_name]
                            header_font = Font(bold=True, color="FFFFFF")
                            fill = PatternFill(
                                start_color="4F81BD",
                                end_color="4F81BD",
                                fill_type="solid",
                            )
                            for cell in ws[1]:
                                cell.font = header_font
                                cell.fill = fill
                            for i, col in enumerate(df.columns):
                                col_letter = ws.cell(row=1, column=i + 1).column_letter
                                ws.column_dimensions[col_letter].width = self._calc_col_width(df[col], col)
                            ws.auto_filter.ref = ws.dimensions

            self.logger.info(f"Saved to '{os.path.basename(save_path)}' ({engine})")
            self.recorder.record_export(save_path)
            self.results_to_save.clear()
        except Exception as e:
            self.logger.error(f"Save failed: {e}")

    def _export_results(self) -> None:
        """Exports staged results using the correct save path."""
        if not self.results_to_save:
            self.logger.warning("Nothing to export.")
            return

        if path := self._prompt_for_save_path():
            self._save_to_excel(path)

    # --- Meta commands & helpers -------------------------------------------------

    def _handle_meta_command(self, command_str: str) -> bool:
        parts = command_str.split()
        cmd = parts[0].lower()

        commands = {
            ".exit": lambda: True,
            ".quit": lambda: True,
            ".help": self._show_help,
            ".tables": self._list_tables,
            ".schema": lambda: self._describe_table(parts),
            ".history": self._show_history,
            ".load": self._add_new_files,
            ".export": self._export_results,
            ".dump": lambda: self._dump_script(parts),
            ".runscript": lambda: self._run_script_interactive(parts),
            ".rename": lambda: self._rename_table(parts),
        }

        if cmd not in commands:
            self.logger.warning(f"Unknown command: {cmd}")
            return False

        should_exit = commands[cmd]()
        if should_exit and cmd in [".exit", ".quit"] and self.results_to_save:
            if (
                self.console.input("Export staged results? (y/n): ")
                .lower()
                .startswith("y")
            ):
                self._export_results()
        return should_exit

    def _dump_script(self, parts: List[str]) -> None:
        filename = parts[1] if len(parts) > 1 else "script.yaml"
        try:
            yaml_content = self.recorder.generate_yaml()
            with open(filename, "w") as f:
                f.write(yaml_content)
            self.logger.info(f"Session dumped to '[bold cyan]{filename}[/bold cyan]'")
        except Exception as e:
            self.logger.error(f"Failed to dump script: {e}")

    def _show_help(self) -> None:
        self.console.print("\n[bold]Commands:[/bold]")
        self.console.print(
            "  .help, .tables, .schema <t>, .history, .load, .rename <o> <n>, .export, .exit"
        )
        self.console.print(
            "  [bold yellow].dump <file>[/bold yellow]   Save current session to YAML"
        )
        self.console.print(
            "  [bold yellow].runscript <file>[/bold yellow] Run a YAML script"
        )

    def _list_tables(self) -> None:
        if self.db_connection:
            try:
                tables = self.db_connection.execute("SHOW TABLES").fetchdf()["name"]
                self.console.print(f"\n[cyan]Tables ({len(tables)}):[/cyan]")
                for t in tables:
                    self.console.print(f" - {t}")
            except Exception:
                pass

    def _describe_table(self, parts: List[str]) -> None:
        if len(parts) == 2 and self.db_connection:
            try:
                df = self.db_connection.execute(f"DESCRIBE {parts[1]}").fetchdf()
                t = Table(title=f"Schema: {parts[1]}")
                for c in df.columns:
                    t.add_column(c)
                for _, r in df.iterrows():
                    t.add_row(*[str(x) for x in r])
                self.console.print(t)
            except Exception as e:
                self.logger.error(str(e))

    def _rename_table(self, parts: List[str]) -> None:
        if len(parts) == 3:
            try:
                old = parts[1]
                new_raw = parts[2]
                new = normalize_name(new_raw)

                # Detect whether the object is a VIEW or a registered DataFrame (BASE TABLE)
                # so we issue the correct DDL. ALTER VIEW fails on registered DataFrames and
                # ALTER TABLE fails on views.
                try:
                    type_df = self.db_connection.execute(
                        "SELECT table_type FROM information_schema.tables WHERE table_name = ?",
                        [old],
                    ).fetchdf()
                    obj_type = type_df.iloc[0]["table_type"] if not type_df.empty else "VIEW"
                except Exception:
                    obj_type = "VIEW"  # safe fallback

                if obj_type == "VIEW":
                    self.db_connection.execute(f'ALTER VIEW "{old}" RENAME TO "{new}"')
                else:
                    self.db_connection.execute(f'ALTER TABLE "{old}" RENAME TO "{new}"')

                self.logger.info(f"Renamed {old} -> {new}")

                # Update schema cache.
                actual_key = next(
                    (k for k in self.schema_cache if k.lower() == old.lower()), old
                )
                if actual_key in self.schema_cache:
                    self.schema_cache[new] = self.schema_cache.pop(actual_key)

                # Update loaded_files_map so YAML alias resolution stays consistent.
                for file_path, tables in self.loaded_files_map.items():
                    self.loaded_files_map[file_path] = [
                        new if t == old else t for t in tables
                    ]

            except Exception as e:
                self.logger.error(str(e))

    def _show_history(self) -> None:
        for i, c in enumerate(self.history, 1):
            self.console.print(f"{i}: {c}")

    def _handle_history_rerun(self, cmd: str) -> None:
        try:
            idx = int(cmd[1:])
            if 1 <= idx <= len(self.history):
                self._execute_query(self.history[idx - 1])
        except Exception:
            pass

    def _add_new_files(self) -> None:
        if paths := self._prompt_for_paths("Select Files", [("All", "*.*")], True):
            self._load_data(paths)

    # --- YAML scripting ----------------------------------------------------------

    def _run_script_interactive(self, parts: List[str]) -> None:
        script_path = parts[1] if len(parts) > 1 else None
        if not script_path:
            self.logger.warning("Usage: .runscript <file>")
            return
        if not YAML_AVAILABLE or yaml is None:
            self.logger.error("PyYAML missing.")
            return

        try:
            with open(script_path, "r") as f:
                config = yaml.safe_load(f)
            self._execute_yaml_script(config)
        except Exception as e:
            self.logger.error(f"Script Error: {e}")

    def _execute_yaml_script(self, config: Dict[str, Any]) -> None:
        """Execute operations from YAML config with validation and options."""
        if not self.db_connection:
            self._init_db()

        try:
            inputs, tasks, export, options = parse_script_config(config)
        except ScriptConfigError as e:
            self.logger.error(f"Invalid script config: {e}")
            return

        if options.memory_limit:
            try:
                self.db_connection.execute(f"SET memory_limit='{options.memory_limit}';")
            except Exception:
                self.logger.debug("DuckDB memory limit config failed. Using defaults.")

        if inputs:
            self._load_data([i.path for i in inputs])

            for item in inputs:
                if not item.alias:
                    continue

                found_tables = resolve_alias_targets(self.loaded_files_map, item.path)
                if not found_tables:
                    self.logger.warning(f"No loaded tables found for input '{item.path}'.")
                    continue

                alias_safe = normalize_name(item.alias)

                if len(found_tables) == 1:
                    self.db_connection.execute(
                        f'ALTER VIEW "{found_tables[0]}" RENAME TO "{alias_safe}"'
                    )
                    self.logger.info(f"Aliased {found_tables[0]} -> {alias_safe}")
                else:
                    for tbl in found_tables:
                        suffix = tbl.split("_", 1)[-1] if "_" in tbl else "sheet"
                        new_name = normalize_name(f"{item.alias}_{suffix}")
                        self.db_connection.execute(
                            f'ALTER VIEW "{tbl}" RENAME TO "{new_name}"'
                        )
                        self.logger.info(f"Aliased {tbl} -> {new_name}")

        for task in tasks:
            try:
                df = self.db_connection.execute(task.sql).fetchdf()
                if task.export:
                    try:
                        self._export_dataframe(df, task.export, default_sheet=task.name)
                    except Exception as e:
                        self.logger.error(f"Export for task '{task.name}' failed: {e}")
                        if options.stop_on_error:
                            break
                else:
                    self.results_to_save[task.name] = df
                self.logger.info(f"Task '{task.name}' complete.")
            except Exception as e:
                self.logger.error(f"Task '{task.name}' failed: {e}")
                if options.stop_on_error:
                    break

        if export:
            self._save_to_excel(export.path)


__all__ = ["SheetQL"]

