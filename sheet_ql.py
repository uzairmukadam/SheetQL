import os
import re
import argparse
from collections import deque
import pandas as pd
import duckdb
from typing import Any

from rich.console import Console
from rich.table import Table

from openpyxl.styles import Font, PatternFill

try:
    import yaml

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import tkinter as tk
    from tkinter import filedialog

    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False


class SheetQL:
    """An interactive command-line tool to run SQL queries on data files."""

    PROMPT_SQL = "SQL> "
    PROMPT_CONTINUE = "  -> "
    DEFAULT_EXPORT_FILENAME = "query_result.xlsx"
    HISTORY_MAX_LEN = 50

    def __init__(self) -> None:
        """Initializes the SheetQL tool."""
        self.console = Console()
        self.db_connection: duckdb.DuckDBPyConnection | None = None
        self.results_to_save: dict[str, pd.DataFrame] = {}
        self.history: deque[str] = deque(maxlen=self.HISTORY_MAX_LEN)

    def run_interactive(self) -> None:
        """Starts and runs the main application lifecycle for interactive mode."""
        try:
            self._display_welcome()
            self.db_connection = duckdb.connect(database=":memory:")

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
                if initial_dataframes := self._load_data(initial_paths):
                    self._register_dataframes(initial_dataframes)
                    self.console.print(
                        "\n[bold green]--- 🦆 DuckDB is ready ---[/bold green]"
                    )
                    self._list_tables()
                    self._run_interactive_loop()

        except Exception as e:
            self.console.print(
                f"[bold red]An unexpected error occurred: {e}[/bold red]"
            )
        finally:
            self.console.print("\n[bold cyan]👋 Goodbye![/bold cyan]")

    def run_batch(self, config_path: str) -> None:
        """Runs the application in batch mode using a YAML config file."""
        self.console.print(
            f"[bold cyan]🚀 Starting batch mode with config: '{config_path}'[/bold cyan]"
        )

        if not YAML_AVAILABLE:
            self.console.print(
                "[bold red]❌ Error: PyYAML is not installed. Please run 'pip install pyyaml' to use batch mode.[/bold red]"
            )
            return

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
        except FileNotFoundError:
            self.console.print(
                f"[bold red]❌ Error: Configuration file not found at '{config_path}'[/bold red]"
            )
            return
        except yaml.YAMLError as e:
            self.console.print(f"[bold red]❌ Error parsing YAML file: {e}[/bold red]")
            return

        self.db_connection = duckdb.connect(database=":memory:")
        self._execute_yaml_script(config)

    def _display_welcome(self) -> None:
        """Prints a welcome message to the console."""
        self.console.print(
            "[bold green]--- SheetQL: Interactive SQL Query Tool for Spreadsheet Files ---[/bold green]"
        )
        self.console.print("Type your SQL query and end it with a semicolon ';'.")
        self.console.print(
            "Type [bold yellow].help[/bold yellow] for a list of commands."
        )
        if not TKINTER_AVAILABLE:
            self.console.print(
                "[yellow]Note: Tkinter not found. Using command-line for file selection.[/yellow]"
            )

    def _prompt_for_paths(
        self, title: str, filetypes: list[tuple[str, str]], allow_multiple: bool
    ) -> list[str] | None:
        """Generic method to get file paths from the user via GUI or CLI."""
        if TKINTER_AVAILABLE:
            root = tk.Tk()
            root.withdraw()
            if allow_multiple:
                paths = filedialog.askopenfilenames(title=title, filetypes=filetypes)
            else:
                paths = [filedialog.askopenfilename(title=title, filetypes=filetypes)]
            root.destroy()
            return list(paths) if paths and paths[0] else None

        self.console.print(f"\n[cyan]Please enter the path(s) for: {title}[/cyan]")
        if allow_multiple:
            self.console.print(
                "[cyan]You can enter multiple paths separated by commas.[/cyan]"
            )

        paths_input = self.console.input("[bold]File path(s): [/bold]")
        raw_paths = [p.strip().strip("'\"") for p in paths_input.split(",")]
        valid_paths = [
            p for p in raw_paths if p and os.path.exists(p) and os.path.isfile(p)
        ]

        if not valid_paths:
            self.console.print("[red]Error: No valid file paths were provided.[/red]")
            return None

        return valid_paths

    def _load_data(self, file_paths: list[str]) -> dict[str, pd.DataFrame]:
        """Loads all supported files into pandas DataFrames."""
        all_dataframes = {}
        with self.console.status("[bold green]Loading data files...[/bold green]"):
            for file_path in file_paths:
                try:
                    ext = os.path.splitext(file_path)[1].lower()
                    base_name = re.sub(
                        r"[^a-zA-Z0-9_]+",
                        "_",
                        os.path.splitext(os.path.basename(file_path))[0],
                    )
                    table_name: str = ""

                    if ext in [".xlsx", ".xls"]:
                        xls = pd.ExcelFile(file_path)
                        for sheet_name in xls.sheet_names:
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            clean_sheet = re.sub(r"[^a-zA-Z0-9_]+", "_", sheet_name)
                            table_name = (
                                f"{base_name}{ext.replace('.', '_')}_{clean_sheet}"
                            )
                            all_dataframes[table_name] = df
                        continue
                    elif ext == ".csv":
                        df = pd.read_csv(file_path)
                        table_name = f"{base_name}_csv"
                    elif ext == ".parquet":
                        df = pd.read_parquet(file_path)
                        table_name = f"{base_name}_parquet"
                    elif ext in [".json", ".jsonl", ".ndjson"]:
                        df = pd.read_json(file_path, lines=ext in [".jsonl", ".ndjson"])
                        table_name = f"{base_name}_json"
                    else:
                        self.console.print(
                            f"[yellow]Warning: Unsupported file type '{ext}'. Skipping.[/yellow]"
                        )
                        continue

                    all_dataframes[table_name] = df
                except Exception as e:
                    self.console.print(
                        f"[bold red]Error loading '{file_path}': {e}[/bold red]"
                    )
        self.console.print(
            f"[green]✔ Loaded {len(all_dataframes)} table(s) from {len(file_paths)} file(s).[/green]"
        )
        return all_dataframes

    def _register_dataframes(self, dataframes: dict[str, pd.DataFrame]) -> None:
        """Registers new DataFrames as views in the existing DuckDB connection."""
        if not self.db_connection:
            return
        for table_name, df in dataframes.items():
            self.db_connection.register(table_name, df)

    def _run_interactive_loop(self) -> None:
        """Runs the main loop for accepting and executing user queries."""
        query_buffer = ""
        while True:
            prompt = self.PROMPT_SQL if not query_buffer else self.PROMPT_CONTINUE
            try:
                line = self.console.input(prompt)

                if line.strip().startswith("!"):
                    self._handle_history_rerun(line.strip())
                    query_buffer = ""
                    continue

                query_buffer += line + " "
            except (KeyboardInterrupt, EOFError):
                if self._handle_meta_command(".exit"):
                    break
                query_buffer = ""
                self.console.print()
                continue

            if line.strip().lower().startswith("."):
                if self._handle_meta_command(line.strip()):
                    break
                query_buffer = ""
                continue

            if query_buffer.strip().endswith(";"):
                query_to_run = query_buffer.strip()
                self.history.append(query_to_run)
                self._execute_query(query_to_run)
                query_buffer = ""

    def _handle_meta_command(self, command_str: str) -> bool:
        """Handles meta-commands. Returns True if the app should exit."""
        parts = command_str.split()
        command = parts[0].lower()
        commands: dict[str, Any] = {
            ".exit": lambda: True,
            ".quit": lambda: True,
            ".help": self._show_help,
            ".tables": self._list_tables,
            ".schema": lambda: self._describe_table(parts),
            ".history": self._show_history,
            ".load": self._add_new_files,
            ".runscript": lambda: self._run_script_interactive(parts),
            ".rename": lambda: self._rename_table(parts),
            ".export": self._export_results,
        }
        if command in commands:
            if command in [".exit", ".quit"]:
                if self.results_to_save:
                    choice = self.console.input(
                        "Export staged results before quitting? (y/n): "
                    ).lower()
                    if choice.startswith("y"):
                        self._export_results()
                return True
        else:
            self.console.print(
                f"[red]Unknown command: '{command}'. Type .help for assistance.[/red]"
            )
        return False

    def _show_help(self) -> None:
        """Prints the help message with available commands."""
        self.console.print("\n[bold]Available Commands:[/bold]")
        self.console.print("[yellow].help[/yellow]         - Show this help message.")
        self.console.print(
            "[yellow].tables[/yellow]       - List all available tables."
        )
        self.console.print(
            "[yellow].schema <table>[/yellow] - Describe a table's columns and types."
        )
        self.console.print("[yellow].history[/yellow]      - Show previous queries.")
        self.console.print(
            "[yellow].load[/yellow]         - Load additional data files."
        )
        self.console.print(
            "[yellow].runscript [path][/yellow]- Execute a YAML script file."
        )
        self.console.print("[yellow].rename <o> <n>[/yellow]- Rename a table.")
        self.console.print(
            "[yellow].export[/yellow]       - Export staged results to Excel."
        )
        self.console.print("[yellow].exit / .quit[/yellow] - Exit the application.")
        self.console.print("\nEnd any SQL query with a semicolon ';'.")
        self.console.print("Re-run history item with [yellow]!N[/yellow] (e.g., !5).")

    def _execute_query(self, query: str) -> None:
        """Executes a SQL query and handles the results."""
        if not self.db_connection:
            return
        try:
            with self.console.status("[bold green]Executing query...[/bold green]"):
                results = self.db_connection.execute(query).fetchdf()
        except Exception as e:
            self.console.print(f"\n[bold red]❌ SQL Error: {e}[/bold red]")
            return

        if results.empty:
            self.console.print(
                "\n[bold yellow]✅ Query returned no data.[/bold yellow]"
            )
            return

        self.console.print("\n[bold green]✅ Query Successful![/bold green]")
        self._display_results_table(results)
        self._prompt_to_stage_results(results)

    def _display_results_table(self, df: pd.DataFrame) -> None:
        """Displays a pandas DataFrame in a rich Table."""
        table = Table(show_header=True, header_style="bold magenta")
        for col in df.columns:
            table.add_column(str(col))
        for _, row in df.head(20).iterrows():
            table.add_row(*[str(item) for item in row])
        self.console.print(table)
        if len(df) > 20:
            self.console.print(f"... (and {len(df) - 20} more rows)")

    def _prompt_to_stage_results(self, results: pd.DataFrame) -> None:
        """Asks the user if they want to stage the results for export."""
        choice = self.console.input(
            "\n[bold]💾 Stage these results for export? (y/n): [/bold]"
        ).lower()
        if choice.startswith("y"):
            sheet_name = self.console.input(
                "[bold]    Enter a name for the Excel sheet: [/bold]"
            )
            if sheet_name:
                self.results_to_save[sheet_name] = results
                self.console.print(
                    "[green]    ✔ Results staged. Use .export to save.[/green]"
                )
            else:
                self.console.print(
                    "[yellow]    Sheet name cannot be empty. Results not staged.[/yellow]"
                )

    def _export_results(self) -> None:
        """Exports all staged results to a single, formatted Excel file."""
        if not self.results_to_save:
            self.console.print("[yellow]No results are staged for export.[/yellow]")
            return

        save_path = self._prompt_for_paths(
            title="Select Save Location",
            filetypes=[("Excel Files", "*.xlsx")],
            allow_multiple=False,
        )

        if save_path and save_path[0]:
            self._save_to_excel(save_path[0])
        else:
            self.console.print("[yellow]Save operation cancelled.[/yellow]")

    def _save_to_excel(self, save_path: str) -> None:
        """Saves all staged results to a single, formatted Excel file."""
        try:
            with self.console.status(
                "[bold green]Saving and formatting Excel file...[/bold green]"
            ):
                with pd.ExcelWriter(save_path, engine="openpyxl") as writer:
                    for sheet_name, df in self.results_to_save.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    self._format_excel_sheets(writer)
            self.console.print(
                f"\n[bold green]✨ Success! Results saved to '{os.path.basename(save_path)}'[/bold green]"
            )
            self.results_to_save.clear()
        except Exception as e:
            self.console.print(f"[bold red]Error during save: {e}[/bold red]")

    def _format_excel_sheets(self, writer: pd.ExcelWriter) -> None:
        """Applies professional formatting to all sheets in the Excel workbook."""
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(
            start_color="4F81BD", end_color="4F81BD", fill_type="solid"
        )

        for worksheet in writer.book.worksheets:
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill

            for column_cells in worksheet.columns:
                max_length = max(
                    len(str(cell.value))
                    for cell in column_cells
                    if cell.value is not None
                )
                worksheet.column_dimensions[column_cells[0].column_letter].width = (
                    max_length + 2
                )

            worksheet.auto_filter.ref = worksheet.dimensions

    def _list_tables(self) -> None:
        """Prints the list of available tables."""
        if not self.db_connection:
            return
        try:
            tables = self.db_connection.execute("SHOW TABLES;").fetchdf()
            self.console.print("\n[bold cyan]Available Tables:[/bold cyan]")
            for name in tables["name"]:
                self.console.print(f"  - {name}")
        except Exception as e:
            self.console.print(f"[bold red]Could not list tables: {e}[/bold red]")

    def _add_new_files(self) -> None:
        """Handles the logic for loading additional files into the session."""
        self.console.print("\n[cyan]Select additional data files to load...[/cyan]")
        if new_paths := self._prompt_for_paths(
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
            if new_dataframes := self._load_data(new_paths):
                self._register_dataframes(new_dataframes)
                self.console.print("[green]✔ New files loaded and registered.[/green]")
                self._list_tables()

    def _describe_table(self, command_parts: list[str]) -> None:
        """Shows the schema for a given table."""
        if len(command_parts) != 2:
            self.console.print("[red]Usage: .schema <table_name>[/red]")
            return

        table_name = command_parts[1]
        if not self.db_connection:
            return

        try:
            schema_df = self.db_connection.execute(
                f'DESCRIBE "{table_name}";'
            ).fetchdf()
            table = Table(
                title=f"Schema for '{table_name}'", header_style="bold magenta"
            )
            for col in schema_df.columns:
                table.add_column(col.replace("_", " ").title())
            for _, row in schema_df.iterrows():
                table.add_row(*[str(item) for item in row])
            self.console.print(table)
        except Exception as e:
            self.console.print(f"[bold red]❌ Error describing table: {e}[/bold red]")

    def _rename_table(self, command_parts: list[str]) -> None:
        """Renames a view in the database."""
        if len(command_parts) != 3:
            self.console.print(
                "[red]Usage: .rename <old_table_name> <new_table_name>[/red]"
            )
            return

        old_name, new_name = command_parts[1], command_parts[2]
        if not self.db_connection:
            return
        try:
            self.db_connection.execute(
                f'ALTER VIEW "{old_name}" RENAME TO "{new_name}";'
            )
            self.console.print(
                f"[green]✔ Table view '{old_name}' renamed to '{new_name}'.[/green]"
            )
        except Exception as e:
            self.console.print(f"[bold red]❌ Error renaming view: {e}[/bold red]")

    def _show_history(self) -> None:
        """Displays the command history."""
        self.console.print("\n[bold cyan]Query History:[/bold cyan]")
        if not self.history:
            self.console.print("  No history yet.")
            return
        for i, command in enumerate(self.history, 1):
            self.console.print(f"  [yellow]{i:2d}[/yellow]: {command}")

    def _handle_history_rerun(self, command_str: str) -> None:
        """Executes a query from history via !N command."""
        try:
            index = int(command_str[1:])
            if 1 <= index <= len(self.history):
                query = self.history[index - 1]
                self.console.print(f"[cyan]Re-running query {index}:[/cyan] {query}")
                self.history.append(query)
                self._execute_query(query)
            else:
                self.console.print(
                    f"[red]Error: History index {index} is out of bounds.[/red]"
                )
        except (ValueError, IndexError):
            self.console.print(
                "[red]Invalid history command. Use !N where N is a number.[/red]"
            )

    def _run_script_interactive(self, command_parts: list[str]) -> None:
        """Handles the .runscript meta-command."""
        script_path = command_parts[1] if len(command_parts) == 2 else None
        if not script_path:
            paths = self._prompt_for_paths(
                title="Select a YAML Script File",
                filetypes=[("YAML Scripts", "*.yml *.yaml"), ("All files", "*.*")],
                allow_multiple=False,
            )
            if not paths:
                self.console.print("[yellow]Script execution cancelled.")
                return
            script_path = paths[0]

        if not YAML_AVAILABLE:
            self.console.print("[bold red]❌ PyYAML not installed.[/bold red]")
            return

        try:
            with open(script_path, "r") as f:
                config = yaml.safe_load(f)
            self.console.print(
                f"[bold cyan]🚀 Executing script: '{script_path}'[/bold cyan]"
            )
            self._execute_yaml_script(config)
            self.console.print(
                "[bold green]✔ Script finished. Current tables:[/bold green]"
            )
            self._list_tables()
        except FileNotFoundError:
            self.console.print(
                f"[bold red]❌ Script file not found: '{script_path}'[/bold red]"
            )
        except yaml.YAMLError as e:
            self.console.print(f"[bold red]❌ Error parsing YAML file: {e}[/bold red]")

    def _execute_yaml_script(self, config: dict[str, Any]) -> None:
        """Processes the actions defined in a parsed YAML script."""
        if "inputs" in config:
            self._process_yaml_inputs(config.get("inputs", []))
        if "tasks" in config:
            self._process_yaml_tasks(config.get("tasks", []))
        if "export" in config:
            self._process_yaml_export(config.get("export", {}))

    def _process_yaml_inputs(self, inputs: list[dict[str, str]]) -> None:
        """Loads and aliases data files from a YAML script's 'inputs' block."""
        self.console.print("\n[bold]--- 1. Loading Input Files ---[/bold]")
        if not inputs:
            self.console.print("[yellow]Inputs section is empty.")
            return

        paths_to_load = [item["path"] for item in inputs]
        loaded_dfs = self._load_data(paths_to_load)
        alias_map = {
            os.path.basename(item["path"]): item.get("alias")
            for item in inputs
            if item.get("alias")
        }

        for original_name, df in loaded_dfs.items():
            final_name = original_name
            for filename, alias in alias_map.items():
                base_file = re.sub(
                    r"[^a-zA-Z0-9_]+", "_", os.path.splitext(filename)[0]
                )
                ext = os.path.splitext(filename)[1].lower().replace(".", "_")

                if original_name == f"{base_file}{ext}":
                    final_name = alias
                    break
                elif original_name.startswith(f"{base_file}{ext}_"):
                    sheet_part = original_name.split(f"{base_file}{ext}_", 1)[1]
                    final_name = f"{alias}_{sheet_part}"
                    break

            if self.db_connection:
                self.db_connection.register(final_name, df)
        self.console.print("[green]✔ All specified inputs loaded and aliased.[/green]")

    def _process_yaml_tasks(self, tasks: list[dict[str, str]]) -> None:
        """Executes queries from a YAML script's 'tasks' block."""
        self.console.print("\n[bold]--- 2. Executing Tasks ---[/bold]")
        if not tasks:
            self.console.print("[yellow]Tasks section is empty.")
            return

        for i, task in enumerate(tasks):
            name, sql = task.get("name"), task.get("sql")
            if not (name and sql and self.db_connection):
                continue

            self.console.print(f"Executing task: [cyan]'{name}'[/cyan]...")
            try:
                self.results_to_save[name] = self.db_connection.execute(sql).fetchdf()
                self.console.print(
                    f"[green]  ✔ Success! {len(self.results_to_save[name])} rows staged.[/green]"
                )
            except Exception as e:
                self.console.print(
                    f"[bold red]  ❌ Error in task '{name}': {e}[/bold red]"
                )

    def _process_yaml_export(self, export_config: dict[str, str]) -> None:
        """Exports staged results based on a YAML script's 'export' block."""
        self.console.print("\n[bold]--- 3. Exporting Results ---[/bold]")
        if not export_config:
            self.console.print("[yellow]Export section is empty.")
            return

        if self.results_to_save:
            self._save_to_excel(export_config.get("path", self.DEFAULT_EXPORT_FILENAME))
        else:
            self.console.print("[yellow]No results were staged for export.[/yellow]")


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="SheetQL: Run SQL on local data files."
    )
    parser.add_argument(
        "-r",
        "--run",
        dest="config_path",
        help="Run in batch mode with a specified YAML configuration file.",
        metavar="FILE",
    )
    args = parser.parse_args()

    tool = SheetQL()
    if args.config_path:
        tool.run_batch(args.config_path)
    else:
        tool.run_interactive()


if __name__ == "__main__":
    main()
