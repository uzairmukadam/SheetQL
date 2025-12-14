import os
import re
import argparse
import sys
from collections import deque
from typing import Any, Optional, List, Tuple, Dict

import pandas as pd
import duckdb
from rich.console import Console
from rich.table import Table
from openpyxl.styles import Font, PatternFill

try:
    import python_calamine
    CALAMINE_AVAILABLE = True
except ImportError:
    CALAMINE_AVAILABLE = False

try:
    import xlsxwriter
    XLSXWRITER_AVAILABLE = True
except ImportError:
    XLSXWRITER_AVAILABLE = False

try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.lexers import PygmentsLexer
    from pygments.lexers.sql import SqlLexer
    from prompt_toolkit.styles import Style
    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False

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


class SheetQLCompleter(Completer):
    """
    Optimized completer that uses a local schema cache instead of
    querying the database on every keystroke.
    """
    def __init__(self, schema_cache: Dict[str, List[str]]):
        self.schema_cache = schema_cache
        self.keywords = [
            "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "JOIN",
            "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "ON", "AS", "DISTINCT",
            "COUNT", "SUM", "AVG", "MIN", "MAX", "HAVING", "CASE", "WHEN",
            "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN", "IS NULL",
            "IS NOT NULL", "LIKE", "ILIKE", "CAST", "DESCRIBE", "SHOW TABLES",
            "PRAGMA", "EXPORT", "PIVOT", "UNION", "ALL"
        ]

    def get_completions(self, document, complete_event):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        text = document.text_before_cursor.upper()
        
        tables = list(self.schema_cache.keys())

        parts = text.split()
        last_word = parts[-2] if len(parts) > 1 else ""
        
        suggestions = []

        if last_word in ["FROM", "JOIN", "UPDATE", "INTO", "DESCRIBE"]:
            suggestions.extend([(t, "Table") for t in tables])
        
        else:
            suggestions.extend([(k, "Keyword") for k in self.keywords])
            suggestions.extend([(t, "Table") for t in tables])
            
            for table_name in tables:
                if table_name in document.text:
                    columns = self.schema_cache.get(table_name, [])
                    suggestions.extend([(c, f"Column ({table_name})") for c in columns])

        for suggestion, meta in suggestions:
            if suggestion.lower().startswith(word_before_cursor.lower()):
                yield Completion(
                    suggestion, 
                    start_position=-len(word_before_cursor), 
                    display_meta=meta
                )


class SheetQL:
    """An interactive command-line tool to run SQL queries on data files."""

    PROMPT_SQL = "SQL> "
    PROMPT_CONTINUE = "  -> "
    DEFAULT_EXPORT_FILENAME = "query_result.xlsx"
    HISTORY_MAX_LEN = 50

    def __init__(self) -> None:
        self.console = Console()
        self.db_connection: Optional[duckdb.DuckDBPyConnection] = None
        self.results_to_save: Dict[str, pd.DataFrame] = {}
        self.history: deque[str] = deque(maxlen=self.HISTORY_MAX_LEN)
        
        self.schema_cache: Dict[str, List[str]] = {}
        
        self.session = None
        if PROMPT_TOOLKIT_AVAILABLE:
            self.session = PromptSession(history=None)

    def run_interactive(self) -> None:
        """Starts and runs the main application lifecycle for interactive mode."""
        try:
            self._display_welcome()
            
            self.db_connection = duckdb.connect(database=":memory:")
            
            try:
                self.db_connection.execute("SET memory_limit='75%';")
            except (duckdb.ParserException, duckdb.CatalogException, duckdb.BinderException):
                self.console.print("[yellow]Note: Auto-memory tuning unavailable. Using system defaults.[/yellow]")

            if initial_paths := self._prompt_for_paths(
                title="Select Data Files",
                filetypes=[
                    ("Supported Files", "*.xlsx *.xls *.csv *.parquet *.json *.jsonl *.ndjson"),
                    ("All files", "*.*"),
                ],
                allow_multiple=True,
            ):
                self._load_data(initial_paths)
                self.console.print("\n[bold green]--- 🦆 DuckDB is ready ---[/bold green]")
                self._list_tables()
                self._run_interactive_loop()

        except Exception as e:
            self.console.print(f"[bold red]An unexpected error occurred: {e}[/bold red]")
        finally:
            self.console.print("\n[bold cyan]👋 Goodbye![/bold cyan]")

    def run_batch(self, config_path: str) -> None:
        """Runs the application in batch mode using a YAML config file."""
        self.console.print(f"[bold cyan]🚀 Starting batch mode with config: '{config_path}'[/bold cyan]")

        if not YAML_AVAILABLE:
            self.console.print("[bold red]❌ Error: PyYAML is not installed.[/bold red]")
            return

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
        except Exception as e:
            self.console.print(f"[bold red]❌ Error loading YAML: {e}[/bold red]")
            return

        self.db_connection = duckdb.connect(database=":memory:")
        
        try:
            self.db_connection.execute("SET memory_limit='75%';")
        except Exception:
            pass
            
        self._execute_yaml_script(config)

    def _display_welcome(self) -> None:
        self.console.print("[bold green]--- SheetQL: Professional Data Analysis Tool ---[/bold green]")
        self.console.print("Type your SQL query and end it with a semicolon ';'.")
        self.console.print("Type [bold yellow].help[/bold yellow] for a list of commands.")
        
        status = []
        status.append("[green]Rust-Excel[/green]" if CALAMINE_AVAILABLE else "[red]Rust-Excel (Slow)[/red]")
        status.append("[green]Stream-Write[/green]" if XLSXWRITER_AVAILABLE else "[red]Stream-Write (Slow)[/red]")
        status.append("[green]Autocomplete[/green]" if PROMPT_TOOLKIT_AVAILABLE else "[red]Autocomplete (Missing)[/red]")
        self.console.print(f"Engine Status: {', '.join(status)}")

    def _prompt_for_paths(self, title: str, filetypes: List[Tuple[str, str]], allow_multiple: bool) -> Optional[List[str]]:
        """Generic method to get existing file paths from the user."""
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
            self.console.print("[cyan]Separate multiple paths with commas.[/cyan]")

        paths_input = self.console.input("[bold]File path(s): [/bold]")
        raw_paths = [p.strip().strip("'\"") for p in paths_input.split(",")]
        valid_paths = [p for p in raw_paths if p and os.path.exists(p)]
        
        if not valid_paths:
            self.console.print("[red]No valid paths provided.[/red]")
            return None
            
        return valid_paths

    def _load_data(self, file_paths: List[str]) -> List[str]:
        """
        Loads data using Zero-Copy and Native Rust engines where possible.
        Automatically updates the Schema Cache for autocomplete.
        """
        if not self.db_connection: return []

        loaded_tables = []
        
        with self.console.status("[bold green]Linking files to DuckDB...[/bold green]"):
            for file_path in file_paths:
                try:
                    clean_path = str(file_path).replace("\\", "/")
                    ext = os.path.splitext(file_path)[1].lower()
                    base_name = re.sub(r"[^a-zA-Z0-9_]+", "_", os.path.splitext(os.path.basename(file_path))[0])
                    
                    if ext == ".parquet":
                        tbl = f"{base_name}_parquet"
                        self.db_connection.execute(f"CREATE OR REPLACE VIEW {tbl} AS SELECT * FROM '{clean_path}'")
                        loaded_tables.append(tbl)

                    elif ext == ".csv":
                        tbl = f"{base_name}_csv"
                        self.db_connection.execute(f"CREATE OR REPLACE VIEW {tbl} AS SELECT * FROM read_csv_auto('{clean_path}')")
                        loaded_tables.append(tbl)

                    elif ext in [".json", ".jsonl", ".ndjson"]:
                        tbl = f"{base_name}_json"
                        self.db_connection.execute(f"CREATE OR REPLACE VIEW {tbl} AS SELECT * FROM read_json_auto('{clean_path}')")
                        loaded_tables.append(tbl)

                    elif ext in [".xlsx", ".xls"]:
                        engine = "calamine" if CALAMINE_AVAILABLE else None
                        
                        try:
                            xls = pd.ExcelFile(file_path, engine=engine)
                        except Exception:
                            xls = pd.ExcelFile(file_path)

                        for sheet_name in xls.sheet_names:
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            
                            df.columns = [
                                re.sub(r"[^a-zA-Z0-9_]+", "_", str(col).strip()).lower() 
                                for col in df.columns
                            ]
                            
                            clean_sheet = re.sub(r"[^a-zA-Z0-9_]+", "_", sheet_name)
                            tbl = f"{base_name}_{clean_sheet}"
                            
                            self.db_connection.register(tbl, df)
                            loaded_tables.append(tbl)
                    
                    else:
                        self.console.print(f"[yellow]Skipping unsupported file: {ext}[/yellow]")

                except Exception as e:
                    self.console.print(f"[bold red]Error loading '{file_path}': {e}[/bold red]")

        self._update_schema_cache(loaded_tables)
        
        self.console.print(f"[green]✔ Linked {len(loaded_tables)} table(s).[/green]")
        return loaded_tables

    def _update_schema_cache(self, table_names: List[str]) -> None:
        """Fetches columns for tables and stores them in RAM."""
        if not self.db_connection: return
        
        for table in table_names:
            try:
                schema_df = self.db_connection.execute(f"DESCRIBE {table}").fetchdf()
                self.schema_cache[table] = schema_df['column_name'].tolist()
            except Exception:
                pass

    def _run_interactive_loop(self) -> None:
        """Runs the main loop using prompt_toolkit if available."""
        query_buffer = ""
        
        completer = None
        if PROMPT_TOOLKIT_AVAILABLE and self.db_connection:
            completer = SheetQLCompleter(self.schema_cache)

        style = Style.from_dict({'prompt': 'ansicyan bold'})

        while True:
            prompt_text = self.PROMPT_SQL if not query_buffer else self.PROMPT_CONTINUE
            
            try:
                if PROMPT_TOOLKIT_AVAILABLE and self.session:
                    line = self.session.prompt(
                        prompt_text, 
                        completer=completer,
                        lexer=PygmentsLexer(SqlLexer),
                        style=style
                    )
                else:
                    line = self.console.input(prompt_text)

                if line.strip().startswith("!"):
                    self._handle_history_rerun(line.strip())
                    query_buffer = ""
                    continue

                query_buffer += line + " "
            except (KeyboardInterrupt, EOFError):
                if self._handle_meta_command(".exit"): break
                query_buffer = ""
                self.console.print()
                continue

            if line.strip().lower().startswith("."):
                if self._handle_meta_command(line.strip()): break
                query_buffer = ""
                continue

            if query_buffer.strip().endswith(";"):
                query_to_run = query_buffer.strip()
                self.history.append(query_to_run)
                self._execute_query(query_to_run)
                query_buffer = ""

    def _save_to_excel(self, save_path: str) -> None:
        """Saves results using XlsxWriter (Streaming) if available."""
        try:
            with self.console.status("[bold green]Saving Excel file...[/bold green]"):
                
                engine = "xlsxwriter" if XLSXWRITER_AVAILABLE else "openpyxl"
                
                with pd.ExcelWriter(save_path, engine=engine) as writer:
                    for sheet_name, df in self.results_to_save.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        if engine == "xlsxwriter":
                            workbook = writer.book
                            worksheet = writer.sheets[sheet_name]
                            
                            header_fmt = workbook.add_format({
                                'bold': True, 'fg_color': '#4F81BD', 'font_color': 'white'
                            })
                            
                            for col_num, value in enumerate(df.columns.values):
                                worksheet.write(0, col_num, value, header_fmt)
                                
                            for i, col in enumerate(df.columns):
                                max_len = max(
                                    df[col].astype(str).map(len).max(),
                                    len(str(col))
                                ) + 2
                                worksheet.set_column(i, i, min(max_len, 50))
                        
                        elif engine == "openpyxl":
                            self._format_excel_sheets_openpyxl(writer)

            self.console.print(f"\n[bold green]✨ Saved to '{os.path.basename(save_path)}' using {engine}[/bold green]")
            self.results_to_save.clear()
            
        except Exception as e:
            self.console.print(f"[bold red]Error during save: {e}[/bold red]")

    def _format_excel_sheets_openpyxl(self, writer: pd.ExcelWriter) -> None:
        """Fallback formatting if XlsxWriter is not installed."""
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")

        for worksheet in writer.book.worksheets:
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill
            worksheet.auto_filter.ref = worksheet.dimensions

    def _handle_meta_command(self, command_str: str) -> bool:
        parts = command_str.split()
        command = parts[0].lower()
        
        commands = {
            ".exit": lambda: True, 
            ".quit": lambda: True,
            ".help": self._show_help,
            ".tables": self._list_tables,
            ".schema": lambda: self._describe_table(parts),
            ".history": self._show_history,
            ".load": self._add_new_files,
            ".export": self._export_results,
            ".rename": lambda: self._rename_table(parts),
            ".runscript": lambda: self._run_script_interactive(parts),
        }
        
        if command not in commands:
            self.console.print(f"[red]Unknown command: {command}[/red]")
            return False

        should_exit = commands[command]()
        
        if should_exit and command in [".exit", ".quit"] and self.results_to_save:
             choice = self.console.input("Export staged results before quitting? (y/n): ").lower()
             if choice.startswith("y"):
                 self._export_results()
        
        return should_exit

    def _execute_query(self, query: str) -> None:
        if not self.db_connection: return
        try:
            with self.console.status("[bold green]Executing...[/bold green]"):
                res = self.db_connection.execute(query).fetchdf()
            if res.empty:
                self.console.print("[yellow]No data returned.[/yellow]")
            else:
                self.console.print("[green]Query Successful![/green]")
                self._display_results_table(res)
                self._prompt_to_stage_results(res)
        except Exception as e:
            self.console.print(f"[red]SQL Error: {e}[/red]")

    def _display_results_table(self, df: pd.DataFrame) -> None:
        table = Table(show_header=True, header_style="bold magenta")
        for col in df.columns:
            table.add_column(str(col))
        for _, row in df.head(20).iterrows():
            table.add_row(*[str(x) for x in row])
        self.console.print(table)
        if len(df) > 20:
            self.console.print(f"... ({len(df)-20} more rows)")

    def _prompt_to_stage_results(self, results: pd.DataFrame) -> None:
        if self.console.input("\nStage for export? (y/n): ").lower().startswith("y"):
            name = self.console.input("Sheet name: ")
            if name:
                self.results_to_save[name] = results
                self.console.print("[green]Staged.[/green]")

    def _export_results(self) -> None:
        if not self.results_to_save:
            return self.console.print("[yellow]Nothing to export.[/yellow]")
        if path := self._prompt_for_save_path():
            self._save_to_excel(path)

    def _prompt_for_save_path(self):
        if TKINTER_AVAILABLE:
            root = tk.Tk()
            root.withdraw()
            p = filedialog.asksaveasfilename(
                title="Save Export",
                initialfile=self.DEFAULT_EXPORT_FILENAME,
                defaultextension=".xlsx",
                filetypes=[("Excel Files", "*.xlsx")]
            )
            root.destroy()
            return p
        return self.console.input(f"Save path (default {self.DEFAULT_EXPORT_FILENAME}): ")

    def _list_tables(self) -> None:
        if self.db_connection:
            try:
                tables = self.db_connection.execute("SHOW TABLES").fetchdf()['name']
                self.console.print("\n[cyan]Tables:[/cyan]")
                for t in tables:
                    self.console.print(f" - {t}")
            except: pass

    def _describe_table(self, parts):
        if len(parts) == 2 and self.db_connection:
            try:
                df = self.db_connection.execute(f"DESCRIBE {parts[1]}").fetchdf()
                t = Table(title=f"Schema: {parts[1]}")
                for c in df.columns: t.add_column(c)
                for _, r in df.iterrows(): t.add_row(*[str(x) for x in r])
                self.console.print(t)
            except Exception as e:
                self.console.print(f"[red]Error: {e}[/red]")
        else:
            self.console.print("[red]Usage: .schema <table_name>[/red]")

    def _rename_table(self, parts: List[str]) -> None:
        if len(parts) != 3:
            return self.console.print("[red]Usage: .rename <old> <new>[/red]")
        try:
            self.db_connection.execute(f'ALTER VIEW "{parts[1]}" RENAME TO "{parts[2]}"')
            self.console.print(f"[green]Renamed {parts[1]} to {parts[2]}[/green]")
            if parts[1] in self.schema_cache:
                self.schema_cache[parts[2]] = self.schema_cache.pop(parts[1])
        except Exception as e:
            self.console.print(f"[red]Error: {e}[/red]")

    def _show_help(self) -> None:
        self.console.print("\n[bold]Commands:[/bold]")
        self.console.print("  .help             Show this message")
        self.console.print("  .tables           List tables")
        self.console.print("  .schema <table>   Show table columns")
        self.console.print("  .history          Show query history")
        self.console.print("  .load             Load new files")
        self.console.print("  .rename <o> <n>   Rename a table")
        self.console.print("  .export           Save staged results")
        self.console.print("  .runscript <file> Run a YAML script")
        self.console.print("  .exit             Quit")

    def _show_history(self):
        for i, cmd in enumerate(self.history, 1):
            self.console.print(f"{i}: {cmd}")

    def _handle_history_rerun(self, cmd):
        try:
            idx = int(cmd[1:])
            if 1 <= idx <= len(self.history):
                self._execute_query(self.history[idx-1])
        except: pass

    def _add_new_files(self):
        paths = self._prompt_for_paths("Select Files", [("All", "*.*")], True)
        if paths:
            self._load_data(paths)

    def _run_script_interactive(self, command_parts: List[str]) -> None:
        script_path = command_parts[1] if len(command_parts) == 2 else None
        if not script_path:
            paths = self._prompt_for_paths("Select Script", [("YAML", "*.yml *.yaml")], False)
            if not paths: return
            script_path = paths[0]
        
        if not YAML_AVAILABLE:
            return self.console.print("[red]PyYAML missing.[/red]")
        try:
            with open(script_path, "r") as f:
                config = yaml.safe_load(f)
            self.console.print(f"[bold cyan]Executing {script_path}...[/bold cyan]")
            self._execute_yaml_script(config)
            self._list_tables()
        except Exception as e:
            self.console.print(f"[red]Script Error: {e}[/red]")

    def _execute_yaml_script(self, config: Dict[str, Any]) -> None:
        if "inputs" in config:
            self._process_yaml_inputs(config.get("inputs", []))
        if "tasks" in config:
            self._process_yaml_tasks(config.get("tasks", []))
        if "export" in config:
            self._process_yaml_export(config.get("export", {}))

    def _process_yaml_inputs(self, inputs: List[Dict[str, str]]) -> None:
        """Loads files and applies renaming aliases as specified in YAML."""
        self.console.print("\n[bold]--- 1. Loading Input Files ---[/bold]")
        if not inputs: return

        paths_to_load = [item["path"] for item in inputs]
        loaded_table_names = self._load_data(paths_to_load)
        
        alias_map = {
            os.path.basename(item["path"]): item.get("alias")
            for item in inputs
            if item.get("alias")
        }

        for current_table_name in loaded_table_names:
            for filename, alias in alias_map.items():
                base_file = re.sub(r"[^a-zA-Z0-9_]+", "_", os.path.splitext(filename)[0])
                ext_part = os.path.splitext(filename)[1].lower().replace(".", "_")

                is_match = (
                    current_table_name == f"{base_file}_{ext_part}" or 
                    current_table_name == f"{base_file}{ext_part}" or 
                    current_table_name.startswith(f"{base_file}{ext_part}_")
                )

                if is_match:
                    if any(x in current_table_name for x in ["_csv", "_parquet", "_json"]):
                        new_name = alias 
                    else:
                        try:
                            sheet_suffix = current_table_name.split(f"{base_file}{ext_part}_", 1)[1]
                            new_name = f"{alias}_{sheet_suffix}"
                        except IndexError:
                            new_name = alias

                    try:
                        self.db_connection.execute(f'DROP VIEW IF EXISTS "{new_name}";')
                        self.db_connection.execute(f'ALTER VIEW "{current_table_name}" RENAME TO "{new_name}";')
                        self.console.print(f"  ➜ Aliased '{current_table_name}' to [cyan]'{new_name}'[/cyan]")
                        
                        if current_table_name in self.schema_cache:
                            self.schema_cache[new_name] = self.schema_cache.pop(current_table_name)
                            
                    except Exception as e:
                        self.console.print(f"[red]Failed to alias {current_table_name}: {e}[/red]")
                    break

    def _process_yaml_tasks(self, tasks: List[Dict[str, str]]) -> None:
        self.console.print("\n[bold]--- 2. Executing Tasks ---[/bold]")
        for task in tasks:
            name, sql = task.get("name"), task.get("sql")
            if not (name and sql and self.db_connection): continue
            
            self.console.print(f"Executing task: [cyan]'{name}'[/cyan]...")
            try:
                self.results_to_save[name] = self.db_connection.execute(sql).fetchdf()
                self.console.print(f"[green]  ✔ Success! {len(self.results_to_save[name])} rows staged.[/green]")
            except Exception as e:
                self.console.print(f"[bold red]  ❌ Error in task '{name}': {e}[/bold red]")

    def _process_yaml_export(self, export_config: Dict[str, str]) -> None:
        self.console.print("\n[bold]--- 3. Exporting Results ---[/bold]")
        if self.results_to_save:
            path = export_config.get("path", self.DEFAULT_EXPORT_FILENAME)
            self._save_to_excel(path)
        else:
            self.console.print("[yellow]No results were staged for export.[/yellow]")


def main() -> None:
    parser = argparse.ArgumentParser(description="SheetQL Professional")
    parser.add_argument("-r", "--run", dest="config_path", help="Run batch config")
    args = parser.parse_args()
    
    tool = SheetQL()
    if args.config_path:
        tool.run_batch(args.config_path)
    else:
        tool.run_interactive()

if __name__ == "__main__":
    main()