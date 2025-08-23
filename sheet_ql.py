# query_tool.py
import os
import re
import pandas as pd
import duckdb
from typing import Dict, List, Optional

from rich.console import Console
from rich.table import Table

from openpyxl.styles import Font, PatternFill

try:
    import tkinter as tk
    from tkinter import filedialog
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

class SheetQL:
    """An interactive command-line tool to run SQL queries on data files."""

    def __init__(self):
        """Initializes the SheetQL tool."""
        self.console = Console()
        self.db_connection: Optional[duckdb.DuckDBPyConnection] = None
        self.results_to_save: Dict[str, pd.DataFrame] = {}

    def run(self):
        """Starts and runs the main application lifecycle."""
        try:
            self._display_welcome()
            
            self.db_connection = duckdb.connect(database=':memory:')

            initial_paths = self._select_files()
            if not initial_paths:
                return

            initial_dataframes = self._load_data(initial_paths)
            if not initial_dataframes:
                return
            
            self._register_dataframes(initial_dataframes)
            self.console.print("[bold green]--- 🦆 DuckDB is ready ---[/bold green]")
            self._list_tables()

            self._run_interactive_loop()
            self._save_results()

        except Exception as e:
            self.console.print(f"[bold red]An unexpected error occurred: {e}[/bold red]")
        finally:
            self.console.print("\n[bold cyan]👋 Goodbye![/bold cyan]")

    def _display_welcome(self):
        """Prints a welcome message to the console."""
        self.console.print("[bold green]--- SheetQL: Interactive SQL Query Tool for Spreadsheet Files ---[/bold green]")
        self.console.print("Type your SQL query and end it with a semicolon ';'.")
        self.console.print("Type [bold yellow].help[/bold yellow] for a list of commands.")
        if not TKINTER_AVAILABLE:
            self.console.print("[yellow]Note: Tkinter not found. Using command-line for file selection.[/yellow]")

    def _select_files_gui(self) -> Optional[List[str]]:
        """Opens a GUI dialog to select data files."""
        self.console.print("\n[cyan]Waiting for you to select data file(s) in the dialog window...[/cyan]")
        root = tk.Tk()
        root.withdraw()
        file_paths = filedialog.askopenfilenames(
            title="Select Data Files (Excel, CSV, Parquet)",
            filetypes=[
                ("Supported Files", "*.xlsx *.xls *.csv"),
                ("Excel Files", "*.xlsx *.xls"),
                ("CSV Files", "*.csv"),
                ("All files", "*.*")
            ],
        )
        root.destroy()
        return list(file_paths) if file_paths else None

    def _select_files_cli(self) -> Optional[List[str]]:
        """Prompts for file paths in the command line."""
        self.console.print("\n[cyan]Please enter the full path to your data file(s) (Excel, CSV).[/cyan]")
        self.console.print("[cyan]You can enter multiple paths separated by commas.[/cyan]")
        paths_input = self.console.input("[bold]File path(s): [/bold]")
        
        raw_paths = [p.strip().strip("'\"") for p in paths_input.split(',')]
        valid_paths = [p for p in raw_paths if os.path.exists(p) and os.path.isfile(p)]
        
        if not valid_paths:
            self.console.print("[red]Error: No valid file paths were provided.[/red]")
            return None
        
        return valid_paths

    def _select_files(self) -> Optional[List[str]]:
        """Selects files using GUI if available, otherwise falls back to CLI."""
        if TKINTER_AVAILABLE:
            file_paths = self._select_files_gui()
        else:
            file_paths = self._select_files_cli()

        if not file_paths:
            self.console.print("[yellow]Operation cancelled: No files selected.[/yellow]")
            return None
        
        self.console.print(f"[green]✔ Selected {len(file_paths)} file(s).[/green]")
        return file_paths

    def _load_data(self, file_paths: List[str]) -> Dict[str, pd.DataFrame]:
        """Loads all supported files into pandas DataFrames."""
        all_dataframes = {}
        with self.console.status("[bold green]Loading data files...[/bold green]") as status:
            for file_path in file_paths:
                try:
                    file_extension = os.path.splitext(file_path)[1].lower()
                    base_name = re.sub(r'[^a-zA-Z0-9_]+', '_', os.path.splitext(os.path.basename(file_path))[0])

                    if file_extension in ['.xlsx', '.xls']:
                        xls = pd.ExcelFile(file_path)
                        for sheet_name in xls.sheet_names:
                            status.update(f"Reading sheet '{sheet_name}' from '{os.path.basename(file_path)}'...")
                            df = pd.read_excel(xls, sheet_name=sheet_name)
                            table_name = f"{base_name}_{re.sub(r'[^a-zA-Z0-9_]+', '_', sheet_name)}"
                            all_dataframes[table_name] = df
                    elif file_extension == '.csv':
                        status.update(f"Reading CSV '{os.path.basename(file_path)}'...")
                        df = pd.read_csv(file_path)
                        all_dataframes[base_name] = df
                    elif file_extension == '.parquet':
                        status.update(f"Reading Parquet '{os.path.basename(file_path)}'...")
                        df = pd.read_parquet(file_path)
                        all_dataframes[base_name] = df
                    else:
                        self.console.print(f"[yellow]Warning: Unsupported file type '{file_extension}'. Skipping '{os.path.basename(file_path)}'.[/yellow]")

                except Exception as e:
                    self.console.print(f"[bold red]Error loading '{file_path}': {e}[/bold red]")
        self.console.print("[green]✔ Data loading complete.[/green]")
        return all_dataframes

    def _register_dataframes(self, dataframes: Dict[str, pd.DataFrame]):
        """Registers new DataFrames as views in the existing DuckDB connection."""
        if not self.db_connection:
            return
        for table_name, df in dataframes.items():
            self.db_connection.register(table_name, df)

    def _run_interactive_loop(self):
        """Runs the main loop for accepting and executing user queries."""
        query_buffer = ""
        while True:
            prompt = "SQL> " if not query_buffer else "  -> "
            try:
                line = self.console.input(prompt)
                query_buffer += line + " "
            except (KeyboardInterrupt, EOFError):
                break

            if line.strip().lower().startswith('.'):
                if self._handle_meta_command(line.strip()):
                    break
                query_buffer = ""
                continue

            if query_buffer.strip().endswith(';'):
                self._execute_query(query_buffer)
                query_buffer = ""

    def _handle_meta_command(self, command_str: str) -> bool:
        """Handles meta-commands. Returns True if exiting."""
        command_parts = command_str.split()
        command = command_parts[0]

        if command in ['.exit', '.quit']:
            return True
        elif command == '.help':
            self.console.print("\n[bold]Available Commands:[/bold]")
            self.console.print("[yellow].help[/yellow]                      - Show this help message.")
            self.console.print("[yellow].load[/yellow]                      - Load additional data files into the session.")
            self.console.print("[yellow].rename <old> <new>[/yellow]        - Rename a table (view).")
            self.console.print("[yellow].exit[/yellow]                      - Exit the application.")
            self.console.print("\nEnd any SQL query with a semicolon ';'.")
        elif command == '.load':
            self._add_new_files()
        elif command == '.rename':
            self._rename_table(command_parts)
        else:
            self.console.print(f"[red]Unknown command: '{command}'. Type .help for assistance.[/red]")
        return False

    def _add_new_files(self):
        """Handles the logic for loading additional files into the session."""
        self.console.print("\n[cyan]Select additional data files to load...[/cyan]")
        new_paths = self._select_files()
        if new_paths:
            new_dataframes = self._load_data(new_paths)
            if new_dataframes:
                self._register_dataframes(new_dataframes)
                self.console.print("[green]✔ New files loaded and registered.[/green]")
                self._list_tables()

    def _rename_table(self, command_parts: List[str]):
        """Renames a view in the database."""
        if len(command_parts) != 3:
            self.console.print("[red]Usage: .rename <old_table_name> <new_table_name>[/red]")
            return
        
        old_name, new_name = command_parts[1], command_parts[2]

        if not self.db_connection: return
        try:
            self.db_connection.execute(f'ALTER VIEW "{old_name}" RENAME TO "{new_name}";')
            self.console.print(f"[green]✔ Table view '{old_name}' renamed to '{new_name}'.[/green]")
        except Exception as e:
            self.console.print(f"[bold red]❌ Error renaming view: {e}[/bold red]")

    def _list_tables(self):
        """Prints the list of available tables."""
        if not self.db_connection: return
        tables = self.db_connection.execute("SHOW TABLES;").fetchdf()
        self.console.print("\n[bold cyan]Available Tables:[/bold cyan]")
        for name in tables['name']:
            self.console.print(f"  - {name}")

    def _execute_query(self, query: str):
        """Executes a SQL query and handles the results."""
        if not self.db_connection: return
        try:
            with self.console.status("[bold green]Executing query...[/bold green]"):
                results = self.db_connection.execute(query).fetchdf()
        except Exception as e:
            self.console.print(f"\n[bold red]❌ SQL Error: {e}[/bold red]")
            return

        if results.empty:
            self.console.print("\n[bold yellow]✅ Query executed successfully but returned no data.[/bold yellow]")
            return

        self.console.print("\n[bold green]✅ Query Successful![/bold green]")
        self._display_results_table(results)
        self._prompt_to_save(results)

    def _display_results_table(self, df: pd.DataFrame):
        """Displays a pandas DataFrame in a rich Table."""
        table = Table(show_header=True, header_style="bold magenta")
        for col in df.columns:
            table.add_column(col)
        for _, row in df.head(20).iterrows():
            table.add_row(*[str(item) for item in row])
        self.console.print(table)
        if len(df) > 20:
            self.console.print(f"... (and {len(df) - 20} more rows)")

    def _prompt_to_save(self, results: pd.DataFrame):
        """Asks the user if they want to save the results."""
        save_choice = self.console.input("\n[bold]💾 Save these results? (y/n): [/bold]").lower()
        if save_choice in ['y', 'yes']:
            sheet_name = self.console.input("[bold]   Enter a name for the Excel sheet: [/bold]")
            if sheet_name:
                self.results_to_save[sheet_name] = results
                self.console.print(f"[green]   ✔ Results staged to be saved as sheet '{sheet_name}'.[/green]")
            else:
                self.console.print("[yellow]   Sheet name cannot be empty. Results not staged.[/yellow]")

    def _get_save_path_gui(self) -> Optional[str]:
        """Opens a GUI dialog to select a save location."""
        self.console.print("\n[cyan]Waiting for you to select a save location in the dialog window...[/cyan]")
        root = tk.Tk()
        root.withdraw()
        save_path = filedialog.asksaveasfilename(
            title="Select location to save query results",
            initialfile="query_result.xlsx",
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
        )
        root.destroy()
        return save_path

    def _get_save_path_cli(self) -> Optional[str]:
        """Prompts for a save path in the command line."""
        self.console.print("\n[cyan]Please enter the full path where you want to save the report.[/cyan]")
        default_name = "query_result.xlsx"
        save_path = self.console.input(f"[bold]Save path (default: {default_name}): [/bold]")
        if not save_path:
            save_path = default_name
        
        directory = os.path.dirname(save_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            
        return save_path

    def _save_results(self):
        """Saves all staged results to a single, formatted Excel file."""
        if not self.results_to_save:
            return

        if TKINTER_AVAILABLE:
            save_path = self._get_save_path_gui()
        else:
            save_path = self._get_save_path_cli()

        if save_path:
            try:
                with self.console.status("[bold green]Saving and formatting Excel file...[/bold green]"):
                    with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                        for sheet_name, df in self.results_to_save.items():
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                        self._format_excel_sheets(writer)
                self.console.print(f"\n[bold green]✨ Success! All {len(self.results_to_save)} result(s) saved to '{os.path.basename(save_path)}'[/bold green]")
            except Exception as e:
                self.console.print(f"[bold red]Error during save: {e}[/bold red]")
        else:
            self.console.print("[yellow]Save operation cancelled. Staged results were not saved.[/yellow]")

    def _format_excel_sheets(self, writer: pd.ExcelWriter):
        """Applies professional formatting to all sheets in the Excel workbook."""
        workbook = writer.book
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")

        for worksheet in workbook.worksheets:
            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill

            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            worksheet.auto_filter.ref = worksheet.dimensions

if __name__ == "__main__":
    tool = SheetQL()
    tool.run()
