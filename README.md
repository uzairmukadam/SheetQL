# SheetQL: Interactive SQL Query Tool for Data Files

**Query, analyze, and report on your local data files using the power of SQL, right from your terminal.**

SheetQL is a powerful command-line tool that transforms your local data files (Excel, CSV, JSON, and Parquet) into a relational database, allowing you to run complex SQL queries without cumbersome manual steps. It's designed for data analysts, software engineers, and anyone who needs to quickly analyze data with the speed and precision of SQL.


---

## 🚀 Key Features

* **Broad File Support**: Natively query **Excel** (`.xlsx`, `.xls`), **CSV** (`.csv`), **JSON** (`.json`, `.jsonl`) and **Apache Parquet** (`.parquet`) files.
* **Powerful Automation**: Execute complex workflows non-interactively with **YAML scripts** for reproducible analysis and reporting.
* **Interactive SQL Console**: Run standard SQL queries in a live, multi-line terminal session with command history.
* **Live Session Introspection**: Check table structures with the `.schema` command and review past queries with `.history`.
* **Dynamic File Loading**: Load additional files into your session at any time with the `.load` command without restarting.
* **GUI & CLI File Selection**: Uses a graphical file picker if available, but gracefully falls back to a command-line interface on headless systems.
* **Custom Table Names**: Rename the default long table names to shorter, more convenient aliases using the `.rename` command.
* **Professional Excel Reports**: Save multiple query results to a single, beautifully formatted Excel file with styled headers, auto-fitted columns, and filters.
* **Fast & Efficient**: Leverages the high-performance DuckDB analytical engine for near-instant query results.
* **Per-Task Exports (YAML)**: Export each query to its own destination (`.xlsx`, `.csv`, `.json`) while still supporting a single combined Excel export.

---

## 📋 System Requirements

* **Python**: Version 3.9 or newer.
* **Operating System**: Windows, macOS, or Linux.
* **Memory**: 4GB RAM recommended
* **Tkinter (Optional)**: For the graphical file dialogs. If not present, the tool will use a command-line fallback.

---

## 🛠️ Installation Guide

### 1. Clone the Repository (Optional)

If you have Git installed, this is the recommended way to get the project files.

```bash
git clone [https://github.com/uzairmukadam/sheetql.git](https://github.com/uzairmukadam/sheetql.git)
cd sheetql
```

Alternatively, you can just download the `sheet_ql.py` and `requirements.txt` files into the same folder.

### 2. Set Up a Virtual Environment (Recommended)

Using a virtual environment keeps your project dependencies isolated from your system's Python installation.

```bash
# Create the virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Required Libraries

Install the core tool along with the high-performance engines (Rust reader, streaming writer, autocomplete UI) using the provided requirements file.

```bash
pip install -r requirements.txt
```

**Note**: If you are on a restricted system where you cannot install high-performance packages (like `calamine` or `prompt_toolkit`), the tool will automatically fallback to standard libraries (`pandas`/`openpyxl`) to ensure functionality.

### 4. (Recommended) Install as a CLI tool

SheetQL is now an installable package with a `sheetql` command.

```bash
# From the repository root
pip install -e .

# Optional extras:
# pip install -e ".[ui,perf,batch]"
```

## ▶️ How to Run

### Interactive Mode

Launch the tool to explore data, run queries, and build reports interactively.

```bash
sheetql interactive

# Legacy entrypoint (still supported)
python sheet_ql.py
```

### Batch Mode (Automation)

Execute a saved pipeline script non-interactively. Perfect for scheduled tasks or "End of Month" reporting.

```bash
sheetql run -c monthly_report.yml

# Legacy entrypoint (still supported)
python sheet_ql.py --run monthly_report.yml
```

### One-off Query Mode (Non-interactive)

Run a single query against one or more input files and print or export the result.

```bash
sheetql query -i data.xlsx -q "SELECT * FROM my_table LIMIT 10" --format json
sheetql query -i data.csv -f query.sql -o out/result.xlsx --sheet-name Result
```

### Inspect Mode

List inferred tables (and optionally columns) without starting the interactive shell.

```bash
sheetql inspect -i data.xlsx --schema
sheetql inspect -i data.csv --format json
```

## 📖 Usage Instructions

### Step 1: Select Your Data Files

When the tool starts, select your files via the dialog or path input.

* **CSV/Parquet**: Linked instantly (0ms load time) using Zero-Copy views.
* **Excel**: Parsed rapidly using the Rust engine.

### Step 2: Write SQL with Autocomplete

Type your queries with support from the IntelliSense engine.

* **Tab Completion**: Press `Tab` to autocomplete keywords (`SELECT`, `WHERE`), table names, and columns.

* **Context Aware**: The tool intelligently suggests columns specific to the tables you are currently querying.

### Step 3: Use Meta-Commands

Instead of a SQL query, you can type special commands (starting with a dot):

* `.tables`: List all loaded tables/views.
* `.schema <table>`: View column names and data types.
* `.load`: Add more files to the current session without restarting.
* `.rename <old> <new>`: Rename a table alias (e.g., `sales_data_2023_v2` -> `sales`).
* `.dump <filename.yml>`: Save your current session (inputs + queries) as a reusable script.
* `.export`: Save all staged query results to a formatted Excel file.
* `.history`: Display previous queries.
* `.exit` or `.quit`: Exits the application (prompts to save first).

### Table naming and aliases

When you load files or define aliases, SheetQL automatically **normalizes** names into safe SQL identifiers that DuckDB accepts:

* All names are lowercased and non-alphanumeric characters are replaced with `_`.
* Leading/trailing/multiple underscores are collapsed.
* If a name would start with a digit (e.g. a filename like `2026_report.xlsx`), SheetQL prefixes it with `t_` so it becomes a valid identifier, e.g. `t_2026_report_xlsx`.

This makes scripts more robust (no \"view does not exist\" errors from numeric-leading names), while keeping the mapping deterministic so you can predict table names from filenames and aliases.

### Step 4: Rerun from History

Made a mistake? Press Up Arrow to edit, or use history expansion:

* `!N`: Rerun the Nth query in your history (e.g., `!3`).

### Step 6: Save Your Results

After a query runs successfully, you will be prompted to stage the results. You can stage multiple results and then use the `.export` command to save them all to a single, formatted Excel report.

## ⚙️ Automated Execution with YAML

You no longer need to write YAML scripts by hand.

1. **Explore**: Load your files and run your queries interactively.
2. **Stage**: When a query produces a good result, answer `y` when prompted to stage it.
3. **Dump**: Run `.dump my_pipeline.yml`.

SheetQL will generate a production-ready script file for you:

**Generated `my_pipeline.yml` (example)**:

```
options:
  memory_limit: "75%"
  stop_on_error: true

variables:
  out_dir: "C:/Reports"

inputs:
  - path: "C:/Data/raw_sales.csv"
    alias: "sales_raw"
  - path: "C:/Data/targets.xlsx"
    alias: "targets"

tasks:
  - name: "Q1_Performance"
    sql: >
      SELECT s.Region, SUM(s.Amount) 
      FROM sales_raw s 
      JOIN targets t ON s.Region = t.City
      WHERE s.Amount > t.Goal
    export:
      path: "${out_dir}/Q1_Performance.csv"

  - name: "Summary"
    sql: >
      SELECT COUNT(*) AS n_rows FROM sales_raw

export:
  path: "${out_dir}/Q1_Summary.xlsx"
```

To run this next month, simply execute: `sheetql run -c my_pipeline.yml`

### YAML tips for fast iteration

* **Change sources quickly** with `variables` and `${var}` substitution in both `inputs[].path` and `tasks[].sql`.
* **Export each task separately** by adding `tasks[].export.path`.\n+  Supported formats by extension: `.xlsx`, `.csv`, `.json`.\n+  For `.xlsx`, you can also set `tasks[].export.sheet`.\n+* **Combined Excel export still works** using the top-level `export.path`.\n+  Any task **without** `tasks[].export` is staged and included there.

## 💡 Troubleshooting

* **Logs**: If the application crashes or behaves unexpectedly, check the `sheetql.log` file created in the same directory. It contains detailed debug traces that are hidden from the main console.

* **Memory Errors**: The tool is configured to use 75% of available RAM. If you hit limits on massive files, ensure you are using `.parquet` or `.csv` formats, which utilize DuckDB's out-of-core streaming.

* **Missing Features**: If Autocomplete or Fast Excel loading is not working, check the startup banner. It will display the status of optional engines (e.g., `Rust-Excel [Red]`). Re-run `pip install -r requirements.txt`.

## 🤝 Contributing

Contributions are welcome! If you have ideas for new features, bug fixes, or improvements, please feel free to open an issue or fork the repository.

## 🚀 Future Features

* **Persistent Sessions**: Save and load your entire session, including loaded tables and renames, so you can pick up where you left off.
* **Additional Export Formats**: Add Markdown/HTML exports and template-based reporting.
* **Basic Charting**: A command to generate simple text-based charts in the terminal or save graphical charts to an image file.

## 📄 License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
