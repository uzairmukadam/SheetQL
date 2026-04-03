# SheetQL

> **Query your local data files with SQL. Instantly.**

SheetQL is a command-line tool powered by [DuckDB](https://duckdb.org/) that turns Excel, CSV, JSON, and Parquet files into queryable database tables — no server, no setup, no import wizards. Run SQL directly against your files, join across them, and export polished reports.

---

## Features

- **Query any local file** — Excel (`.xlsx`, `.xls`), CSV, JSON (`.json`, `.jsonl`, `.ndjson`), and Parquet
- **Full SQL** via DuckDB — aggregations, window functions, CTEs, multi-file JOINs
- **Interactive shell** with SQL syntax highlighting, Tab autocomplete, Ctrl+R history search
- **One-off query mode** — pipe results to stdout as table/CSV/JSON, or write directly to a file
- **YAML automation** — define pipelines with variables, multi-step tasks, and per-task exports; replay them on demand
- **Session recording** — explore interactively, then `.dump` your session to a ready-to-run YAML script
- **Professional Excel output** — auto-fitted column widths, styled headers, and dropdown filters on every export
- **Persistent logs** — each run appends to `sheetql.log` with a timestamped session separator
- **Nuitka-ready** — compile to a standalone `.exe` with `python build.py`

---

## Installation

**Requirements:** Python 3.9+

```bash
git clone https://github.com/uzairmukadam/sheetql.git
cd sheetql

python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate
```

There are two ways to run SheetQL. Choose whichever fits your workflow:

### Option A — Run directly (no install required)

Install dependencies from `requirements.txt` and run `sheet_ql.py` directly. This is the simplest route — no package installation needed.

```bash
pip install -r requirements.txt
python sheet_ql.py
```

> `sheet_ql.py` supports interactive mode and batch mode (`--run`). For the full subcommand CLI (`query`, `inspect`) use Option B.

### Option B — Install as a CLI package

Install SheetQL as a proper Python package. This registers the `sheetql` command and unlocks all subcommands.

```bash
# Core install:
pip install -e .

# Recommended — all optional engines (autocomplete, Rust Excel reader, YAML scripting):
pip install -e ".[all]"
```

### Optional extras

| Extra | What it adds | Install |
|---|---|---|
| `ui` | SQL syntax highlighting + Tab autocomplete + Ctrl+R history | `pip install -e ".[ui]"` |
| `perf` | Rust-based Excel reader (much faster loading) + streaming Excel writer | `pip install -e ".[perf]"` |
| `batch` | YAML script execution | `pip install -e ".[batch]"` |
| `all` | All of the above | `pip install -e ".[all]"` |
| `dev` | pytest + Nuitka | `pip install -e ".[dev]"` |

---

## Quick Start

**Option A — using `sheet_ql.py` directly:**
```bash
# Interactive shell
python sheet_ql.py

# Run a YAML pipeline
python sheet_ql.py --run monthly_report.yml

# Enable debug logging
python sheet_ql.py --debug
```

**Option B — using the installed `sheetql` CLI:**
```bash
# Interactive shell
sheetql interactive

# One-off query — print results to terminal
sheetql query -i sales.csv -q "SELECT region, SUM(amount) AS total FROM sales_csv GROUP BY region"

# One-off query — write results to Excel
sheetql query -i sales.csv -q "SELECT * FROM sales_csv LIMIT 100" -o report.xlsx

# Inspect what tables a file produces, with column names
sheetql inspect -i data.xlsx --schema

# Run a saved YAML pipeline
sheetql run -c monthly_report.yml

# Version / help
sheetql --version
sheetql --help
```

---

## Interactive Mode

Start the interactive shell:

```bash
# Option A — run directly (no install)
python sheet_ql.py

# Option B — installed CLI
sheetql interactive
```

A file picker dialog opens (or a path prompt on headless systems). After selecting files, SheetQL loads them and drops into the SQL prompt.

### Writing queries

Queries are terminated with a semicolon. Multi-line input is supported — keep typing and press Enter; SheetQL waits for the `;` before executing.

```
SQL> SELECT product, SUM(revenue) AS total
  -> FROM sales_csv
  -> GROUP BY product
  -> ORDER BY total DESC;
```

Results are shown in a formatted table (up to 15 rows previewed). You are then asked whether to **stage** the result for export.

### Tab autocomplete & history

If `prompt_toolkit` is installed (via `[ui]` extra):

- **Tab** — autocomplete SQL keywords, table names, and column names
- **Up / Down arrows** — navigate previous queries
- **Ctrl+R** — reverse-search through session history

### History re-run

```
SQL> .history        # show numbered query history
SQL> !3              # re-run query #3
```

### Meta-commands

Type these at the `SQL>` prompt instead of a query:

| Command | Description |
|---|---|
| `.tables` | List all loaded tables |
| `.schema <table>` | Show columns and data types for a table |
| `.load` | Add more files to the current session without restarting |
| `.rename <old> <new>` | Rename a table to a shorter alias |
| `.export` | Save all staged results to a formatted Excel file |
| `.dump <file.yml>` | Save the current session as a reusable YAML script |
| `.runscript <file.yml>` | Execute a YAML script inside the current session |
| `.history` | Show previous queries |
| `.exit` / `.quit` | Exit (prompts to export staged results first) |

### Staging and exporting results

After each successful query you are prompted:

```
Stage for export? (y/n): y
Sheet name: Q1_Summary
Staged 'Q1_Summary'
```

Stage as many results as you like under different sheet names, then:

```
SQL> .export
```

This saves all staged results to a single `.xlsx` file with:
- Styled blue headers
- Auto-fitted column widths
- Dropdown filters on every column

### Table naming

SheetQL auto-generates table names from file and sheet names:

| Source | Table name |
|---|---|
| `sales_2024.csv` | `sales_2024_csv` |
| `targets.xlsx` → sheet `Q1 Goals` | `targets_q1_goals` |
| `2026_report.xlsx` | `t_2026_report_xlsx` *(digit-leading names get a `t_` prefix)* |
| `My Weird File!.csv` | `my_weird_file_csv` |

Use `.rename` to shorten these: `.rename sales_2024_csv sales`

---

## One-off Query Mode

Run a single query without entering the interactive shell. Useful for scripting and pipelines.

```bash
sheetql query -i <file(s)> -q <sql> [options]
```

**Options:**

| Flag | Description |
|---|---|
| `-i / --input` | One or more input files (required) |
| `-q / --query` | SQL string to execute |
| `-f / --query-file` | Path to a `.sql` file |
| `--alias new=old` | Rename a table before querying (repeatable) |
| `--limit N` | Truncate result to N rows |
| `--format table\|csv\|json` | stdout format (default: `table`) |
| `-o / --output` | Write to `.xlsx`, `.csv`, or `.json` instead of stdout |
| `--sheet-name` | Sheet name when writing `.xlsx` |

**Examples:**

```bash
# Print top revenue regions as a table
sheetql query -i sales.csv -q "SELECT region, SUM(amount) FROM sales_csv GROUP BY 1 ORDER BY 2 DESC"

# Write filtered data to JSON (pipe-friendly)
sheetql query -i data.xlsx -q "SELECT * FROM data_sheet1 WHERE status = 'active'" --format json

# Load a .sql file and export to Excel
sheetql query -i orders.csv customers.xlsx -f report.sql -o output/report.xlsx --sheet-name Orders

# Rename a long auto-generated table name before querying
sheetql query -i "2026 Jan Report.xlsx" --alias jan=t_2026_jan_report_sheet1 -q "SELECT * FROM jan"
```

---

## Inspect Mode

Check what tables and columns a file produces without running a query:

```bash
# List tables
sheetql inspect -i data.xlsx

# List tables with their columns
sheetql inspect -i data.xlsx --schema

# Output as JSON (for scripting)
sheetql inspect -i data.xlsx --format json
```

---

## YAML Scripting (Automation)

YAML scripts define a reproducible pipeline: which files to load, what queries to run, and where to save the results. Run them on a schedule or hand them off to colleagues.

### Script structure

```yaml
# Optional: reusable variables substituted with ${VAR_NAME}
# Falls back to environment variables if not defined here.
variables:
  data_dir: C:/Data
  out_dir: C:/Reports

# Engine options
options:
  memory_limit: "75%"   # fraction of RAM DuckDB may use
  stop_on_error: true   # abort remaining tasks on first failure

# Input files to load
inputs:
  - path: ${data_dir}/sales.csv
    alias: sales          # optional: rename the auto-generated table name

  - path: ${data_dir}/targets.xlsx
    alias: targets

# SQL tasks to execute
tasks:
  - name: Q1_Performance
    sql: >
      SELECT s.region, SUM(s.amount) AS revenue, t.goal
      FROM sales s
      JOIN targets_q1 t ON s.region = t.region
      WHERE s.quarter = 'Q1'
      GROUP BY s.region, t.goal
    export:
      path: ${out_dir}/Q1_Performance.csv   # export this task directly

  - name: Summary
    sql: SELECT COUNT(*) AS total_rows, SUM(amount) AS grand_total FROM sales
    # no export here — this result is included in the combined Excel at the bottom

# Combined Excel export (collects all tasks without their own export)
export:
  path: ${out_dir}/Monthly_Report.xlsx
```

### Running a script

```bash
sheetql run -c monthly_report.yml
```

### Per-task exports

Each task can write its result to its own file:

```yaml
tasks:
  - name: active_customers
    sql: SELECT * FROM customers WHERE status = 'active'
    export:
      path: ${out_dir}/active_customers.xlsx
      sheet: Customers        # optional sheet name for .xlsx

  - name: revenue_summary
    sql: SELECT region, SUM(revenue) FROM sales GROUP BY region
    export:
      path: ${out_dir}/revenue.csv
```

Supported export formats: `.xlsx`, `.csv`, `.json`.

### Variable substitution

`${VAR}` placeholders are resolved in this order:

1. The `variables:` block in the YAML file
2. Environment variables (e.g. `${USERPROFILE}`, `${HOME}`, `${MY_DATA_PATH}`)
3. Left unchanged if not found in either

This makes scripts portable — colleagues can override paths via environment variables without editing the YAML.

### Generate a script from an interactive session

You do not need to write YAML by hand. Explore interactively, then let SheetQL generate the script:

1. Start the interactive shell and load your files
2. Run your queries and stage the good results (`y` when prompted)
3. Run `.dump my_pipeline.yml`

SheetQL writes a complete, ready-to-run script including variables, inputs, tasks, and the export path. Edit it if needed, then replay it anytime:

```bash
# Option A — run directly
python sheet_ql.py --run my_pipeline.yml

# Option B — installed CLI
sheetql run -c my_pipeline.yml
```

---

## Logging

SheetQL logs to `sheetql.log` in the working directory. Each run **appends** to the file — previous sessions are preserved and separated by a timestamped header:

```
================================================================================
  SESSION START  |  2026-04-03 18:18:00  |  INFO  |  sheetql run -c report.yml
================================================================================
2026-04-03 18:18:00 | INFO     | SheetQL | 🚀 Batch mode: 'report.yml'
2026-04-03 18:18:01 | INFO     | SheetQL | ✔ Loaded 2 tables.
...
```

Enable verbose debug output:

```bash
# Option A
python sheet_ql.py --debug

# Option B
sheetql --debug interactive
```

---

## Building a Standalone Executable

Compile SheetQL into a single `.exe` (Windows) using [Nuitka](https://nuitka.net/):

```bash
# Install Nuitka (included in the dev extra)
pip install -e ".[dev]"

# Standard release build → dist/sheetql.exe
python build.py

# Directory build (faster startup, no extraction step)
python build.py --no-onefile

# Debug build (includes symbols, verbose output)
python build.py --debug
```

**Requirements:** A C compiler must be available — either MSVC (Visual Studio) or MinGW-w64. Nuitka will prompt to auto-download MinGW-w64 if neither is detected.

---

## Troubleshooting

**Startup banner shows red engines**

```
Engine Status: Rust-Excel [red], Stream-Write [red], Autocomplete [red]
```

One or more optional packages are missing. Run:

```bash
# Option A
pip install -r requirements.txt

# Option B
pip install -e ".[all]"
```

**Memory errors on large files**

SheetQL uses 75% of available RAM by default. For very large files, prefer `.parquet` or `.csv` — these use DuckDB's out-of-core streaming and never load the full file into memory.

You can also override the limit in a YAML script:

```yaml
options:
  memory_limit: "4GB"
```

**`view does not exist` after loading Excel**

Excel sheets are registered as in-memory tables (not views). If you see this error when using `.rename`, it has been fixed in v4.0.0 — make sure you are on the latest version.

**Logs**

If the tool crashes or behaves unexpectedly, check `sheetql.log`. It contains the full debug trace for every session.

---

## Project Layout

```
sheetql/
├── sheetql/
│   ├── __init__.py
│   ├── __main__.py      # python -m sheetql entry point
│   ├── cli.py           # subcommand definitions (interactive, query, run, inspect)
│   ├── engine.py        # core SheetQL class (loading, querying, exporting)
│   ├── scripting.py     # YAML config parsing and validation
│   ├── session.py       # session recorder → .dump YAML generation
│   ├── completion.py    # Tab autocomplete provider
│   ├── naming.py        # filename → SQL identifier normalization
│   ├── deps.py          # optional dependency detection
│   └── logging.py       # logging setup (append mode + session separators)
├── tests/
│   └── test_sheet_ql.py
├── build.py             # Nuitka build helper
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## License

MIT — see [LICENSE](LICENSE).
