# SheetQL

**SheetQL** is a small, focused tool for **quick analytics on flat files** like CSV, Excel, Parquet, and JSON using **full SQL** in the terminal. There is no database server, no ETL platform, and no spreadsheet GUI: you point SheetQL at files, get **DuckDB** tables and views, run queries, optionally stage results, and export to Excel or other formats.

It is built for analysts, engineers, and anyone who wants **ad-hoc questions answered fast** on local data without standing up heavier tooling.

---

## What makes SheetQL different

| Specialty | What it means for you |
|-----------|-------------------------|
| **DuckDB in-process** | SQL runs inside your Python process: fast, local, no server to install or secure. |
| **Zero-copy paths** | Parquet, CSV, and JSON are exposed as **views** where possible so DuckDB can scan them efficiently without loading everything into RAM first. |
| **Excel without a server** | `.xlsx` / `.xls` are read (optionally via a fast Rust reader), normalized, and **registered** as tables so you can join them with CSV/Parquet in one session. |
| **One interactive loop** | Pick files (or paths), run SQL ended with **`;`**, see a Rich preview table, optional **staging** for a multi-sheet Excel export, and **meta-commands** for common chores. |
| **CLI for scripts** | `sheetql query`, `inspect`, and `run` fit into shell scripts, CI, and notebooks-style one-offs. |
| **YAML replay** | Describe inputs, SQL tasks, and exports in YAML; run the same pipeline later. **`.dump`** can generate YAML from what you did interactively. |
| **Sensible guardrails** | Identifiers used in `DESCRIBE` / DDL are **quoted safely**; YAML `memory_limit` values are **validated** before being sent to DuckDB. |

---

## Features

- **Interactive SQL shell** — Multiline queries (wait for `;`), Rich result tables, execution time on each run, optional staging → **`.export`** to one styled `.xlsx`.
- **Completions while you type** — With the `ui` extra: SQL highlighting, **Tab** completion, suggestions from history (**→** to accept when shown). **↑ / ↓** move through prior lines (incremental Ctrl+R search is disabled so completions can stay on while typing).
- **Meta-commands** — `.peek`, `.count`, `.files`, `.cwd`, `.clear`, plus tables/schema/load/rename/export/dump/runscript/history/exit (see below).
- **`sheetql query`** — Load files, run one SQL statement, optional **`--limit`** pushed into the engine for `SELECT`/`WITH`, stdout as table / CSV / JSON or **`-o`** file.
- **`sheetql inspect`** — See which DuckDB table names a file produces, optionally with columns (or JSON for tooling).
- **`sheetql run`** — Execute a validated YAML pipeline (inputs, tasks, per-task exports, final Excel).
- **Session recording** — Stage results interactively, then **`.dump`** to emit a runnable YAML script.
- **Logging** — Append-only **`sheetql.log`** with a clear session header each run.
- **Optional native build** — **`python build.py`** (Nuitka) for a standalone Windows executable.

---

## Installation

**Python 3.9+**

```bash
git clone https://github.com/uzairmukadam/sheetql.git
cd sheetql
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate
```

### Option A — Run without installing the package

```bash
pip install -r requirements.txt
python sheet_ql.py
```

`sheet_ql.py` supports **interactive** mode and **`--run` / `-r`** for a YAML file. For subcommands **`query`**, **`inspect`**, **`run`**, install the package (Option B).

### Option B — Install the `sheetql` CLI

```bash
pip install -e .
# Recommended optional stacks:
pip install -e ".[all]"
```

| Extra | Purpose |
|-------|---------|
| `ui` | `prompt_toolkit` + Pygments: highlighting, completions, history UI |
| `perf` | Faster Excel read (`python-calamine`) + streaming Excel write (`xlsxwriter`) |
| `batch` | PyYAML for scripts and `.dump` / `run` |
| `all` | `ui` + `perf` + `batch` |
| `dev` | Tests + Nuitka (`pip install -e ".[dev]"`) |

---

## Quick start

```bash
# Interactive (default when no subcommand is given)
sheetql
# or explicitly:
sheetql interactive

# One-off query to the terminal
sheetql query -i sales.csv -q "SELECT sales_rep, SUM(amount) FROM sales_csv GROUP BY 1"

# Inspect table names (and columns) for a file
sheetql inspect -i report.xlsx --schema

# Run a saved pipeline
sheetql run -c pipeline.yml

# Legacy entry (same interactive + batch -r as above)
python sheet_ql.py
python sheet_ql.py -r pipeline.yml
```

---

## Using the interactive shell

1. Start SheetQL; choose files in the dialog (or enter paths if there is no GUI).
2. Tables appear with auto-generated names (see **Table naming** below).
3. Type SQL; end each statement with **`;`**. Continuation lines use the **`->`** prompt.
4. After a **SELECT**-style result, you may **stage** it for a named sheet in a later **`.export`**.
5. Use **meta-commands** (they start with `.` and are handled before the SQL buffer).

### Meta-commands

| Command | Purpose |
|---------|---------|
| `.help` | Full list with short descriptions |
| `.tables` | List DuckDB tables |
| `.files` | Files you opened → table names created from each |
| `.peek <table> [n]` | First **n** rows (`SELECT * … LIMIT n`), default **15**; no staging prompt |
| `.count <table>` | Row count + timing |
| `.schema <table>` | Column names and types |
| `.history` | Numbered history; **`!n`** re-runs query *n* |
| `.load` | Add more files in-session |
| `.rename <old> <new>` | Rename a table or view (Excel tables use the correct DDL) |
| `.export` | Write all staged sheets to one `.xlsx` |
| `.dump [file]` | Write session YAML (default `script.yaml`) |
| `.runscript <file>` | Run a YAML script inside the current session |
| `.cwd` | Show working directory (helpful for paths) |
| `.clear` | Clear the terminal |
| `.exit` / `.quit` | Leave (offers export if something is staged) |

The **bottom status bar** lists a subset of these for quick reference.

### Table naming

| Source | Example table name |
|--------|---------------------|
| `sales_2024.csv` | `sales_2024_csv` |
| `targets.xlsx` sheet `Q1 Goals` | `targets_q1_goals` |
| Leading digits in basename | `t_` prefix, e.g. `t_2026_report_xlsx` |

Use **`.rename`** for shorter aliases in long SQL.

---

## CLI reference

### `sheetql query`

```text
sheetql query -i <file> [more files...] -q "SQL" [options]
```

| Option | Meaning |
|--------|---------|
| `-i` / `--input` | Input files (required) |
| `-q` / `--query` | SQL string |
| `-f` / `--query-file` | Read SQL from a file |
| `--alias new=old` | Rename a relation before running SQL (repeatable) |
| `--limit N` | For `SELECT` / `WITH`, limit is applied **inside DuckDB** when possible |
| `--format` | `table` (default), `csv`, or `json` |
| `-o` / `--output` | Write `.xlsx`, `.csv`, or `.json` |
| `--sheet-name` | Sheet name when writing `.xlsx` |

### `sheetql inspect`

```bash
sheetql inspect -i data.csv
sheetql inspect -i book.xlsx --schema
sheetql inspect -i a.parquet --format json
```

### `sheetql run`

```bash
sheetql run -c monthly_report.yml
```

---

## YAML pipelines

Scripts bundle **inputs** (paths and optional **aliases**), **tasks** (named SQL, optional per-task **export**), optional top-level **`export`** for a combined Excel workbook, **`variables`** for `${NAME}` substitution (falling back to environment variables), and **`options`** such as `memory_limit` and `stop_on_error`.

Example (abbreviated):

```yaml
variables:
  data_dir: ./data
  out_dir: ./out

options:
  memory_limit: "75%"
  stop_on_error: true

inputs:
  - path: ${data_dir}/sales.csv
    alias: sales

tasks:
  - name: ByRegion
    sql: SELECT region, SUM(amount) AS total FROM sales GROUP BY 1
    export:
      path: ${out_dir}/by_region.csv

export:
  path: ${out_dir}/combined.xlsx
```

Run: **`sheetql run -c file.yml`** or **`python sheet_ql.py -r file.yml`**.

---

## Logging & debugging

- Default log file: **`sheetql.log`** (append mode, session separators).
- Verbose console logging: **`--debug`** on the CLI or **`python sheet_ql.py --debug`**.

---

## Building a standalone executable (Windows)

```bash
pip install -e ".[dev]"
python build.py
```

Produces **`dist/sheetql.exe`** (one-file default). A C toolchain (MSVC or MinGW) is required; Nuitka can bootstrap MinGW if needed.

---

## Development & deployment checks

From the repo root (with dev dependencies optional for Black/Ruff):

```bash
pip install -r requirements.txt   # or pip install -e ".[all]" && pip install black ruff
python -m black --check .
python -m ruff check .
python -m unittest discover tests -v
```

GitHub Actions (`.github/workflows/python-app.yml`) runs **Black**, **Ruff**, and **unittest** on Python 3.9–3.11 for pushes and pull requests to **`main`**.

---

## Troubleshooting

| Symptom | What to try |
|---------|----------------|
| Red items on the welcome “engine” line | Install optional deps: **`pip install -e ".[all]"`** or **`pip install -r requirements.txt`**. |
| Large file memory pressure | Prefer **Parquet/CSV** for huge scans; tune **`memory_limit`** in YAML `options`. |
| Completions not appearing while typing | Ensure **`pip install -e ".[ui]"`**; the shell intentionally trades **Ctrl+R incremental search** for **complete-while-typing** (see prompt_toolkit behavior). |
| Unexpected errors | Inspect **`sheetql.log`** for the full traceback. |

---

## Project layout

```text
sheetql/
├── sheetql/
│   ├── __init__.py
│   ├── __main__.py          # python -m sheetql
│   ├── cli.py               # interactive | query | inspect | run
│   ├── engine.py            # SheetQL: load, query, export, meta-commands
│   ├── scripting.py         # YAML parsing & validation
│   ├── session.py           # SessionRecorder → .dump YAML
│   ├── completion.py        # SQL completer (prompt_toolkit)
│   ├── naming.py            # Safe SQL identifiers from paths/sheets
│   ├── duckdb_util.py       # Identifier quoting, bulk schema, renames, pragmas
│   ├── constants.py         # Shared defaults
│   ├── deps.py              # Optional dependency probes
│   └── logging.py           # Rich + file logging
├── tests/
│   ├── test_sheet_ql.py
│   └── test_duckdb_util.py
├── sheet_ql.py              # Legacy entry (delegates to package)
├── build.py                 # Nuitka helper
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## License

MIT — see [LICENSE](LICENSE).
