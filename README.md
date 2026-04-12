# SheetQL

**SheetQL** is a small, focused tool for **quick analytics on flat files** like CSV, Excel, Parquet, and JSON using **full SQL** in the terminal. There is no database server, no ETL platform, and no spreadsheet GUI: you point SheetQL at files, get **DuckDB** tables and views, run queries, optionally stage results, and export to Excel or other formats.

It is built for analysts, engineers, and anyone who wants **ad-hoc questions answered fast** on local data without standing up heavier tooling.

**Primary way to run it:** clone this repository, create a virtual environment (recommended), install dependencies with **`pip install -r requirements.txt`**, and run **`python sheet_ql.py`**. Optional: install as a package with **`pip install -e .`** if you want the **`sheetql`** command on your PATH, or use a pre-built **Windows portable build** from [Releases](https://github.com/uzairmukadam/sheetql/releases) (see below).

---

## What makes SheetQL different

| Specialty | What it means for you |
|-----------|-------------------------|
| **DuckDB in-process** | SQL runs inside your Python process: fast, local, no server to install or secure. |
| **Zero-copy paths** | Parquet, CSV, and JSON are exposed as **views** where possible so DuckDB can scan them efficiently without loading everything into RAM first. |
| **Excel without a server** | `.xlsx` / `.xls` are read (optionally via a fast Rust reader), normalized, and **registered** as tables so you can join them with CSV/Parquet in one session. |
| **One interactive loop** | Pick files (or paths), run SQL ended with **`;`**, see a Rich preview table, optional **staging** for a multi-sheet Excel export, and **meta-commands** for common chores. |
| **CLI for scripts** | From the repo: `python sheet_ql.py query`, `inspect`, and `run` (or `sheetql …` after `pip install -e .`). |
| **YAML replay** | Describe inputs, SQL tasks, and exports in YAML; run the same pipeline later. **`.dump`** can generate YAML from what you did interactively. |
| **Sensible guardrails** | Identifiers used in `DESCRIBE` / DDL are **quoted safely**; YAML `memory_limit` values are **validated** before being sent to DuckDB. |

---

## Features

- **Interactive SQL shell** — Multiline queries (wait for `;`), Rich result tables, execution time on each run, optional staging → **`.export`** to one styled `.xlsx`.
- **Completions while you type** — With **`prompt_toolkit`** (included in **`requirements.txt`**): SQL highlighting, **Tab** completion, suggestions from history (**→** to accept when shown). **↑ / ↓** move through prior lines (Ctrl+R incremental search is disabled so completions stay on while typing).
- **Meta-commands** — `.peek`, `.count`, `.files`, `.cwd`, `.clear`, plus tables/schema/load/rename/export/dump/runscript/history/exit (see below).
- **`query` / `inspect` / `run` subcommands** — e.g. `python sheet_ql.py query …` (same flags as `sheetql query …` if the package is installed).
- **Session recording** — Stage results interactively, then **`.dump`** to emit a runnable YAML script.
- **Logging** — Append-only **`sheetql.log`** with a clear session header each run.
- **Standalone Windows build** — [GitHub Releases](https://github.com/uzairmukadam/sheetql/releases) (folder zip and/or single **`sheetql.exe`**, depending on the asset), or build from source with **`build.bat`** / **`python build.py`**.

---

## Installation

**Python 3.9+**

### Recommended: run from a clone (no package install required)

```bash
git clone https://github.com/uzairmukadam/sheetql.git
cd sheetql
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate

pip install -r requirements.txt
python sheet_ql.py
```

- **`python sheet_ql.py`** — interactive SQL shell (file picker, meta-commands, `.dump`, etc.).
- **`python sheet_ql.py -r script.yml`** — run a YAML pipeline without entering the shell.
- **`python sheet_ql.py query …`**, **`inspect …`**, **`run …`** — same subcommands as the `sheetql` CLI; pass **`--help`** after the subcommand for options.
- **`python sheet_ql.py --help`** — full CLI help.

[`requirements.txt`](requirements.txt) includes core, UI (highlighting, completions, bottom bar), performance Excel readers/writers, and PyYAML so the above works out of the box.

### Optional: install the `sheetql` command on your PATH

If you prefer typing **`sheetql`** instead of **`python sheet_ql.py`**:

```bash
pip install -e .
# Same dependency bundles as pyproject extras:
pip install -e ".[all]"
```

| Extra | Purpose |
|-------|---------|
| `ui` | `prompt_toolkit` + Pygments (included in `requirements.txt` already) |
| `perf` | `python-calamine`, `xlsxwriter` (included in `requirements.txt`) |
| `batch` | PyYAML (included in `requirements.txt`) |
| `all` | `ui` + `perf` + `batch` |
| `dev` | Tests + Nuitka + PyInstaller for **`build.py`** |

### Pre-built executable (Windows, GitHub Releases)

If you do **not** want a Python environment on Windows, use **[Releases](https://github.com/uzairmukadam/sheetql/releases)**.

- **Folder build (recommended for speed)** — Download and **unzip** the archive, then run **`sheetql.exe`** **inside** that folder (next to **`_internal`** and DLLs). Do not move only the `.exe` out of the folder; the app needs the rest of the files.
- **Single-file `sheetql.exe`** — If the release includes a lone executable, run it from any folder; usage is the same (`sheetql.exe`, `sheetql.exe query …`, etc.). That mode unpacks to a temp directory each run, so startup and exit are slower.

Relative paths follow your current working directory. Unsigned builds may show SmartScreen — **More info → Run anyway** if you trust the release.

---

## Quick start

From the repo root (venv activated, **`pip install -r requirements.txt`** done):

```bash
# Interactive shell
python sheet_ql.py

# YAML batch file
python sheet_ql.py -r pipeline.yml

# One-off query
python sheet_ql.py query -i sales.csv -q "SELECT sales_rep, SUM(amount) FROM sales_csv GROUP BY 1"

python sheet_ql.py inspect -i report.xlsx --schema
python sheet_ql.py run -c pipeline.yml
```

If you ran **`pip install -e .`**, you can replace **`python sheet_ql.py`** with **`sheetql`** (e.g. `sheetql query …`). With the **release Windows build**, run **`sheetql.exe`** from the unzipped folder (or the single-file asset, if provided) the same way.

---

## Using the interactive shell

1. Run **`python sheet_ql.py`** from the repo root; choose files in the dialog (or enter paths if there is no GUI).
2. Tables appear with auto-generated names (see **Table naming** below).
3. Type SQL; end each statement with **`;`**. Continuation lines use the **`->`** prompt.
4. After a **SELECT**-style result, you may **stage** it for a named sheet in a later **`.export`**.
5. Use **meta-commands** (they start with `.` and are handled before the SQL buffer).
6. **Update check (interactive only)** — On launch, SheetQL may query the public **GitHub Releases** API in a **background thread** (no blocking dialogs). The **bottom bar** can show **Update available: v…** if a release tag is newer than your installed version, or **Could not check for updates** if the network is unavailable or the request fails (errors are not shown as popups).

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

From a clone, invoke subcommands with **`python sheet_ql.py …`** (examples below use that form). If you installed the package, replace the prefix with **`sheetql`**.

### `query`

```text
python sheet_ql.py query -i <file> [more files...] -q "SQL" [options]
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

### `inspect`

```bash
python sheet_ql.py inspect -i data.csv
python sheet_ql.py inspect -i book.xlsx --schema
python sheet_ql.py inspect -i a.parquet --format json
```

### `run`

```bash
python sheet_ql.py run -c monthly_report.yml
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

Script files may use either **`.yaml`** or **`.yml`**; both are loaded the same way.

Run: **`python sheet_ql.py run -c file.yaml`** (or **`file.yml`**) or **`python sheet_ql.py -r file.yml`**.

---

## Logging & debugging

- Default log file: **`sheetql.log`** (append mode, session separators).
- Verbose console logging: **`python sheet_ql.py --debug`** (or subcommands with **`--debug`**).

---

## Building a standalone executable (Windows)

To produce your own Windows executable (same idea as the GitHub Release artifact), you need packaging/build tools. **`build.bat`** uses an editable install so the frozen app can import the **`sheetql`** package:

1. **One-click** — From the repo root, double-click **`build.bat`**. It runs **`pip install -e ".[all,dev]"`** then **`python build.py --backend auto`** (Nuitka first, then PyInstaller if the C backend fails).
2. **Manual** — After **`pip install -e ".[all,dev]"`**:
   - **`python build.py`** — Nuitka only (needs a working MSVC or Nuitka-downloaded MinGW; may fail on some setups).
   - **`python build.py --backend pyinstaller`** — PyInstaller only (no C compiler; larger binary, usually reliable).

The executable embeds **`assets/SheetQL.ico`** as the app icon. **PyInstaller** defaults to a **folder** build at **`dist/sheetql/sheetql.exe`** (faster startup and exit than a single-file bundle, which unpacks to a temp directory on every run). **Nuitka** still defaults to **`dist/sheetql.exe`**. Use **`python build.py --backend pyinstaller --onefile`** when you want a single **`dist/sheetql.exe`** to attach to a release.

**Shipping a folder build on GitHub:** zip the **entire** **`dist/sheetql`** directory (so unzip → `sheetql/sheetql.exe` and dependencies). **Shipping one file:** build with **`--onefile`** and attach **`dist/sheetql.exe`** only.

---

## Development & deployment checks

From the repo root (with dev dependencies optional for Black/Ruff):

```bash
pip install -r requirements.txt
pip install black ruff pytest   # optional; or pip install -e ".[dev]" for full dev + build stack
python -m black --check .
python -m ruff check .
python -m unittest discover tests -v
```

GitHub Actions (`.github/workflows/python-app.yml`) runs **Black**, **Ruff**, and **unittest** on Python 3.9–3.11 for pushes and pull requests to **`main`**.

Release version: edit **`sheetql/_version.py`** (`__version__`); **`pyproject.toml`** reads it via setuptools `dynamic` metadata. Match GitHub release tags (e.g. **`v3.0.1`**).

---

## Troubleshooting

| Symptom | What to try |
|---------|----------------|
| Red items on the welcome “engine” line | Run **`pip install -r requirements.txt`** from the repo root (or **`pip install -e ".[all]"`**). |
| Large file memory pressure | Prefer **Parquet/CSV** for huge scans; tune **`memory_limit`** in YAML `options`. |
| Completions not appearing while typing | Ensure **`prompt_toolkit`** is installed (**`requirements.txt`** includes it). The shell trades **Ctrl+R incremental search** for **complete-while-typing**. |
| Unexpected errors | Inspect **`sheetql.log`** for the full traceback. |
| **`sheetql.exe` blocked or “unknown publisher”** | Typical for unsigned release builds; use **More info → Run anyway** or unblock the file in file properties. |

---

## Project layout

```text
sheetql/
├── sheetql/
│   ├── __init__.py
│   ├── _version.py          # app version string (CLI, update check, packaging)
│   ├── __main__.py          # python -m sheetql
│   ├── cli.py               # interactive | query | inspect | run
│   ├── engine.py            # SheetQL: load, query, export, meta-commands
│   ├── scripting.py         # YAML parsing & validation
│   ├── session.py           # SessionRecorder → .dump YAML
│   ├── completion.py        # SQL completer (prompt_toolkit)
│   ├── naming.py            # Safe SQL identifiers from paths/sheets
│   ├── duckdb_util.py       # Identifier quoting, bulk schema, renames, pragmas
│   ├── constants.py         # Shared defaults, version, release check, YAML suffixes
│   ├── update_check.py      # GitHub Releases latest-tag check (stdlib HTTP)
│   ├── deps.py              # Optional dependency probes
│   └── logging.py           # Rich + file logging
├── tests/
│   ├── test_sheet_ql.py
│   ├── test_duckdb_util.py
│   └── test_update_check.py
├── sheet_ql.py              # Primary entry: python sheet_ql.py (forwards to sheetql.cli)
├── build.py                 # Nuitka / PyInstaller helper
├── build.bat                # Windows: deps + build.py --backend auto
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## License

MIT — see [LICENSE](LICENSE).
