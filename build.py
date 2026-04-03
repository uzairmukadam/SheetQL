"""
build.py — Nuitka build helper for SheetQL
==========================================

Produces a standalone Windows executable at dist/sheetql.exe.

Requirements
------------
- Nuitka:        pip install nuitka   (or: pip install -e ".[dev]")
- C compiler:    MSVC (Visual Studio) OR MinGW-w64
                 Nuitka will offer to auto-download MinGW-w64 if MSVC is not found.

Usage
-----
    python build.py              # standard release build (single .exe)
    python build.py --debug      # include debug symbols, verbose Nuitka output
    python build.py --no-onefile # directory distribution instead of single exe
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.resolve()
ENTRY_POINT  = PROJECT_ROOT / "sheetql" / "__main__.py"
OUTPUT_DIR   = PROJECT_ROOT / "dist"
BUILD_DIR    = PROJECT_ROOT / "build"
EXE_NAME     = "sheetql"

# Packages that must be explicitly bundled (native extensions or large sub-packages).
INCLUDE_PACKAGES = [
    "duckdb",
    "python_calamine",
    "openpyxl",
    "xlsxwriter",
    "yaml",           # pyyaml
    "prompt_toolkit",
    "pygments",
    "rich",
    "pandas",
    "numpy",
]

# Packages whose data files (XML templates, locale data, DuckDB extensions)
# must be copied alongside the compiled binary.
INCLUDE_PACKAGE_DATA = [
    "openpyxl",   # styles/theme XML files required at runtime
    "duckdb",     # SQL extension files
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_nuitka() -> None:
    """Abort with a helpful message if Nuitka is not installed."""
    try:
        subprocess.check_call(
            [sys.executable, "-m", "nuitka", "--version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(
            "\n[ERROR] Nuitka is not installed.\n"
            "Install it with:\n\n"
            "    pip install nuitka\n\n"
            "Or install all dev dependencies at once:\n\n"
            "    pip install -e \".[dev]\"\n"
        )
        sys.exit(1)


def clean_dirs() -> None:
    """Remove previous build and dist artefacts."""
    for d in (BUILD_DIR, OUTPUT_DIR):
        if d.exists():
            print(f"  Cleaning {d.relative_to(PROJECT_ROOT)} ...")
            shutil.rmtree(d)


def build(*, debug: bool = False, onefile: bool = True) -> None:
    """Invoke Nuitka with all flags required for SheetQL."""
    check_nuitka()

    print("\nSheetQL — Nuitka build")
    print("=" * 40)
    clean_dirs()

    cmd = [sys.executable, "-m", "nuitka"]

    # --- Output mode ---------------------------------------------------------
    if onefile:
        cmd.append("--onefile")
    else:
        cmd.append("--standalone")

    # --- Windows-specific ----------------------------------------------------
    cmd += [
        "--windows-console-mode=force",   # keep terminal window for CLI app
    ]

    # --- Nuitka plugins -------------------------------------------------------
    cmd += [
        "--enable-plugin=tk-inter",       # bundle Tkinter for the file-picker GUI
        "--enable-plugin=numpy",          # satisfies pandas' numpy C-extension internals
    ]

    # --- Package inclusion ---------------------------------------------------
    for pkg in INCLUDE_PACKAGES:
        cmd.append(f"--include-package={pkg}")

    for pkg in INCLUDE_PACKAGE_DATA:
        cmd.append(f"--include-package-data={pkg}")

    # --- Output --------------------------------------------------------------
    cmd += [
        f"--output-filename={EXE_NAME}",
        f"--output-dir={OUTPUT_DIR}",
    ]

    # --- Debug / verbosity ---------------------------------------------------
    if debug:
        cmd += ["--debug", "--verbose"]
    else:
        # Strip docstrings and asserts from release builds for a smaller binary.
        cmd += ["--python-flag=no_docstrings", "--python-flag=no_asserts"]

    # --- Entry point ---------------------------------------------------------
    cmd.append(str(ENTRY_POINT))

    print("\nRunning:")
    print("  " + " \\\n    ".join(str(c) for c in cmd))
    print()

    ret = subprocess.call(cmd)

    if ret != 0:
        print(f"\n[ERROR] Nuitka exited with code {ret}.")
        sys.exit(ret)

    exe_path = OUTPUT_DIR / (EXE_NAME + (".exe" if sys.platform == "win32" else ""))
    print(f"\n✔  Build complete: {exe_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build SheetQL with Nuitka")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Include debug symbols and enable verbose Nuitka output",
    )
    parser.add_argument(
        "--no-onefile",
        dest="onefile",
        action="store_false",
        help="Produce a directory distribution instead of a single .exe",
    )
    args = parser.parse_args()
    build(debug=args.debug, onefile=args.onefile)


if __name__ == "__main__":
    main()
