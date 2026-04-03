"""
Entry point for ``python -m sheetql`` and Nuitka compilation.

Usage::

    python -m sheetql [subcommand] [options]
    python -m sheetql --help
    python -m sheetql --version
"""
import sys

from sheetql.cli import main

if __name__ == "__main__":
    sys.exit(main())
