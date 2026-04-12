"""
Entry point for ``python -m sheetql`` (after ``pip install -e .``) and for frozen builds.

From a git clone, prefer::

    python sheet_ql.py [subcommand] [options]

Equivalent when the package is installed::

    python -m sheetql [subcommand] [options]
    python -m sheetql --help
    python -m sheetql --version
"""

import sys

from sheetql.cli import main

if __name__ == "__main__":
    sys.exit(main())
