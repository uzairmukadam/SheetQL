import logging
import sys
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

_SEPARATOR_WIDTH = 80


def _write_session_header(log_file: str, debug_mode: bool) -> None:
    """Write a visual session separator directly to the log file (non-fatal)."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "DEBUG" if debug_mode else "INFO"
        cmd = " ".join(sys.argv) if sys.argv else "sheetql"
        header = (
            f"\n{'=' * _SEPARATOR_WIDTH}\n"
            f"  SESSION START  |  {timestamp}  |  {mode}  |  {cmd}\n"
            f"{'=' * _SEPARATOR_WIDTH}\n"
        )
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(header)
    except OSError:
        pass  # non-fatal — proceed without the separator


def setup_logging(debug_mode: bool = False, log_file: Optional[str] = "sheetql.log") -> logging.Logger:
    """
    Configure and return the main SheetQL logger.

    Parameters
    ----------
    debug_mode:
        Whether to enable verbose debug logging to the console.
    log_file:
        Path to the log file. If None, file logging is disabled.
        Opened in **append** mode so previous sessions are preserved.
        A timestamped separator is written at the start of each session.
    """
    logger = logging.getLogger("SheetQL")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []

    if log_file:
        _write_session_header(log_file, debug_mode)
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )
        logger.addHandler(file_handler)

    console_level = logging.DEBUG if debug_mode else logging.INFO
    rich_handler = RichHandler(
        console=Console(stderr=True), show_time=False, show_path=False, markup=True
    )
    rich_handler.setLevel(console_level)
    logger.addHandler(rich_handler)

    logging.getLogger("duckdb").setLevel(logging.WARNING)
    logging.getLogger("prompt_toolkit").setLevel(logging.WARNING)
    logging.captureWarnings(True)

    return logger
