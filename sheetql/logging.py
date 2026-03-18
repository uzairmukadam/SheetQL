import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


def setup_logging(debug_mode: bool = False, log_file: Optional[str] = "sheetql.log") -> logging.Logger:
    """
    Configure and return the main SheetQL logger.

    Parameters
    ----------
    debug_mode:
        Whether to enable verbose debug logging to the console.
    log_file:
        Path to the log file to write detailed logs to. If None, file logging is disabled.
    """
    logger = logging.getLogger("SheetQL")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []

    if log_file:
        file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        )
        file_handler.setFormatter(file_formatter)
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

