from importlib.util import find_spec

CALAMINE_AVAILABLE = find_spec("python_calamine") is not None
XLSXWRITER_AVAILABLE = find_spec("xlsxwriter") is not None

try:
    from prompt_toolkit import PromptSession  # noqa: F401
    from prompt_toolkit.completion import Completer, Completion  # noqa: F401
    from prompt_toolkit.lexers import PygmentsLexer  # noqa: F401
    from pygments.lexers.sql import SqlLexer  # noqa: F401
    from prompt_toolkit.styles import Style  # noqa: F401

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False


try:
    import yaml  # noqa: F401

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


try:
    import tkinter as tk  # noqa: F401
    from tkinter import filedialog  # noqa: F401

    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False


__all__ = [
    "CALAMINE_AVAILABLE",
    "XLSXWRITER_AVAILABLE",
    "PROMPT_TOOLKIT_AVAILABLE",
    "YAML_AVAILABLE",
    "TKINTER_AVAILABLE",
]
