from typing import Dict, List

from sheetql.deps import PROMPT_TOOLKIT_AVAILABLE

if PROMPT_TOOLKIT_AVAILABLE:
    from prompt_toolkit.completion import Completer, Completion
else:
    # Lightweight fallbacks to keep type-checkers happy when prompt_toolkit is missing.
    class Completer:  # type: ignore[override]
        pass

    class Completion:  # type: ignore[override]
        def __init__(self, *args, **kwargs) -> None:
            pass


class SheetQLCompleter(Completer):
    """Context-aware autocompletion provider for the interactive shell."""

    def __init__(self, schema_cache: Dict[str, List[str]]):
        self.schema_cache = schema_cache
        self.keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP BY",
            "ORDER BY",
            "LIMIT",
            "JOIN",
            "LEFT JOIN",
            "INNER JOIN",
            "ON",
            "AS",
            "DISTINCT",
            "COUNT",
            "SUM",
            "AVG",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "DESCRIBE",
            "SHOW TABLES",
            "EXPORT",
        ]

    def get_completions(self, document, complete_event):
        if not PROMPT_TOOLKIT_AVAILABLE:
            return

        word = document.get_word_before_cursor(WORD=True)
        upper_text = document.text_before_cursor.upper()
        parts = upper_text.split()
        last_word = ""

        if parts:
            if document.text_before_cursor.endswith(" ") or word == "":
                last_word = parts[-1]
            elif len(parts) > 1:
                last_word = parts[-2]

        tables = list(self.schema_cache.keys())
        suggestions = []

        if last_word in ["FROM", "JOIN", "UPDATE", "INTO", "DESCRIBE"]:
            suggestions.extend([(t, "Table") for t in tables])
        else:
            suggestions.extend([(k, "Keyword") for k in self.keywords])
            suggestions.extend([(t, "Table") for t in tables])
            for table_name in tables:
                cols = self.schema_cache.get(table_name, [])
                suggestions.extend([(c, f"Column ({table_name})") for c in cols])

        for suggestion, meta in suggestions:
            if suggestion.lower().startswith(word.lower()):
                yield Completion(
                    suggestion, start_position=-len(word), display_meta=meta
                )

