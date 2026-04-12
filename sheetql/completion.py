import re
from typing import Dict, List, TYPE_CHECKING, Iterator

from sheetql.deps import PROMPT_TOOLKIT_AVAILABLE

if TYPE_CHECKING:
    from prompt_toolkit.completion import CompleteEvent
    from prompt_toolkit.document import Document

if PROMPT_TOOLKIT_AVAILABLE:
    from prompt_toolkit.completion import Completer, Completion
else:
    # Lightweight fallbacks to keep type-checkers happy when prompt_toolkit is missing.
    class Completer:  # type: ignore[override]
        pass

    class Completion:  # type: ignore[override]
        def __init__(self, *args, **kwargs) -> None:
            pass


# Table name after FROM / JOIN / comma in FROM list (DuckDB identifiers: letters, digits, _).
_TABLE_PREFIX = re.compile(
    r"(?:^|\s)"
    r"(?:FROM|JOIN|"
    r"(?:LEFT|RIGHT|INNER|CROSS)\s+JOIN|"
    r"FULL\s+OUTER\s+JOIN|"
    r"UPDATE|INTO|DESCRIBE)\s+"
    r"([\w]*)$",
    re.IGNORECASE,
)
_COMMA_TABLE = re.compile(r",\s*([\w]*)$", re.IGNORECASE)


class SheetQLCompleter(Completer):
    """Context-aware autocompletion for the interactive SQL shell."""

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

    def _yield_tables(self, prefix: str) -> Iterator[Completion]:
        """Complete DuckDB table/view names; prefix is the partial identifier (may be '')."""
        plen = len(prefix)
        for t in sorted(self.schema_cache.keys()):
            if not t.lower().startswith(prefix.lower()):
                continue
            yield Completion(
                t, start_position=-plen if plen else 0, display_meta="Table"
            )

    def get_completions(
        self, document: "Document", complete_event: "CompleteEvent"
    ) -> Iterator[Completion]:
        if not PROMPT_TOOLKIT_AVAILABLE:
            return

        text = document.text_before_cursor

        m = _COMMA_TABLE.search(text)
        if m:
            prefix = m.group(1) or ""
            yield from self._yield_tables(prefix)
            return

        m = _TABLE_PREFIX.search(text)
        if m:
            prefix = m.group(1) or ""
            yield from self._yield_tables(prefix)
            return

        # SQL tokens (alnum + underscore), not "big words" — WORD=True breaks identifiers.
        word = document.get_word_before_cursor(WORD=False)
        upper_text = document.text_before_cursor.upper()
        parts = upper_text.split()
        last_word = ""

        if parts:
            if text.endswith(" ") or word == "":
                last_word = parts[-1]
            elif len(parts) > 1:
                last_word = parts[-2]

        tables = list(self.schema_cache.keys())
        suggestions: List[tuple[str, str]] = []

        join_like = {"FROM", "JOIN", "UPDATE", "INTO", "DESCRIBE"}
        if last_word in join_like:
            suggestions.extend((t, "Table") for t in tables)
        elif word == "":
            # Empty prefix: offering every column explodes the menu and stalls the UI.
            suggestions.extend((k, "Keyword") for k in self.keywords)
            suggestions.extend((t, "Table") for t in tables)
        else:
            suggestions.extend((k, "Keyword") for k in self.keywords)
            suggestions.extend((t, "Table") for t in tables)
            for table_name in tables:
                for c in self.schema_cache.get(table_name, []):
                    suggestions.append((c, f"Column ({table_name})"))

        wl = word.lower()
        for suggestion, meta in suggestions:
            if suggestion.lower().startswith(wl):
                yield Completion(
                    suggestion,
                    start_position=-len(word),
                    display_meta=meta,
                )
