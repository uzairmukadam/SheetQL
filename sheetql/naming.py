import re


_INVALID_CHARS_RE = re.compile(r"[^a-zA-Z0-9_]+")


def normalize_name(raw: str) -> str:
    """
    Normalize a raw filename or alias into a safe SQL identifier.

    - Lowercases
    - Replaces non-alphanumeric/underscore with single underscores
    - Strips leading/trailing underscores
    - If the name starts with a digit, prefixes with 't_'
    """
    name = (raw or "").strip()
    name = name.lower()
    name = _INVALID_CHARS_RE.sub("_", name)
    name = name.strip("_")
    name = re.sub(r"_+", "_", name)

    if not name:
        name = "t"

    if name[0].isdigit():
        name = f"t_{name}"

    return name


__all__ = ["normalize_name"]

