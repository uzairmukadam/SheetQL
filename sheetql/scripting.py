import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class ScriptConfigError(ValueError):
    pass


# Allowed in options.memory_limit when embedded in SET memory_limit='…' (no string breakouts).
_MEMORY_LIMIT_SAFE = re.compile(r"^[0-9A-Za-z.%+\s_-]+$")


def _validate_memory_limit_option(value: str) -> str:
    v = value.strip()
    if not v:
        raise ScriptConfigError("options.memory_limit must not be empty or whitespace.")
    if len(v) > 64:
        raise ScriptConfigError("options.memory_limit must be at most 64 characters.")
    if not _MEMORY_LIMIT_SAFE.fullmatch(v):
        raise ScriptConfigError(
            "options.memory_limit may only contain letters, digits, spaces, "
            "percent, period, plus, or hyphen (no quotes or semicolons)."
        )
    return v


@dataclass(frozen=True)
class ScriptInput:
    path: str
    alias: Optional[str] = None


@dataclass(frozen=True)
class ScriptTask:
    name: str
    sql: str
    export: Optional["ScriptExport"] = None


@dataclass(frozen=True)
class ScriptExport:
    path: Optional[str] = None
    sheet: Optional[str] = None
    format: Optional[str] = None


@dataclass(frozen=True)
class ScriptOptions:
    memory_limit: Optional[str] = None
    stop_on_error: bool = False
    # Use field(default_factory=dict) — the correct dataclasses idiom for mutable defaults.
    variables: Dict[str, str] = field(default_factory=dict)


def _as_dict(config: Any) -> Dict[str, Any]:
    if not isinstance(config, dict):
        raise ScriptConfigError(
            "Script config must be a mapping/dict at the top level."
        )
    return config


def _ensure_list(value: Any, key: str) -> List[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ScriptConfigError(f"'{key}' must be a list.")
    return value


def _ensure_dict(value: Any, key: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ScriptConfigError(f"'{key}' must be a mapping/dict.")
    return value


def _substitute_vars(text: str, variables: Dict[str, str]) -> str:
    """
    Replace ${VAR} placeholders. Resolution order:
    1. `variables` dict (from YAML `variables:` block)
    2. `os.environ` (environment variables — enables ${HOME}, ${USERPROFILE}, etc.)
    3. Leave the placeholder unchanged if not found in either.
    """

    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        return str(variables.get(name) or os.environ.get(name, match.group(0)))

    return re.sub(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", repl, text)


def _parse_export_obj(
    raw_export: Any, key: str, variables: Dict[str, str]
) -> "ScriptExport":
    if not isinstance(raw_export, dict):
        raise ScriptConfigError(f"'{key}' must be a mapping/dict.")
    export_path = raw_export.get("path")
    if export_path is not None and not isinstance(export_path, str):
        raise ScriptConfigError(f"'{key}.path' must be a string if provided.")
    sheet = raw_export.get("sheet")
    if sheet is not None and not isinstance(sheet, str):
        raise ScriptConfigError(f"'{key}.sheet' must be a string if provided.")
    fmt = raw_export.get("format")
    if fmt is not None and not isinstance(fmt, str):
        raise ScriptConfigError(f"'{key}.format' must be a string if provided.")
    return ScriptExport(
        path=_substitute_vars(export_path, variables) if export_path else None,
        sheet=sheet,
        format=str(fmt) if fmt is not None else None,
    )


def parse_script_config(
    config: Any,
) -> Tuple[List[ScriptInput], List[ScriptTask], Optional[ScriptExport], ScriptOptions]:
    cfg = _as_dict(config)

    raw_options = _ensure_dict(cfg.get("options"), "options")
    variables = _ensure_dict(cfg.get("variables"), "variables")
    variables_str = {str(k): str(v) for k, v in variables.items()}

    stop_on_error = bool(
        cfg.get("stop_on_error", raw_options.get("stop_on_error", False))
    )
    memory_limit = raw_options.get("memory_limit")
    memory_limit = str(memory_limit) if memory_limit is not None else None
    if memory_limit is not None:
        memory_limit = _validate_memory_limit_option(memory_limit)

    options = ScriptOptions(
        memory_limit=memory_limit, stop_on_error=stop_on_error, variables=variables_str
    )

    inputs: List[ScriptInput] = []
    for i, item in enumerate(_ensure_list(cfg.get("inputs"), "inputs")):
        if not isinstance(item, dict):
            raise ScriptConfigError(f"'inputs[{i}]' must be a mapping/dict.")
        path = item.get("path")
        if not path or not isinstance(path, str):
            raise ScriptConfigError(f"'inputs[{i}].path' must be a non-empty string.")
        alias = item.get("alias")
        if alias is not None and not isinstance(alias, str):
            raise ScriptConfigError(
                f"'inputs[{i}].alias' must be a string if provided."
            )
        inputs.append(
            ScriptInput(path=_substitute_vars(path, options.variables), alias=alias)
        )

    tasks: List[ScriptTask] = []
    for i, item in enumerate(_ensure_list(cfg.get("tasks"), "tasks")):
        if not isinstance(item, dict):
            raise ScriptConfigError(f"'tasks[{i}]' must be a mapping/dict.")
        name = item.get("name")
        sql = item.get("sql")
        if not name or not isinstance(name, str):
            raise ScriptConfigError(f"'tasks[{i}].name' must be a non-empty string.")
        if not sql or not isinstance(sql, str):
            raise ScriptConfigError(f"'tasks[{i}].sql' must be a non-empty string.")
        task_export = None
        if "export" in item and item.get("export") is not None:
            task_export = _parse_export_obj(
                item.get("export"), f"tasks[{i}].export", options.variables
            )
        tasks.append(
            ScriptTask(
                name=name,
                sql=_substitute_vars(sql, options.variables),
                export=task_export,
            )
        )

    export: Optional[ScriptExport] = None
    raw_export = cfg.get("export")
    if raw_export is not None:
        export = _parse_export_obj(raw_export, "export", options.variables)

    return inputs, tasks, export, options


def resolve_alias_targets(
    loaded_files_map: Dict[str, List[str]], script_path: str
) -> List[str]:
    """
    Map a configured input path to the list of DuckDB table/view names that were produced when
    that file was loaded. Matching tries full normalized path first, then filename-only match.
    """
    for loaded_path, tables in loaded_files_map.items():
        if os.path.normpath(loaded_path) == os.path.normpath(
            script_path
        ) or os.path.basename(loaded_path) == os.path.basename(script_path):
            return tables
    return []


__all__ = [
    "ScriptConfigError",
    "ScriptInput",
    "ScriptTask",
    "ScriptExport",
    "ScriptOptions",
    "parse_script_config",
    "resolve_alias_targets",
]
