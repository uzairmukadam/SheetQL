import os
from typing import List, Dict, Any

from sheetql.constants import DEFAULT_MEMORY_LIMIT
from sheetql.deps import YAML_AVAILABLE

if YAML_AVAILABLE:
    import yaml
else:
    yaml = None  # type: ignore[assignment]


class SessionRecorder:
    """Records session activities to generate YAML scripts."""

    def __init__(self) -> None:
        self.inputs: List[Dict[str, str]] = []
        self.transformations: List[Dict[str, str]] = []
        self.exports: List[Dict[str, str]] = []

    def record_load(self, path: str, alias: str) -> None:
        self.inputs.append({"path": path, "alias": alias})

    def record_query(self, name: str, sql: str) -> None:
        if sql.strip().upper().startswith(("SHOW", "DESCRIBE", "PRAGMA")):
            return
        self.transformations.append({"name": name, "sql": sql})

    def record_export(self, path: str) -> None:
        self.exports.append({"path": path})

    def generate_yaml(self) -> str:
        if not YAML_AVAILABLE or yaml is None:
            return "# Error: PyYAML not installed."

        script: Dict[str, Any] = {}

        # Heuristic variables to make paths easier to tweak.
        variables: Dict[str, str] = {}

        input_paths = [i["path"] for i in self.inputs if "path" in i]
        export_path = self.exports[-1]["path"] if self.exports else None

        # Derive a common data directory, if possible.
        if len(input_paths) > 1:
            try:
                common = os.path.commonpath(input_paths)
            except ValueError:
                common = ""
        else:
            common = ""

        if common and common not in (os.path.sep, "."):
            variables["data_dir"] = common.replace("\\", "/")

        # Derive an output directory, if possible.
        if export_path:
            out_dir = os.path.dirname(export_path)
            if out_dir:
                variables["out_dir"] = out_dir.replace("\\", "/")

        if variables:
            script["variables"] = variables

        # Optional engine options. These are safe defaults matching the runtime behavior.
        script["options"] = {
            "memory_limit": DEFAULT_MEMORY_LIMIT,
            "stop_on_error": True,
        }

        def _rewrite_path(path: str) -> str:
            rewritten = path.replace("\\", "/")
            data_dir = variables.get("data_dir")
            if data_dir and rewritten.startswith(data_dir + "/"):
                rewritten = rewritten.replace(data_dir + "/", "${data_dir}/", 1)
            out_dir = variables.get("out_dir")
            if out_dir and rewritten.startswith(out_dir + "/"):
                rewritten = rewritten.replace(out_dir + "/", "${out_dir}/", 1)
            return rewritten

        inputs_serialized = []
        for item in self.inputs:
            serialized = dict(item)
            if "path" in serialized:
                serialized["path"] = _rewrite_path(serialized["path"])
            inputs_serialized.append(serialized)

        if inputs_serialized:
            script["inputs"] = inputs_serialized

        if self.transformations:
            script["tasks"] = self.transformations

        if export_path:
            script["export"] = {"path": _rewrite_path(export_path)}

        return yaml.safe_dump(script, sort_keys=False, default_flow_style=False)
