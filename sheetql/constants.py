"""Shared defaults for engine configuration and generated scripts."""

from ._version import __version__ as PACKAGE_VERSION

DEFAULT_MEMORY_LIMIT = "75%"

GITHUB_RELEASES_REPO = "uzairmukadam/sheetql"
UPDATE_CHECK_TIMEOUT_SEC = 5.0

# Batch / `.runscript` configs: both extensions are supported (PyYAML does not care).
YAML_SCRIPT_SUFFIXES = (".yaml", ".yml")


def get_package_version() -> str:
    """Version string for CLI, update check, and User-Agent (from ``sheetql/_version.py``)."""
    return PACKAGE_VERSION


__all__ = [
    "DEFAULT_MEMORY_LIMIT",
    "GITHUB_RELEASES_REPO",
    "PACKAGE_VERSION",
    "UPDATE_CHECK_TIMEOUT_SEC",
    "YAML_SCRIPT_SUFFIXES",
    "get_package_version",
]
