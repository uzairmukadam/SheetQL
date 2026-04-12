"""Background-friendly GitHub Releases latest-tag check (stdlib only)."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from sheetql.constants import (
    GITHUB_RELEASES_REPO,
    UPDATE_CHECK_TIMEOUT_SEC,
    get_package_version,
)


class UpdateCheckOutcome(Enum):
    UP_TO_DATE = "up_to_date"
    UPDATE_AVAILABLE = "update_available"
    CHECK_FAILED = "check_failed"


@dataclass(frozen=True)
class UpdateCheckResult:
    outcome: UpdateCheckOutcome
    """Latest release tag from GitHub when outcome is UPDATE_AVAILABLE (e.g. v4.0.1)."""
    remote_tag: Optional[str] = None


def _version_tuple(version_string: str) -> Tuple[int, ...]:
    """Parse '4.0.0' or 'v4.0.0-rc1' into a tuple of leading numeric segments."""
    s = version_string.strip()
    if s.lower().startswith("v"):
        s = s[1:].lstrip()
    parts: list[int] = []
    for segment in s.split("."):
        m = re.match(r"^(\d+)", segment)
        parts.append(int(m.group(1)) if m else 0)
    return tuple(parts)


def _compare_versions(current: str, remote_tag: str) -> int:
    """
    Return -1 if current < remote, 0 if equal, 1 if current > remote.
    remote_tag may include a leading 'v'.
    """
    a = _version_tuple(current)
    b = _version_tuple(remote_tag)
    maxlen = max(len(a), len(b))
    ap = a + (0,) * (maxlen - len(a))
    bp = b + (0,) * (maxlen - len(b))
    if ap < bp:
        return -1
    if ap > bp:
        return 1
    return 0


def fetch_latest_release_tag(
    repo: str = GITHUB_RELEASES_REPO,
    *,
    timeout: float = UPDATE_CHECK_TIMEOUT_SEC,
    user_agent: str,
) -> Optional[str]:
    """
    Return tag_name from GitHub releases/latest API, or None on any failure.
    """
    url = f"https://api.github.com/repos/{repo}/releases/latest"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": user_agent,
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        data = json.loads(body)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return None
    except json.JSONDecodeError:
        return None

    tag = data.get("tag_name")
    if not isinstance(tag, str) or not tag.strip():
        return None
    tag = tag.strip()
    if not re.match(r"^v?\d", tag, re.IGNORECASE):
        return None
    return tag


def run_update_check(
    *,
    repo: str = GITHUB_RELEASES_REPO,
    timeout: float = UPDATE_CHECK_TIMEOUT_SEC,
) -> UpdateCheckResult:
    """
    Compare installed version to latest GitHub release tag.
    """
    current = get_package_version()
    ua = f"SheetQL/{current} (release check)"
    tag = fetch_latest_release_tag(repo, timeout=timeout, user_agent=ua)
    if tag is None:
        return UpdateCheckResult(outcome=UpdateCheckOutcome.CHECK_FAILED)

    cmp_ = _compare_versions(current, tag)
    if cmp_ < 0:
        return UpdateCheckResult(
            outcome=UpdateCheckOutcome.UPDATE_AVAILABLE,
            remote_tag=tag,
        )
    return UpdateCheckResult(outcome=UpdateCheckOutcome.UP_TO_DATE)


__all__ = [
    "UpdateCheckOutcome",
    "UpdateCheckResult",
    "fetch_latest_release_tag",
    "run_update_check",
    "_compare_versions",
    "_version_tuple",
]
