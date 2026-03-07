"""Environment-based ClickHouse connection configuration.

Supports multiple named profiles via structured env vars and a
backward-compatible flat env layer that creates/overrides the default
profile.

Structured (named profiles)::

    MCP_CLICKHOUSE_PROFILES_<NAME>_DSN=clickhouse://...
    MCP_CLICKHOUSE_PROFILES_<NAME>_DESCRIPTION=...
    MCP_CLICKHOUSE_PROFILES_<NAME>_QUERY_MAX_ROWS=5000
    MCP_CLICKHOUSE_PROFILES_<NAME>_QUERY_COMMAND_TIMEOUT_SECONDS=60

Flat (default profile only, backward compatible)::

    MCP_CLICKHOUSE_DSN=clickhouse://...
    MCP_CLICKHOUSE_DESCRIPTION=...
    MCP_CLICKHOUSE_QUERY_MAX_ROWS=5000
    MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS=60

Flat vars always win over structured vars for the default profile.
Profile names are case-insensitive and must be alphanumeric (no
underscores).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

DEFAULT_PROFILE_NAME = "default"

HARD_ROW_LIMIT = 50_000
HARD_COMMAND_TIMEOUT_SECONDS = 300

_DEFAULT_DSN = "http://default:@localhost:8123/default"
_DEFAULT_QUERY_MAX_ROWS = 5_000
_DEFAULT_QUERY_COMMAND_TIMEOUT_SECONDS = 30

_STRUCTURED_PREFIX = "MCP_CLICKHOUSE_PROFILES_"

_FLAT_MAP: dict[str, str] = {
    "MCP_CLICKHOUSE_DSN": "dsn",
    "MCP_CLICKHOUSE_DESCRIPTION": "description",
    "MCP_CLICKHOUSE_QUERY_MAX_ROWS": "query_max_rows",
    "MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS": "query_command_timeout_seconds",
}

_KNOWN_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("QUERY_COMMAND_TIMEOUT_SECONDS", "query_command_timeout_seconds"),
    ("QUERY_MAX_ROWS", "query_max_rows"),
    ("DESCRIPTION", "description"),
    ("DSN", "dsn"),
)

_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9]+$")


@dataclass
class _ProfileData:
    dsn: str | None = None
    description: str | None = None
    query_max_rows: int = _DEFAULT_QUERY_MAX_ROWS
    query_command_timeout_seconds: int = _DEFAULT_QUERY_COMMAND_TIMEOUT_SECONDS


@dataclass
class _Registry:
    profiles: dict[str, _ProfileData] = field(default_factory=dict)


_registry: _Registry | None = None


def _parse_structured_profiles() -> dict[str, dict[str, str]]:
    """Scan env for ``MCP_CLICKHOUSE_PROFILES_<NAME>_<FIELD>`` keys."""
    result: dict[str, dict[str, str]] = {}
    for key, value in os.environ.items():
        if not key.startswith(_STRUCTURED_PREFIX):
            continue
        remainder = key[len(_STRUCTURED_PREFIX) :]
        name, field_key = _split_profile_remainder(remainder)
        if name is None:
            continue
        result.setdefault(name, {})[field_key] = value
    return result


def _split_profile_remainder(remainder: str) -> tuple[str | None, str]:
    """Extract (profile_name, field_key) from the portion after the prefix.

    Tries each known suffix longest-first so that multi-word suffixes
    like ``QUERY_COMMAND_TIMEOUT_SECONDS`` are matched before shorter
    ones.  Returns ``(None, "")`` when the remainder is not parseable.
    """
    for suffix, field_key in _KNOWN_SUFFIXES:
        if remainder.endswith("_" + suffix):
            name = remainder[: -(len(suffix) + 1)]
            if _PROFILE_NAME_RE.fullmatch(name):
                return name.lower(), field_key
    return None, ""


def _build_default_from_flat() -> dict[str, str]:
    """Read flat ``MCP_CLICKHOUSE_*`` vars into profile-field dict."""
    result: dict[str, str] = {}
    for env_key, field_key in _FLAT_MAP.items():
        value = os.environ.get(env_key)
        if value is not None:
            result[field_key] = value
    return result


def _materialize(raw: dict[str, str]) -> _ProfileData:
    """Convert a raw ``{field_key: str_value}`` dict into a ``_ProfileData``."""
    data = _ProfileData()
    if "dsn" in raw:
        data.dsn = raw["dsn"]
    desc = raw.get("description", "")
    data.description = desc.strip() if desc and desc.strip() else None
    if "query_max_rows" in raw:
        data.query_max_rows = _clamp_int(
            raw["query_max_rows"], _DEFAULT_QUERY_MAX_ROWS, HARD_ROW_LIMIT
        )
    if "query_command_timeout_seconds" in raw:
        data.query_command_timeout_seconds = _clamp_int(
            raw["query_command_timeout_seconds"],
            _DEFAULT_QUERY_COMMAND_TIMEOUT_SECONDS,
            HARD_COMMAND_TIMEOUT_SECONDS,
        )
    return data


def _clamp_int(raw: str, default: int, max_val: int) -> int:
    try:
        return min(int(raw), max_val)
    except (ValueError, TypeError):
        return default


def _resolve_profiles() -> _Registry:
    """Merge structured + flat env vars into a registry (cached)."""
    structured = _parse_structured_profiles()
    flat = _build_default_from_flat()

    merged: dict[str, dict[str, str]] = {}
    for name, fields in structured.items():
        merged[name] = dict(fields)

    if flat:
        default_raw = merged.get(DEFAULT_PROFILE_NAME, {})
        default_raw.update(flat)
        merged[DEFAULT_PROFILE_NAME] = default_raw

    if not merged:
        merged[DEFAULT_PROFILE_NAME] = {"dsn": _DEFAULT_DSN}

    registry = _Registry()
    for name, raw in merged.items():
        registry.profiles[name] = _materialize(raw)
    return registry


def _get_registry() -> _Registry:
    global _registry  # noqa: PLW0603
    if _registry is None:
        _registry = _resolve_profiles()
    return _registry


def reset_registry() -> None:
    """Drop cached profiles so the next access re-reads env vars.

    Intended for tests only.
    """
    global _registry  # noqa: PLW0603
    _registry = None


def _lookup(profile: str | None) -> tuple[str, _ProfileData]:
    """Return ``(name, data)`` for a profile, raising on unknown names."""
    name = (profile or "").strip().lower() or DEFAULT_PROFILE_NAME
    reg = _get_registry()
    data = reg.profiles.get(name)
    if data is None:
        available = ", ".join(sorted(reg.profiles))
        raise ValueError(
            f"MCP ClickHouse profile '{name}' was not found. "
            f"Available profiles: {available}"
        )
    return name, data


# -- Public API ----------------------------------------------------------------


def get_profiles() -> list[dict[str, Any]]:
    """Return all configured profiles with name and description."""
    reg = _get_registry()
    return [
        {"name": name, "description": data.description}
        for name, data in reg.profiles.items()
    ]


def get_client(profile: str | None = None) -> Client:
    """Build a ClickHouse client for the given profile.

    If *profile* is ``None`` or empty the default profile is used.
    """
    _, data = _lookup(profile)
    dsn = data.dsn or _DEFAULT_DSN
    return clickhouse_connect.get_client(dsn=dsn)


def get_limits(profile: str | None = None) -> dict[str, Any]:
    """Return execution limits for the given profile."""
    _, data = _lookup(profile)
    return {
        "query": {
            "max_rows": {
                "value": data.query_max_rows,
                "description": (
                    "Row cap applied to every query. Use LIMIT for pagination."
                ),
                "is_overridable": False,
                "scope": "query",
            },
            "hard_row_limit": {
                "value": HARD_ROW_LIMIT,
                "description": (
                    "Absolute row ceiling; max_rows is clamped to this value."
                ),
                "is_overridable": False,
                "scope": "query",
            },
            "command_timeout_seconds": {
                "value": data.query_command_timeout_seconds,
                "description": (
                    "Maximum execution time allowed for a query before it is "
                    "terminated."
                ),
                "is_overridable": False,
                "scope": "query",
            },
        },
    }


def get_max_rows(profile: str | None = None) -> int:
    """Return the max_rows limit for the given profile (for run_query)."""
    _, data = _lookup(profile)
    return data.query_max_rows


def get_command_timeout(profile: str | None = None) -> int:
    """Return the command timeout in seconds for the given profile."""
    _, data = _lookup(profile)
    return data.query_command_timeout_seconds
