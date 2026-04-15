"""ClickHouse connection configuration (user config file + environment).

Supports multiple named profiles via an optional user config file
(~/.config/mcp-clickhousex/config.json), structured env vars, and a
backward-compatible flat env layer. File is read first; env overrides.

Structured (named profiles)::

    MCP_CLICKHOUSE_PROFILES_<NAME>_DSN=clickhouse://...
    MCP_CLICKHOUSE_PROFILES_<NAME>_DESCRIPTION=...
    MCP_CLICKHOUSE_PROFILES_<NAME>_QUERY_MAX_ROWS=500
    MCP_CLICKHOUSE_PROFILES_<NAME>_QUERY_COMMAND_TIMEOUT_SECONDS=60
    MCP_CLICKHOUSE_PROFILES_<NAME>_SNAPSHOT_MAX_ROWS=10000
    MCP_CLICKHOUSE_PROFILES_<NAME>_SNAPSHOT_COMMAND_TIMEOUT_SECONDS=120

Flat (default profile only, backward compatible)::

    MCP_CLICKHOUSE_DSN=clickhouse://...
    MCP_CLICKHOUSE_DESCRIPTION=...
    MCP_CLICKHOUSE_QUERY_MAX_ROWS=500
    MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS=60
    MCP_CLICKHOUSE_SNAPSHOT_MAX_ROWS=10000
    MCP_CLICKHOUSE_SNAPSHOT_COMMAND_TIMEOUT_SECONDS=120

Flat vars always win over structured vars for the default profile.
Profile names are case-insensitive and must be alphanumeric (no
underscores).
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from mcp_clickhousex.models import (
    ExecutionLimits,
    OptionDescriptor,
    Profile,
    QueryLimits,
)

DEFAULT_PROFILE_NAME = "default"

INTERACTIVE_HARD_ROW_LIMIT = 1_000
SNAPSHOT_HARD_ROW_LIMIT = 50_000
HARD_COMMAND_TIMEOUT_SECONDS = 300

_DEFAULT_DSN = "http://default:@localhost:8123/default"
_DEFAULT_QUERY_MAX_ROWS = 500
_DEFAULT_QUERY_COMMAND_TIMEOUT_SECONDS = 30
_DEFAULT_SNAPSHOT_MAX_ROWS = 10_000
_DEFAULT_SNAPSHOT_COMMAND_TIMEOUT_SECONDS = 120

_STRUCTURED_PREFIX = "MCP_CLICKHOUSE_PROFILES_"

_FLAT_MAP: dict[str, str] = {
    "MCP_CLICKHOUSE_DSN": "dsn",
    "MCP_CLICKHOUSE_DESCRIPTION": "description",
    "MCP_CLICKHOUSE_QUERY_MAX_ROWS": "query_max_rows",
    "MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS": "query_command_timeout_seconds",
    "MCP_CLICKHOUSE_SNAPSHOT_MAX_ROWS": "snapshot_max_rows",
    "MCP_CLICKHOUSE_SNAPSHOT_COMMAND_TIMEOUT_SECONDS": (
        "snapshot_command_timeout_seconds"
    ),
}

_KNOWN_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("SNAPSHOT_COMMAND_TIMEOUT_SECONDS", "snapshot_command_timeout_seconds"),
    ("QUERY_COMMAND_TIMEOUT_SECONDS", "query_command_timeout_seconds"),
    ("SNAPSHOT_MAX_ROWS", "snapshot_max_rows"),
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
    snapshot_max_rows: int = _DEFAULT_SNAPSHOT_MAX_ROWS
    snapshot_command_timeout_seconds: int = _DEFAULT_SNAPSHOT_COMMAND_TIMEOUT_SECONDS


@dataclass
class _Registry:
    profiles: dict[str, _ProfileData] = field(default_factory=dict)


_registry: _Registry | None = None

_USER_CONFIG_DIR = "mcp-clickhousex"
_USER_CONFIG_FILENAME = "config.json"

_JSON_PROFILE_KEYS: tuple[tuple[str, str], ...] = (
    ("dsn", "dsn"),
    ("description", "description"),
    ("query_max_rows", "query_max_rows"),
    ("query_command_timeout_seconds", "query_command_timeout_seconds"),
    ("snapshot_max_rows", "snapshot_max_rows"),
    ("snapshot_command_timeout_seconds", "snapshot_command_timeout_seconds"),
)


def _user_config_path() -> Path:
    """Return path to user config (e.g. ~/.config/mcp-clickhousex/config.json)."""
    return Path.home() / ".config" / _USER_CONFIG_DIR / _USER_CONFIG_FILENAME


def _load_user_config() -> dict[str, dict[str, str]] | None:
    """Load profiles from user config file. Return None if missing/invalid."""
    path = _user_config_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict) or "profiles" not in raw:
        return None
    profiles = raw["profiles"]
    if not isinstance(profiles, dict):
        return None
    result: dict[str, dict[str, str]] = {}
    for name, fields in profiles.items():
        name_lower = name.lower() if isinstance(name, str) else ""
        if not _PROFILE_NAME_RE.fullmatch(name_lower):
            continue
        if not isinstance(fields, dict):
            continue
        row: dict[str, str] = {}
        for json_key, field_key in _JSON_PROFILE_KEYS:
            if json_key not in fields:
                continue
            val = fields[json_key]
            if val is None:
                continue
            row[field_key] = str(val)
        if row:
            result[name_lower] = row
    return result if result else None


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
            raw["query_max_rows"], _DEFAULT_QUERY_MAX_ROWS, INTERACTIVE_HARD_ROW_LIMIT
        )
    if "query_command_timeout_seconds" in raw:
        data.query_command_timeout_seconds = _clamp_int(
            raw["query_command_timeout_seconds"],
            _DEFAULT_QUERY_COMMAND_TIMEOUT_SECONDS,
            HARD_COMMAND_TIMEOUT_SECONDS,
        )
    if "snapshot_max_rows" in raw:
        data.snapshot_max_rows = _clamp_int(
            raw["snapshot_max_rows"],
            _DEFAULT_SNAPSHOT_MAX_ROWS,
            SNAPSHOT_HARD_ROW_LIMIT,
        )
    if "snapshot_command_timeout_seconds" in raw:
        data.snapshot_command_timeout_seconds = _clamp_int(
            raw["snapshot_command_timeout_seconds"],
            _DEFAULT_SNAPSHOT_COMMAND_TIMEOUT_SECONDS,
            HARD_COMMAND_TIMEOUT_SECONDS,
        )
    return data


def _clamp_int(raw: str, default: int, max_val: int) -> int:
    try:
        return min(int(raw), max_val)
    except (ValueError, TypeError):
        return default


def _resolve_profiles() -> _Registry:
    """Merge user config file, then structured env, then flat env (later wins)."""
    file_profiles = _load_user_config() or {}
    structured = _parse_structured_profiles()
    flat = _build_default_from_flat()

    merged: dict[str, dict[str, str]] = dict(file_profiles)
    for name, fields in structured.items():
        merged.setdefault(name, {}).update(fields)
    if flat:
        merged.setdefault(DEFAULT_PROFILE_NAME, {}).update(flat)

    if not merged:
        merged = {DEFAULT_PROFILE_NAME: {"dsn": _DEFAULT_DSN}}

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


def get_profiles() -> list[Profile]:
    """Return all configured profiles with name and description."""
    reg = _get_registry()
    return [
        Profile(name=name, description=data.description)
        for name, data in reg.profiles.items()
    ]


def _parse_dsn(dsn: str) -> dict[str, Any]:
    """Decompose a DSN URL into ``clickhouse_connect.get_client`` kwargs.

    Applies :func:`~urllib.parse.unquote` to the username and password so
    that percent-encoded special characters (``#`` → ``%23``, etc.) are
    decoded before being passed to the driver.
    """
    parsed = urlparse(dsn)
    kwargs: dict[str, Any] = {}
    if parsed.hostname:
        kwargs["host"] = parsed.hostname
    if parsed.port:
        kwargs["port"] = parsed.port
    if parsed.username is not None:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password is not None:
        kwargs["password"] = unquote(parsed.password)
    if parsed.path and parsed.path != "/":
        kwargs["database"] = parsed.path.lstrip("/").split("/")[0]
    if parsed.scheme in ("https", "clickhouses"):
        kwargs["secure"] = True
    for k, v in parse_qs(parsed.query).items():
        kwargs[k] = v[0]
    return kwargs


def get_client(profile: str | None = None) -> Client:
    """Build a ClickHouse client for the given profile.

    If *profile* is ``None`` or empty the default profile is used.
    """
    _, data = _lookup(profile)
    dsn = data.dsn or _DEFAULT_DSN
    return clickhouse_connect.get_client(**_parse_dsn(dsn))


def get_limits(profile: str | None = None) -> ExecutionLimits:
    """Return execution limits for the given profile."""
    _, data = _lookup(profile)
    return ExecutionLimits(
        query=QueryLimits(
            max_rows=OptionDescriptor[int](
                value=data.query_max_rows,
                description=(
                    "Row cap applied to every interactive query. "
                    "Use snapshot=true for larger result sets."
                ),
                is_overridable=False,
                scope="query",
            ),
            hard_row_limit=OptionDescriptor[int](
                value=INTERACTIVE_HARD_ROW_LIMIT,
                description=(
                    "Absolute row ceiling for interactive queries; "
                    "max_rows is clamped to this value."
                ),
                is_overridable=False,
                scope="query",
            ),
            command_timeout_seconds=OptionDescriptor[int](
                value=data.query_command_timeout_seconds,
                description=(
                    "Maximum execution time allowed for a query before it is "
                    "terminated."
                ),
                is_overridable=False,
                scope="query",
            ),
        ),
        snapshot=QueryLimits(
            max_rows=OptionDescriptor[int](
                value=data.snapshot_max_rows,
                description=(
                    "Row cap applied to snapshot queries (snapshot=true). "
                    "Result is persisted to disk; fetch via the snapshot URI."
                ),
                is_overridable=False,
                scope="snapshot",
            ),
            hard_row_limit=OptionDescriptor[int](
                value=SNAPSHOT_HARD_ROW_LIMIT,
                description=(
                    "Absolute row ceiling for snapshot queries; "
                    "snapshot_max_rows is clamped to this value."
                ),
                is_overridable=False,
                scope="snapshot",
            ),
            command_timeout_seconds=OptionDescriptor[int](
                value=data.snapshot_command_timeout_seconds,
                description=(
                    "Maximum execution time allowed for a snapshot query before it "
                    "is terminated."
                ),
                is_overridable=False,
                scope="snapshot",
            ),
        ),
    )


def get_max_rows(profile: str | None = None) -> int:
    """Return the max_rows limit for the given profile (for run_query)."""
    _, data = _lookup(profile)
    return data.query_max_rows


def get_command_timeout(profile: str | None = None) -> int:
    """Return the command timeout in seconds for the given profile."""
    _, data = _lookup(profile)
    return data.query_command_timeout_seconds


def get_snapshot_max_rows(profile: str | None = None) -> int:
    """Return the snapshot max_rows limit for the given profile."""
    _, data = _lookup(profile)
    return data.snapshot_max_rows


def get_snapshot_timeout(profile: str | None = None) -> int:
    """Return the snapshot command timeout in seconds for the given profile."""
    _, data = _lookup(profile)
    return data.snapshot_command_timeout_seconds
