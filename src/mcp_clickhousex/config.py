"""Environment-based ClickHouse connection configuration.

Profiles are derived from configuration: the single cluster is the default
profile (flat env vars). Flat env vars override the default profile only.
No MCP_CLICKHOUSE_PROFILES; profile list is implicit.
"""

from __future__ import annotations

import os
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

DEFAULT_PROFILE_NAME = "default"

# Hard safety limits (not configurable).
HARD_ROW_LIMIT = 50_000
HARD_COMMAND_TIMEOUT_SECONDS = 300

# Flat env keys that override the default profile only.
_ENV_DSN = "MCP_CLICKHOUSE_DSN"
_ENV_HOST = "MCP_CLICKHOUSE_HOST"
_ENV_PORT = "MCP_CLICKHOUSE_PORT"
_ENV_USER = "MCP_CLICKHOUSE_USER"
_ENV_PASSWORD = "MCP_CLICKHOUSE_PASSWORD"
_ENV_DATABASE = "MCP_CLICKHOUSE_DATABASE"
_ENV_DESCRIPTION = "MCP_CLICKHOUSE_DESCRIPTION"
_ENV_QUERY_MAX_ROWS = "MCP_CLICKHOUSE_QUERY_MAX_ROWS"
_ENV_QUERY_COMMAND_TIMEOUT_SECONDS = "MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS"


def get_profiles() -> list[dict[str, Any]]:
    """Return configured profiles derived from configuration.

    Single cluster: one profile "default" with description from
    MCP_CLICKHOUSE_DESCRIPTION (flat env = default profile only).
    """
    desc = os.environ.get(_ENV_DESCRIPTION)
    description = desc.strip() if desc and desc.strip() else None
    return [{"name": DEFAULT_PROFILE_NAME, "description": description}]


def get_client(profile: str | None = None) -> Client:
    """Build a ClickHouse client for the given profile.

    If *profile* is None or empty, the default profile is used.
    Connection is read from flat env (default profile overrides only).
    """
    name = (profile or "").strip() or DEFAULT_PROFILE_NAME
    if name != DEFAULT_PROFILE_NAME:
        raise ValueError(
            f"MCP ClickHouse profile '{name}' was not found. "
            f"Available profiles: {DEFAULT_PROFILE_NAME}"
        )

    dsn = os.environ.get(_ENV_DSN)
    if dsn:
        return clickhouse_connect.get_client(dsn=dsn)

    return clickhouse_connect.get_client(
        host=os.environ.get(_ENV_HOST, "localhost"),
        port=int(os.environ.get(_ENV_PORT, "8123")),
        username=os.environ.get(_ENV_USER, "default"),
        password=os.environ.get(_ENV_PASSWORD, ""),
        database=os.environ.get(_ENV_DATABASE, "default"),
    )


def _get_int_env(key: str, default: int, max_val: int) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return min(int(raw), max_val)
    except ValueError:
        return default


def get_limits(profile: str | None = None) -> dict[str, Any]:
    """Return execution limits for the given profile (default profile only).

    Flat env MCP_CLICKHOUSE_QUERY_MAX_ROWS and
    MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS override the default profile;
    values are clamped to hard limits.
    """
    max_rows = _get_int_env(_ENV_QUERY_MAX_ROWS, 5_000, HARD_ROW_LIMIT)
    timeout = _get_int_env(
        _ENV_QUERY_COMMAND_TIMEOUT_SECONDS, 30, HARD_COMMAND_TIMEOUT_SECONDS
    )

    return {
        "query": {
            "max_rows": {
                "value": max_rows,
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
                "value": timeout,
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
    return get_limits(profile)["query"]["max_rows"]["value"]
