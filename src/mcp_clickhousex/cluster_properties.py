"""Cluster properties and execution limits for ClickHouse."""

from __future__ import annotations

from typing import Any

from mcp_clickhousex.config import get_client, get_limits


def get_cluster_properties(profile: str | None = None) -> dict[str, Any]:
    """Return ClickHouse cluster (node) version and execution limits for the profile.

    Connects with *profile* (default if None), runs ``SELECT version()``,
    and merges in configured execution limits.
    """
    client = get_client(profile)
    result = client.query("SELECT version()")
    version = ""
    if result.result_rows:
        version = str(result.result_rows[0][0])

    limits = get_limits(profile)

    return {
        "version": version,
        "limits": limits,
    }
