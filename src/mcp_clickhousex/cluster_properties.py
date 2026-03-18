"""Cluster properties and execution limits for ClickHouse."""

from __future__ import annotations

from mcp_clickhousex.config import get_client, get_limits
from mcp_clickhousex.models import ClusterPropertiesModel


def get_cluster_properties(profile: str | None = None) -> ClusterPropertiesModel:
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

    return ClusterPropertiesModel(version=version, limits=limits)
