"""MCP server: ClickHouse metadata discovery and read-only queries."""

from __future__ import annotations

import sys
from importlib.metadata import version
from typing import Any

from mcp.server.fastmcp import FastMCP

from mcp_clickhousex import metadata, query
from mcp_clickhousex.cluster_properties import (
    get_cluster_properties as get_cluster_properties_impl,
)
from mcp_clickhousex.config import get_profiles

mcp = FastMCP("mcp-clickhouse", json_response=True)


def main() -> None:
    """CLI entrypoint for ``uvx mcp-clickhouse``."""
    if "--version" in sys.argv or "-V" in sys.argv:
        print(version("mcp-clickhousex"))
        return
    mcp.run(transport="stdio")


@mcp.tool()
def list_profiles() -> list[dict[str, Any]]:
    """[ClickHouse] List configured profiles."""
    return get_profiles()


@mcp.tool()
def get_cluster_properties(profile: str | None = None) -> dict[str, Any]:
    """[ClickHouse] Get cluster properties and execution limits.

    profile: Optional. Profile name. Src: profiles.
    """
    return get_cluster_properties_impl(profile)


@mcp.tool()
def run_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """[ClickHouse] Execute read-only SQL.

    sql: Read-only SELECT statement.
    parameters: Optional. Query parameter values keyed by name.
    database: Optional. Default database for the query. Src: databases.
    profile: Optional. Profile name. Src: profiles.
    """
    return query.run_query(
        sql, parameters=parameters, database=database, profile=profile
    )


@mcp.tool()
def analyze_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
    types: list[str] | None = None,
) -> dict[str, str]:
    """[ClickHouse] Analyze query execution plan.

    sql: Read-only SELECT statement.
    parameters: Optional. Query parameter values keyed by name.
    database: Optional. Default database for the query. Src: databases.
    profile: Optional. Profile name. Src: profiles.
    types: Optional. EXPLAIN types (plan, pipeline, syntax).
    """
    return query.analyze_query(
        sql, parameters=parameters, database=database, profile=profile, types=types
    )


@mcp.tool()
def list_databases(profile: str | None = None) -> dict[str, Any]:
    """[ClickHouse] List databases.

    profile: Optional. Profile name. Src: profiles.
    """
    return metadata.list_databases(profile=profile)


@mcp.tool()
def list_tables(
    database: str | None = None, profile: str | None = None
) -> dict[str, Any]:
    """[ClickHouse] List tables and views in a database.

    Returns name, engine, primary_key, sorting_key, partition_key,
    total_rows, total_bytes for query analysis.

    database: Optional. Database name. Src: databases.
    profile: Optional. Profile name. Src: profiles.
    """
    return metadata.list_tables(database, profile=profile)


@mcp.tool()
def list_columns(
    table: str, database: str | None = None, profile: str | None = None
) -> dict[str, Any]:
    """[ClickHouse] List columns for a table or view.

    table: Table name; may be qualified as ``database.table``. Src: tables.
    database: Optional. Database name. Src: databases.
    profile: Optional. Profile name. Src: profiles.
    """
    return metadata.list_columns(table, database, profile=profile)


# -- Resources (profile-first hierarchy: one static + four templates) ---------


@mcp.resource(
    "clickhouse://profiles",
    name="profiles",
    description="[ClickHouse] List configured profiles.",
    mime_type="application/json",
)
def resource_profiles() -> list[dict[str, Any]]:
    """[ClickHouse] List configured profiles."""
    return get_profiles()


@mcp.resource(
    "clickhouse://profiles/{profile}/cluster-properties",
    name="cluster-properties",
    description="[ClickHouse] Cluster properties and limits. Src: profiles.",
    mime_type="application/json",
)
def resource_cluster_properties_for_profile(profile: str) -> dict[str, Any]:
    """[ClickHouse] Get cluster properties for profile. Src: profiles."""
    return get_cluster_properties_impl(profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases",
    name="databases",
    description="[ClickHouse] List databases for profile. Src: profiles.",
    mime_type="application/json",
)
def resource_databases_for_profile(profile: str) -> dict[str, Any]:
    """[ClickHouse] List databases for profile. Src: profiles."""
    return metadata.list_databases(profile=profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases/{database}/tables",
    name="tables",
    description="[ClickHouse] List tables for profile and db. Src: profiles, dbs.",
    mime_type="application/json",
)
def resource_tables_for_profile_database(profile: str, database: str) -> dict[str, Any]:
    """[ClickHouse] List tables for profile and database. Src: profiles, dbs."""
    return metadata.list_tables(database, profile=profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases/{database}/tables/{table}/columns",
    name="table-columns",
    description="[ClickHouse] List columns (profile+db). Src: profiles, dbs, tables.",
    mime_type="application/json",
)
def resource_columns_for_profile_database_table(
    profile: str, database: str, table: str
) -> dict[str, Any]:
    """[ClickHouse] List columns for table in profile+db. Src: profiles, dbs."""
    return metadata.list_columns(table, database, profile=profile)
