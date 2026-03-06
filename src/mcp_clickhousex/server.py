"""MCP server: ClickHouse metadata discovery and read-only queries."""

from __future__ import annotations

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
    profile: str | None = None,
) -> dict[str, Any]:
    """[ClickHouse] Execute read-only SQL and return columns and rows.

    Database and table must be specified in the SQL text
    (e.g. ``SELECT * FROM mydb.mytable``).

    sql: Read-only SELECT statement.
    parameters: Optional. Query parameter values keyed by name.
    profile: Optional. Profile name. Src: profiles.
    """
    return query.run_query(sql, parameters=parameters, profile=profile)


@mcp.tool()
def list_databases(profile: str | None = None) -> dict[str, Any]:
    """[ClickHouse] List databases (from system.databases).

    profile: Optional. Profile name. Src: profiles.
    """
    return metadata.list_databases(profile=profile)


@mcp.tool()
def list_tables(
    database: str | None = None, profile: str | None = None
) -> dict[str, Any]:
    """[ClickHouse] List tables and views in a database (from system.tables).

    database: Optional. Database name. Src: databases.
    profile: Optional. Profile name. Src: profiles.
    """
    return metadata.list_tables(database, profile=profile)


@mcp.tool()
def list_columns(
    table: str, database: str | None = None, profile: str | None = None
) -> dict[str, Any]:
    """[ClickHouse] List columns for a table or view (from system.columns).

    table: Table name; may be qualified as ``database.table``. Src: tables.
    database: Optional. Database name. Src: databases.
    profile: Optional. Profile name. Src: profiles.
    """
    return metadata.list_columns(table, database, profile=profile)
