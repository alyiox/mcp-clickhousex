"""MCP server: ClickHouse metadata discovery and read-only queries."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from mcp_clickhouse import metadata, query

mcp = FastMCP("mcp-clickhouse", json_response=True)


def main() -> None:
    """CLI entrypoint for ``uvx mcp-clickhouse``."""
    mcp.run(transport="stdio")


@mcp.tool()
def run_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a read-only SELECT against ClickHouse and return columns and rows.

    Database and table must be specified in the SQL text
    (e.g. ``SELECT * FROM mydb.mytable``).
    """
    return query.run_query(sql, parameters=parameters)


@mcp.tool()
def list_databases() -> dict[str, Any]:
    """List all ClickHouse databases (from system.databases)."""
    return metadata.list_databases()


@mcp.tool()
def list_tables(database: str | None = None) -> dict[str, Any]:
    """List tables and views in a database (from system.tables).

    If *database* is omitted the connection's default database is used.
    """
    return metadata.list_tables(database)


@mcp.tool()
def list_columns(table: str, database: str | None = None) -> dict[str, Any]:
    """List columns for a table or view (from system.columns).

    *table* may be qualified as ``database.table``.
    """
    return metadata.list_columns(table, database)
