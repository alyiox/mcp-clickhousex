"""MCP server: ClickHouse metadata discovery and read-only queries."""

from __future__ import annotations

import sys
from importlib.metadata import version
from typing import Annotated, Any, Literal

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from mcp_clickhousex import metadata, query
from mcp_clickhousex.cluster_properties import (
    get_cluster_properties as get_cluster_properties_impl,
)
from mcp_clickhousex.config import get_profiles
from mcp_clickhousex.models import (
    ClusterPropertiesModel,
    ExplainResultModel,
    ProfileModel,
    QueryResultModel,
    TabularResultModel,
)

mcp = FastMCP("mcp-clickhouse", json_response=True)


def main() -> None:
    """CLI entrypoint for ``uvx mcp-clickhouse``."""
    if "--version" in sys.argv or "-V" in sys.argv:
        print(version("mcp-clickhousex"))
        return
    mcp.run(transport="stdio")


@mcp.tool()
def list_profiles() -> list[ProfileModel]:
    """[ClickHouse] List configured profiles."""
    return get_profiles()


@mcp.tool()
def get_cluster_properties(
    profile: Annotated[
        str | None,
        Field(description="Optional profile name. Src: profiles."),
    ] = None,
) -> ClusterPropertiesModel:
    """[ClickHouse] Get cluster properties and execution limits."""
    return get_cluster_properties_impl(profile)


@mcp.tool()
def run_query(
    sql: Annotated[
        str,
        Field(description="Read-only SELECT statement."),
    ],
    parameters: Annotated[
        dict[str, Any] | None,
        Field(description="Optional query parameter values keyed by name."),
    ] = None,
    database: Annotated[
        str | None,
        Field(description="Optional default database. Src: databases."),
    ] = None,
    profile: Annotated[
        str | None,
        Field(description="Optional profile name. Src: profiles."),
    ] = None,
) -> QueryResultModel:
    """[ClickHouse] Execute read-only SQL."""
    return query.run_query(
        sql, parameters=parameters, database=database, profile=profile
    )


@mcp.tool()
def analyze_query(
    sql: Annotated[
        str,
        Field(description="Read-only SELECT statement."),
    ],
    parameters: Annotated[
        dict[str, Any] | None,
        Field(description="Optional query parameter values keyed by name."),
    ] = None,
    database: Annotated[
        str | None,
        Field(description="Optional default database. Src: databases."),
    ] = None,
    profile: Annotated[
        str | None,
        Field(description="Optional profile name. Src: profiles."),
    ] = None,
    types: Annotated[
        list[Literal["plan", "pipeline", "syntax"]] | None,
        Field(description="Optional EXPLAIN output types to include."),
    ] = None,
) -> ExplainResultModel:
    """[ClickHouse] Analyze query execution plan."""
    return query.analyze_query(
        sql, parameters=parameters, database=database, profile=profile, types=types
    )


@mcp.tool()
def list_databases(
    profile: Annotated[
        str | None,
        Field(description="Optional profile name. Src: profiles."),
    ] = None,
) -> TabularResultModel:
    """[ClickHouse] List databases."""
    return metadata.list_databases(profile=profile)


@mcp.tool()
def list_tables(
    database: Annotated[
        str | None,
        Field(description="Optional database name. Src: databases."),
    ] = None,
    profile: Annotated[
        str | None,
        Field(description="Optional profile name. Src: profiles."),
    ] = None,
) -> TabularResultModel:
    """[ClickHouse] List tables and views in a database.

    Returns name, engine, primary_key, sorting_key, partition_key,
    total_rows, total_bytes for query analysis.
    """
    return metadata.list_tables(database, profile=profile)


@mcp.tool()
def list_columns(
    table: Annotated[
        str,
        Field(
            description=(
                "Table name; may be qualified as `database.table`. Src: tables."
            )
        ),
    ],
    database: Annotated[
        str | None,
        Field(description="Optional database name. Src: databases."),
    ] = None,
    profile: Annotated[
        str | None,
        Field(description="Optional profile name. Src: profiles."),
    ] = None,
) -> TabularResultModel:
    """[ClickHouse] List columns for a table or view."""
    return metadata.list_columns(table, database, profile=profile)


# -- Resources (profile-first hierarchy: one static + four templates) ---------


@mcp.resource(
    "clickhouse://profiles",
    name="profiles",
    description="[ClickHouse] List configured profiles.",
    mime_type="application/json",
)
def resource_profiles() -> list[ProfileModel]:
    """[ClickHouse] List configured profiles."""
    return get_profiles()


@mcp.resource(
    "clickhouse://profiles/{profile}/cluster-properties",
    name="cluster-properties",
    description="[ClickHouse] Cluster properties and limits. Src: profiles.",
    mime_type="application/json",
)
def resource_cluster_properties_for_profile(profile: str) -> ClusterPropertiesModel:
    """[ClickHouse] Get cluster properties for profile. Src: profiles."""
    return get_cluster_properties_impl(profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases",
    name="databases",
    description="[ClickHouse] List databases for profile. Src: profiles.",
    mime_type="application/json",
)
def resource_databases_for_profile(profile: str) -> TabularResultModel:
    """[ClickHouse] List databases for profile. Src: profiles."""
    return metadata.list_databases(profile=profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases/{database}/tables",
    name="tables",
    description="[ClickHouse] List tables for profile and db. Src: profiles, dbs.",
    mime_type="application/json",
)
def resource_tables_for_profile_database(
    profile: str, database: str
) -> TabularResultModel:
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
) -> TabularResultModel:
    """[ClickHouse] List columns for table in profile+db. Src: profiles, dbs."""
    return metadata.list_columns(table, database, profile=profile)
