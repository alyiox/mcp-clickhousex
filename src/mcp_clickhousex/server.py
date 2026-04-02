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
    ClusterProperties,
    ExplainResult,
    Profile,
    QueryResult,
    TabularResult,
)

mcp = FastMCP("mcp-clickhousex", json_response=True)


def main() -> None:
    """CLI entrypoint for ``uvx mcp-clickhousex``."""
    if "--version" in sys.argv or "-V" in sys.argv:
        print(version("mcp-clickhousex"))
        return
    mcp.run(transport="stdio")


@mcp.tool()
def list_profiles() -> list[Profile]:
    """[ClickHouse] List configured profiles.

    Each entry includes name and optional description.
    """
    return get_profiles()


@mcp.tool()
def get_cluster_properties(
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
) -> ClusterProperties:
    """[ClickHouse] Get cluster properties and execution limits.

    Returns ClickHouse server version plus enforced limits (max rows,
    timeouts) for the profile.
    """
    return get_cluster_properties_impl(profile)


@mcp.tool()
def run_query(
    sql: Annotated[
        str,
        Field(
            description=(
                "Read-only SELECT or WITH … SELECT. One statement; use qualified "
                "db.table or database. Driver placeholder syntax for parameters."
            ),
        ),
    ],
    parameters: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Named parameters for driver placeholders "
                "(e.g. %(name)s or {name:Type})."
            ),
        ),
    ] = None,
    database: Annotated[
        str | None,
        Field(
            description=(
                "Session default database for unqualified names. Src: databases."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
) -> QueryResult:
    """[ClickHouse] Execute read-only SELECT or WITH … SELECT.

    One statement; DML, DDL, SET, SYSTEM, and similar are rejected.
    Max-rows cap; overflow sets truncated and row_limit. Same SQL
    validation as analyze_query.
    """
    return query.run_query(
        sql, parameters=parameters, database=database, profile=profile
    )


@mcp.tool()
def run_show(
    sql: Annotated[
        str,
        Field(
            description=(
                "Single SHOW statement (e.g. SHOW DATABASES, SHOW CREATE TABLE). "
                "No INTO OUTFILE."
            ),
        ),
    ],
    parameters: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Named parameters for driver placeholders "
                "(e.g. %(name)s or {name:Type})."
            ),
        ),
    ] = None,
    database: Annotated[
        str | None,
        Field(
            description=(
                "Session default database for unqualified names. Src: databases."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
) -> QueryResult:
    """[ClickHouse] Execute SHOW introspection statement.

    One statement per call; INTO OUTFILE rejected. Same max-rows cap and
    timeout behavior as run_query.
    """
    return query.run_show(
        sql, parameters=parameters, database=database, profile=profile
    )


@mcp.tool()
def analyze_query(
    sql: Annotated[
        str,
        Field(
            description=(
                "Read-only SELECT or WITH … SELECT for EXPLAIN. One statement; "
                "same validation as run_query."
            ),
        ),
    ],
    parameters: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                "Named parameters for driver placeholders "
                "(e.g. %(name)s or {name:Type})."
            ),
        ),
    ] = None,
    database: Annotated[
        str | None,
        Field(
            description=(
                "Session default database for unqualified names. Src: databases."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
    types: Annotated[
        list[Literal["plan", "pipeline", "syntax"]] | None,
        Field(
            description=(
                "EXPLAIN variants: plan (indexes), pipeline, syntax. "
                "Default plan and pipeline if omitted."
            ),
        ),
    ] = None,
) -> ExplainResult:
    """[ClickHouse] Explain read-only SELECT or WITH … SELECT.

    Returns plan, pipeline, and/or syntax text. Default types plan and
    pipeline. Uses query timeout and optional database; no max-rows cap
    unlike run_query.
    """
    return query.analyze_query(
        sql, parameters=parameters, database=database, profile=profile, types=types
    )


@mcp.tool()
def list_databases(
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
) -> TabularResult:
    """[ClickHouse] List databases.

    Rows from system.databases visible to the connection.
    """
    return metadata.list_databases(profile=profile)


@mcp.tool()
def list_tables(
    database: Annotated[
        str | None,
        Field(
            description=(
                "Database to list; client default when omitted. Src: databases."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
) -> TabularResult:
    """[ClickHouse] List tables and views in a database.

    Rows from system.tables: name, engine, primary_key, sorting_key,
    partition_key, total_rows, total_bytes for query planning.
    """
    return metadata.list_tables(database, profile=profile)


@mcp.tool()
def list_columns(
    table: Annotated[
        str,
        Field(
            description=("Table or view name, or database.table. Src: tables."),
        ),
    ],
    database: Annotated[
        str | None,
        Field(
            description=(
                "Database when table is unqualified; ignored if table "
                "contains a dot. Client default when omitted. Src: databases."
            ),
        ),
    ] = None,
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Profile name; uses default profile when omitted. Src: profiles."
            ),
        ),
    ] = None,
) -> TabularResult:
    """[ClickHouse] List columns for a table or view.

    Rows from system.columns for the resolved database and table.
    """
    return metadata.list_columns(table, database, profile=profile)


# -- Resources (profile-first hierarchy: one static + four templates) ---------


@mcp.resource(
    "clickhouse://profiles",
    name="profiles",
    description=(
        "[ClickHouse] List configured profiles. "
        "Each entry includes name and optional description."
    ),
    mime_type="application/json",
)
def resource_profiles() -> list[Profile]:
    """[ClickHouse] List configured profiles.

    Each entry includes name and optional description.
    """
    return get_profiles()


@mcp.resource(
    "clickhouse://profiles/{profile}/cluster-properties",
    name="cluster-properties",
    description=(
        "[ClickHouse] Get cluster properties and execution limits. "
        "Returns ClickHouse server version plus enforced limits (max rows, "
        "timeouts) for the profile. Src: profiles."
    ),
    mime_type="application/json",
)
def resource_cluster_properties_for_profile(profile: str) -> ClusterProperties:
    """[ClickHouse] Get cluster properties and execution limits.

    Returns ClickHouse server version plus enforced limits (max rows,
    timeouts) for the profile. Src: profiles.
    """
    return get_cluster_properties_impl(profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases",
    name="databases",
    description=(
        "[ClickHouse] List databases. "
        "Rows from system.databases visible to the connection. Src: profiles."
    ),
    mime_type="application/json",
)
def resource_databases_for_profile(profile: str) -> TabularResult:
    """[ClickHouse] List databases.

    Rows from system.databases visible to the connection. Src: profiles.
    """
    return metadata.list_databases(profile=profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases/{database}/tables",
    name="tables",
    description=(
        "[ClickHouse] List tables and views in a database. "
        "Rows from system.tables: name, engine, primary_key, sorting_key, "
        "partition_key, total_rows, total_bytes for query planning. "
        "Src: profiles, dbs."
    ),
    mime_type="application/json",
)
def resource_tables_for_profile_database(profile: str, database: str) -> TabularResult:
    """[ClickHouse] List tables and views in a database.

    Rows from system.tables: name, engine, primary_key, sorting_key,
    partition_key, total_rows, total_bytes for query planning.
    Src: profiles, dbs.
    """
    return metadata.list_tables(database, profile=profile)


@mcp.resource(
    "clickhouse://profiles/{profile}/databases/{database}/tables/{table}/columns",
    name="table-columns",
    description=(
        "[ClickHouse] List columns for a table or view. "
        "Rows from system.columns for the resolved database and table. "
        "Src: profiles, dbs, tables."
    ),
    mime_type="application/json",
)
def resource_columns_for_profile_database_table(
    profile: str, database: str, table: str
) -> TabularResult:
    """[ClickHouse] List columns for a table or view.

    Rows from system.columns for the resolved database and table.
    Src: profiles, dbs, tables.
    """
    return metadata.list_columns(table, database, profile=profile)
