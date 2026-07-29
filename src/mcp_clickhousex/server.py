"""MCP server: ClickHouse metadata discovery and read-only queries."""

from __future__ import annotations

import sys
from importlib.metadata import version
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from pydantic import Field

from mcp_clickhousex import metadata, query, snapshots
from mcp_clickhousex.cluster_properties import (
    get_cluster_properties as get_cluster_properties_impl,
)
from mcp_clickhousex.config import get_profiles
from mcp_clickhousex.models import (
    ClusterProperties,
    ExplainResult,
    Profile,
    QueryResult,
    ShowResult,
    SnapshotResult,
    TabularResult,
)

# json_response moved off the constructor in mcp 2.0; it is a
# run_streamable_http_async() option now and never applied to this stdio server.
mcp = MCPServer("mcp-clickhousex")

# Tool hints. Every tool here is read-only: validation rejects DML, DDL, SET,
# SYSTEM and friends, so none mutates ClickHouse state. destructive_hint and
# idempotent_hint stay unset throughout — both are meaningful only when
# read_only_hint is false.
#
# open_world_hint splits the tools in two. Introspection and SHOW reach only the
# ClickHouse endpoints named by the configured profiles, a closed domain. The
# free-form SQL tools do not: validate_read_only screens statement keywords, not
# table functions, so url(), s3(), remote() and mysql() can pull from arbitrary
# external hosts.
_READ_ONLY_CLOSED = ToolAnnotations(read_only_hint=True, open_world_hint=False)
_READ_ONLY_OPEN = ToolAnnotations(read_only_hint=True, open_world_hint=True)


def main() -> None:
    """CLI entrypoint for ``uvx mcp-clickhousex``."""
    if "--version" in sys.argv or "-V" in sys.argv:
        print(version("mcp-clickhousex"))
        return
    mcp.run(transport="stdio")


@mcp.tool(annotations=_READ_ONLY_CLOSED)
def list_profiles() -> list[Profile]:
    """[ClickHouse] List configured profiles.

    Each entry includes name and optional description.
    """
    return get_profiles()


@mcp.tool(annotations=_READ_ONLY_CLOSED)
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


@mcp.tool(annotations=_READ_ONLY_OPEN)
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
    snapshot: Annotated[
        bool,
        Field(
            description=(
                "When true, persist the full result as a CSV file and return a "
                "resource URI (chx://snapshots/{id}) instead of inline data. "
                "Use for queries that may exceed the interactive row limit (1 000). "
                "Snapshot limits apply (default 10 000 rows, hard ceiling 50 000). "
                "Entries expire after 7 days."
            ),
        ),
    ] = False,
) -> QueryResult | SnapshotResult:
    """[ClickHouse] Execute read-only SELECT or WITH … SELECT.

    One statement; DML, DDL, SET, SYSTEM, and similar are rejected.
    Max-rows cap; overflow sets truncated and row_limit. Same SQL
    validation as analyze_query.

    Returns ``{data, row_count}`` where ``data`` is an RFC 4180 CSV string.
    Pass ``snapshot=true`` to persist the result to disk and receive a
    ``{snapshot_uri, row_count}`` instead; fetch the CSV via the snapshot
    resource URI.
    """
    return query.run_query(
        sql,
        parameters=parameters,
        database=database,
        profile=profile,
        snapshot=snapshot,
    )


@mcp.tool(annotations=_READ_ONLY_CLOSED)
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
) -> ShowResult:
    """[ClickHouse] Execute SHOW introspection statement.

    One statement per call; INTO OUTFILE rejected. Interactive row limits
    apply (default 500, hard ceiling 1 000). Same timeout as run_query.
    """
    return query.run_show(
        sql, parameters=parameters, database=database, profile=profile
    )


@mcp.tool(annotations=_READ_ONLY_OPEN)
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


@mcp.tool(annotations=_READ_ONLY_CLOSED)
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


@mcp.tool(annotations=_READ_ONLY_CLOSED)
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


@mcp.tool(annotations=_READ_ONLY_CLOSED)
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
    "chx://profiles",
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
    "chx://profiles/{profile}/cluster-properties",
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
    "chx://profiles/{profile}/databases",
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
    "chx://profiles/{profile}/databases/{database}/tables",
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
    "chx://profiles/{profile}/databases/{database}/tables/{table}/columns",
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


@mcp.resource(
    "chx://snapshots/{id}",
    name="snapshot",
    description=(
        "[ClickHouse] Fetch a query result snapshot by ID. "
        "Returns the full result as a CSV string (header row + data rows). "
        "Entries expire after 7 days. Src: run_query with snapshot=true."
    ),
    mime_type="text/csv",
)
def resource_snapshot(id: str) -> str:
    """[ClickHouse] Fetch a query result snapshot by ID.

    Returns the full result as a CSV string. Entries expire after 7 days.
    Src: run_query with snapshot=true.
    """
    csv_data = snapshots.fetch(id)
    if csv_data is None:
        raise ValueError(f"Snapshot '{id}' not found or has expired (TTL: 7 days).")
    return csv_data
