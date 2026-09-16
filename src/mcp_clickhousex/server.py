"""MCP server: ClickHouse metadata discovery and read-only queries."""

from __future__ import annotations

import sys
from importlib.metadata import version
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from pydantic import Field

from mcp_clickhousex import query, snapshots
from mcp_clickhousex.config import get_profiles
from mcp_clickhousex.models import (
    ExplainResult,
    Profile,
    QueryResult,
    ShowResult,
    SnapshotResult,
)

# json_response moved off the constructor in mcp 2.0; it is a
# run_streamable_http_async() option now and never applied to this stdio server.
mcp = MCPServer("mcp-clickhousex")

# Tool hints. Every tool here is read-only: get_client applies ClickHouse's
# readonly=1, which refuses writes outright, so none mutates ClickHouse state.
# destructive_hint and idempotent_hint stay unset throughout — both are
# meaningful only when read_only_hint is false.
#
# open_world_hint is false for all of them. readonly=1 also refuses the external
# table functions (url, s3, remote, mysql), so even free-form SQL reaches only
# the ClickHouse endpoints named by the configured profiles — a domain fixed
# by config, not by the SQL an agent supplies.
_READ_ONLY_CLOSED = ToolAnnotations(read_only_hint=True, open_world_hint=False)


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


@mcp.tool(annotations=_READ_ONLY_CLOSED)
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

    Indexes reports only the key columns the plan used: when it lists no
    Keys, or Granules unpruned (n/n), it is not authoritative about the
    table's keys. Read SHOW CREATE TABLE for the relation
    ReadFromMergeTree names before concluding a key is missing.
    """
    return query.analyze_query(
        sql, parameters=parameters, database=database, profile=profile, types=types
    )


# -- Resources (one static + one template) ------------------------------------


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
