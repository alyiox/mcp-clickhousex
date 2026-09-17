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

mcp = MCPServer("mcp-clickhousex")

# Every tool here is read-only: get_client applies ClickHouse's readonly=1,
# which refuses writes outright. destructive_hint and idempotent_hint stay
# unset -- both are meaningful only when read_only_hint is false.
#
# open_world_hint is false because readonly=1 also refuses the external table
# functions (url, s3, remote, mysql), so even free-form SQL reaches only the
# endpoints named by the configured profiles -- a domain fixed by config.
#
# structured_output is explicit so a return type the SDK cannot model raises
# InvalidSignature at import time instead of silently dropping back to text.
_READ_ONLY_CLOSED = ToolAnnotations(read_only_hint=True, open_world_hint=False)

_SQL_PARAMETERS = Annotated[
    dict[str, Any] | None,
    Field(
        description=(
            "Named parameters for driver placeholders (e.g. %(name)s or {name:Type})."
        ),
    ),
]

_DATABASE = Annotated[
    str | None,
    Field(
        description=(
            "Session default database for unqualified names. Src: system.databases."
        ),
    ),
]

_PROFILE = Annotated[
    str | None,
    Field(description="Profile name; default profile when omitted. Src: profiles."),
]


def main() -> None:
    """CLI entrypoint for ``uvx mcp-clickhousex``."""
    if "--version" in sys.argv or "-V" in sys.argv:
        print(version("mcp-clickhousex"))
        return
    mcp.run(transport="stdio")


@mcp.tool(annotations=_READ_ONLY_CLOSED, structured_output=True)
def list_profiles() -> list[Profile]:
    """[ClickHouse] List configured connection profiles."""
    return get_profiles()


@mcp.tool(annotations=_READ_ONLY_CLOSED, structured_output=True)
def run_query(
    sql: Annotated[
        str,
        Field(
            description=(
                "Read-only SELECT, CTEs allowed. One statement; qualify names "
                "as db.table or set database. Catalog metadata: "
                "system.databases, system.tables, system.columns."
            ),
        ),
    ],
    parameters: _SQL_PARAMETERS = None,
    database: _DATABASE = None,
    profile: _PROFILE = None,
    snapshot: Annotated[
        bool,
        Field(
            description=(
                "Persist the full result to a CSV resource (chx://snapshots/{id}) "
                "instead of returning rows inline. Use when the result may exceed "
                "the interactive cap of 1 000 rows; raises the cap to 10 000 "
                f"(ceiling 50 000). Expires after {snapshots.TTL_DESCRIPTION}."
            ),
        ),
    ] = False,
) -> QueryResult | SnapshotResult:
    """[ClickHouse] Execute read-only SELECT."""
    return query.run_query(
        sql,
        parameters=parameters,
        database=database,
        profile=profile,
        snapshot=snapshot,
    )


@mcp.tool(annotations=_READ_ONLY_CLOSED, structured_output=True)
def run_show(
    sql: Annotated[
        str,
        Field(
            description=(
                "One SHOW statement (e.g. SHOW TABLES FROM db LIKE '%x%', "
                "SHOW CREATE TABLE). Filter with LIKE/ILIKE to stay under the "
                "row cap. No INTO OUTFILE."
            ),
        ),
    ],
    parameters: _SQL_PARAMETERS = None,
    database: _DATABASE = None,
    profile: _PROFILE = None,
) -> ShowResult:
    """[ClickHouse] Execute SHOW introspection statement."""
    return query.run_show(
        sql, parameters=parameters, database=database, profile=profile
    )


@mcp.tool(annotations=_READ_ONLY_CLOSED, structured_output=True)
def analyze_query(
    sql: Annotated[
        str,
        Field(
            description="Read-only SELECT, CTEs allowed, to EXPLAIN. One statement.",
        ),
    ],
    parameters: _SQL_PARAMETERS = None,
    database: _DATABASE = None,
    profile: _PROFILE = None,
    types: Annotated[
        list[Literal["plan", "pipeline", "syntax"]] | None,
        Field(
            description=(
                "EXPLAIN variants: plan (indexes), pipeline, syntax. "
                "Default plan and pipeline."
            ),
        ),
    ] = None,
) -> ExplainResult:
    """[ClickHouse] Explain read-only SELECT.

    Indexes names only the keys the plan used; confirm absent keys from
    system.tables (primary_key, sorting_key, partition_key).
    """
    return query.analyze_query(
        sql, parameters=parameters, database=database, profile=profile, types=types
    )


# -- Resources (one static + one template) ------------------------------------


@mcp.resource(
    "chx://profiles",
    name="profiles",
    description="[ClickHouse] List configured connection profiles.",
    mime_type="application/json",
)
def resource_profiles() -> list[Profile]:
    return get_profiles()


# TTL is interpolated from the one constant, so moving it cannot leave a
# description advertising the old retention.
@mcp.resource(
    "chx://snapshots/{id}",
    name="snapshot",
    description=(
        "[ClickHouse] Fetch a query result snapshot as CSV. "
        f"Expires after {snapshots.TTL_DESCRIPTION}. "
        "Src: run_query with snapshot=true."
    ),
    mime_type="text/csv",
)
def resource_snapshot(id: str) -> str:
    csv_data = snapshots.fetch(id)
    if csv_data is None:
        raise ValueError(
            f"Snapshot '{id}' not found or has expired "
            f"(TTL: {snapshots.TTL_DESCRIPTION})."
        )
    return csv_data
