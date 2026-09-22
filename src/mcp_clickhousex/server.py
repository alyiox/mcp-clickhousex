"""MCP server: ClickHouse discovery, read-only queries and opt-in writes."""

from __future__ import annotations

import sys
from importlib.metadata import version
from typing import Annotated, Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from pydantic import Field

from mcp_clickhousex import command, query, snapshots
from mcp_clickhousex.config import any_profile_allows_write, get_profiles
from mcp_clickhousex.models import CommandResult, ExplainResult, Profile, QueryResult

mcp = MCPServer("mcp-clickhousex")

# Every read tool here is read-only: get_client applies ClickHouse's readonly=1,
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

# run_command inverts every one of those claims. It runs on a client that
# carries readonly=0, so a statement can destroy data, a repeat can change
# more, and the external table functions are back -- putting the domain of
# interaction past what the configured profiles pin down.
_WRITE_OPEN = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=True,
    idempotent_hint=False,
    open_world_hint=True,
)

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

_WRITE_PROFILE = Annotated[
    str | None,
    Field(
        description=(
            "Profile name; must be write-enabled. "
            "Src: profiles where allow_write is true."
        ),
    ),
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
        Field(description="One read-only statement: SELECT, WITH or SHOW."),
    ],
    parameters: _SQL_PARAMETERS = None,
    database: _DATABASE = None,
    profile: _PROFILE = None,
    snapshot: Annotated[
        bool,
        Field(
            description=(
                "Spill the result to a CSV resource under a larger row cap, "
                "instead of returning rows inline."
            ),
        ),
    ] = False,
) -> QueryResult:
    """[ClickHouse] Execute read-only SELECT or SHOW."""
    return query.run_query(
        sql,
        parameters=parameters,
        database=database,
        profile=profile,
        snapshot=snapshot,
    )


@mcp.tool(annotations=_READ_ONLY_CLOSED, structured_output=True)
def analyze_query(
    sql: Annotated[
        str,
        Field(
            description="Read-only SELECT to EXPLAIN. One statement.",
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


# -- Writes (advertised only where a profile opts in) --------------------------


def run_command(
    sql: Annotated[
        str,
        Field(description="One write statement: INSERT, ALTER, CREATE or DROP."),
    ],
    parameters: _SQL_PARAMETERS = None,
    database: _DATABASE = None,
    profile: _WRITE_PROFILE = None,
) -> CommandResult:
    """[ClickHouse] Execute one write statement (DDL/DML).

    Needs a write-enabled profile; refused on read-only ones, the default.
    There is no transaction to roll back in. For reads use run_query.
    """
    return command.run_command(
        sql, parameters=parameters, database=database, profile=profile
    )


_write_tool_registered = False


def _sync_write_tool() -> None:
    """Advertise run_command only while some profile opts into writes.

    Visibility is a server-wide decision, so a read-only deployment spends
    no context on the tool and offers no write surface an agent could be
    talked into; authorization stays per profile, so a call against a locked
    profile is still refused when it is made.
    """
    global _write_tool_registered  # noqa: PLW0603
    enabled = any_profile_allows_write()
    if enabled and not _write_tool_registered:
        mcp.add_tool(run_command, annotations=_WRITE_OPEN, structured_output=True)
    elif _write_tool_registered and not enabled:
        mcp.remove_tool("run_command")
    _write_tool_registered = enabled


_sync_write_tool()


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
