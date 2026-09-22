"""Shared Pydantic models for MCP tool inputs and outputs."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_serializer

from mcp_clickhousex.snapshots import TTL_DESCRIPTION


class MCPBase(BaseModel):
    """Base model for server-facing MCP payloads.

    Unset optional fields are dropped on serialization so a result carries
    only the keys that mean something to the caller.
    """

    model_config = ConfigDict(extra="forbid")

    @model_serializer(mode="wrap")
    def _serialize_without_nulls(self, handler):  # type: ignore[no-untyped-def]
        return {key: value for key, value in handler(self).items() if value is not None}


class Overflow(MCPBase):
    """Row-cap overflow signal shared by the capped result models."""

    truncated: bool | None = Field(
        default=None,
        description=(
            "Whether the result set was truncated due to the enforced row limit."
        ),
    )
    row_limit: int | None = Field(
        default=None,
        description="The enforced maximum number of rows returned for this query.",
    )


class Profile(MCPBase):
    """Summary information for one configured ClickHouse profile."""

    name: str = Field(description="Profile name, for example 'default' or 'warehouse'.")
    description: str | None = Field(
        default=None,
        description="Human- or agent-facing description of the profile.",
    )
    allow_write: bool = Field(
        default=False,
        description="Whether run_command is permitted on this profile.",
    )


class QueryResult(Overflow):
    """Result of a read-only SQL query, inline as CSV or spilled to a resource.

    Exactly one of *data* and *snapshot_uri* is set, decided by the call's
    ``snapshot`` flag; the other is dropped on serialization.
    """

    data: str | None = Field(
        default=None,
        description=(
            "RFC 4180 CSV string: first row is the header, remaining rows are "
            "data. Absent when the result was spilled to snapshot_uri."
        ),
    )
    snapshot_uri: str | None = Field(
        default=None,
        description=(
            "MCP resource URI for the snapshot CSV "
            "(e.g. ``chx://snapshots/{id}``). "
            f"Fetch it via the snapshot resource. Entries expire after "
            f"{TTL_DESCRIPTION}. Absent when rows were returned inline."
        ),
    )
    row_count: int = Field(description="Number of data rows in the result.")


class CommandResult(MCPBase):
    """Outcome of one write statement executed via ``run_command``.

    ClickHouse answers a write with a summary rather than a row count, so
    what the statement moved is reported as written rows and bytes; DDL
    moves neither and reports zero.
    """

    written_rows: int = Field(description="Rows written by the statement; 0 for DDL.")
    written_bytes: int = Field(
        description="Uncompressed bytes written by the statement; 0 for DDL."
    )
    query_id: str | None = Field(
        default=None,
        description="Server query id; join to system.query_log for detail.",
    )


class ExplainResult(MCPBase):
    """Structured EXPLAIN text output keyed by explain type."""

    plan: str | None = Field(default=None, description="EXPLAIN PLAN output.")
    pipeline: str | None = Field(default=None, description="EXPLAIN PIPELINE output.")
    syntax: str | None = Field(default=None, description="EXPLAIN SYNTAX output.")
