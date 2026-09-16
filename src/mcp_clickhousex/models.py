"""Shared Pydantic models for MCP tool inputs and outputs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_serializer

from mcp_clickhousex.snapshots import TTL_DESCRIPTION


class MCPBase(BaseModel):
    """Base model for server-facing MCP payloads."""

    model_config = ConfigDict(extra="forbid")


class Profile(MCPBase):
    """Summary information for one configured ClickHouse profile."""

    name: str = Field(description="Profile name, for example 'default' or 'warehouse'.")
    description: str | None = Field(
        default=None,
        description="Human- or agent-facing description of the profile.",
    )


class TabularResult(MCPBase):
    """Generic tabular result with ordered columns and aligned row values."""

    columns: list[str] = Field(
        description=(
            "Ordered list of column names. Each row aligns with these names by index."
        )
    )
    rows: list[list[Any]] = Field(
        description="Row values aligned with the columns list."
    )


class QueryResult(MCPBase):
    """Result of an interactive read-only SQL query (CSV format)."""

    data: str = Field(
        description=(
            "RFC 4180 CSV string: first row is the header, remaining rows are data."
        )
    )
    row_count: int = Field(description="Number of data rows in the result.")
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

    @model_serializer(mode="wrap")
    def _serialize_without_nulls(self, handler):  # type: ignore[no-untyped-def]
        d = handler(self)
        return {key: value for key, value in d.items() if value is not None}


class SnapshotResult(MCPBase):
    """Result of a snapshot query: CSV persisted to disk, accessible via URI."""

    snapshot_uri: str = Field(
        description=(
            "MCP resource URI for the snapshot CSV "
            "(e.g. ``chx://snapshots/{id}``). "
            f"Fetch it via the snapshot resource. Entries expire after "
            f"{TTL_DESCRIPTION}."
        )
    )
    row_count: int = Field(description="Number of data rows in the snapshot.")
    truncated: bool | None = Field(
        default=None,
        description=(
            "Whether the result set was truncated due to the enforced snapshot row "
            "limit."
        ),
    )
    row_limit: int | None = Field(
        default=None,
        description="The enforced maximum number of rows for the snapshot query.",
    )

    @model_serializer(mode="wrap")
    def _serialize_without_nulls(self, handler):  # type: ignore[no-untyped-def]
        d = handler(self)
        return {key: value for key, value in d.items() if value is not None}


class ShowResult(TabularResult):
    """Result of a SHOW introspection statement (columns + rows + optional truncation)."""  # noqa: E501

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

    @model_serializer(mode="wrap")
    def _serialize_without_nulls(self, handler):  # type: ignore[no-untyped-def]
        d = handler(self)
        return {key: value for key, value in d.items() if value is not None}


class ExplainResult(MCPBase):
    """Structured EXPLAIN text output keyed by explain type."""

    plan: str | None = Field(default=None, description="EXPLAIN PLAN output.")
    pipeline: str | None = Field(default=None, description="EXPLAIN PIPELINE output.")
    syntax: str | None = Field(default=None, description="EXPLAIN SYNTAX output.")

    @model_serializer(mode="wrap")
    def _serialize_without_nulls(self, handler):  # type: ignore[no-untyped-def]
        data = handler(self)
        return {key: value for key, value in data.items() if value is not None}
