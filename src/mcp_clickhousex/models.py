"""Shared Pydantic models for MCP tool inputs and outputs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_serializer


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


class OptionDescriptor[T](MCPBase):
    """Describes an effective server option and how it is enforced."""

    value: T = Field(description="Effective value enforced by the server.")
    description: str = Field(
        description="Explanation of what this option controls for users and agents."
    )
    is_overridable: bool = Field(
        description="Whether the client may request an override for this option."
    )
    scope: str = Field(description="Logical scope in which this option applies.")


class QueryLimits(MCPBase):
    """Execution limits applied to read-only queries."""

    max_rows: OptionDescriptor[int]
    hard_row_limit: OptionDescriptor[int]
    command_timeout_seconds: OptionDescriptor[int]


class ExecutionLimits(MCPBase):
    """Server-enforced execution policies."""

    query: QueryLimits


class ClusterProperties(MCPBase):
    """ClickHouse cluster metadata safe to surface to MCP clients."""

    version: str = Field(description="ClickHouse server version string.")
    limits: ExecutionLimits


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


class QueryResult(TabularResult):
    """Result of an interactive read-only SQL query."""

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
        data = handler(self)
        return {key: value for key, value in data.items() if value is not None}


class ExplainResult(MCPBase):
    """Structured EXPLAIN text output keyed by explain type."""

    plan: str | None = Field(default=None, description="EXPLAIN PLAN output.")
    pipeline: str | None = Field(default=None, description="EXPLAIN PIPELINE output.")
    syntax: str | None = Field(default=None, description="EXPLAIN SYNTAX output.")

    @model_serializer(mode="wrap")
    def _serialize_without_nulls(self, handler):  # type: ignore[no-untyped-def]
        data = handler(self)
        return {key: value for key, value in data.items() if value is not None}
