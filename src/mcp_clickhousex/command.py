"""Write statement execution for the opt-in ``run_command`` tool."""

from __future__ import annotations

from typing import Any

from clickhouse_connect.driver.summary import QuerySummary

from mcp_clickhousex.config import get_write_client, get_write_timeout
from mcp_clickhousex.models import CommandResult
from mcp_clickhousex.validation import validate_write


def run_command(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
) -> CommandResult:
    """Execute one write statement against a write-enabled profile.

    Raises ``PermissionError`` when *profile* is read-only, which is the
    default; the caller sees the same rejection whether or not the tool is
    advertised.
    """
    validate_write(sql)

    settings: dict[str, Any] = {"max_execution_time": get_write_timeout(profile)}
    if database:
        settings["database"] = database

    result = get_write_client(profile).command(
        sql, parameters=parameters or {}, settings=settings
    )

    # A statement that produces no result set -- DDL, INSERT, ALTER -- answers
    # with the query summary. One that does produce rows answers with the
    # value instead and wrote nothing, so an empty summary reads correctly.
    summary = result.summary if isinstance(result, QuerySummary) else {}
    return CommandResult(
        written_rows=int(summary.get("written_rows", 0)),
        written_bytes=int(summary.get("written_bytes", 0)),
        query_id=summary.get("query_id") or None,
    )
