"""Run-query execution logic."""

from __future__ import annotations

from typing import Any

from mcp_clickhousex.config import get_client, get_command_timeout, get_max_rows
from mcp_clickhousex.validation import validate_read_only


def run_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """Execute a read-only SELECT and return ``{columns, rows}``.

    Applies the profile's max_rows limit via ClickHouse max_result_rows;
    if the result exceeds the limit, rows are truncated and ``truncated``
    is set to true.
    """
    validate_read_only(sql)

    client = get_client(profile)
    if parameters is None:
        parameters = {}

    max_rows = get_max_rows(profile)
    timeout = get_command_timeout(profile)
    settings: dict[str, Any] = {
        "max_result_rows": max_rows + 1,
        "result_overflow_mode": "break",
        "max_execution_time": timeout,
    }
    if database:
        settings["database"] = database

    result = client.query(sql, parameters=parameters, settings=settings)

    columns = list(result.column_names)
    rows = [list(row) for row in result.result_rows]

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    out: dict[str, Any] = {
        "columns": columns,
        "rows": rows,
    }
    if truncated:
        out["truncated"] = True
        out["row_limit"] = max_rows

    return out
