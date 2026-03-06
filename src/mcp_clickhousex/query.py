"""Run-query execution logic."""

from __future__ import annotations

from typing import Any

from mcp_clickhousex.config import get_client
from mcp_clickhousex.validation import validate_read_only


def run_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a read-only SELECT and return ``{columns, rows}``."""
    validate_read_only(sql)

    client = get_client()
    result = client.query(sql, parameters=parameters)

    columns = list(result.column_names)
    rows = [list(row) for row in result.result_rows]

    return {"columns": columns, "rows": rows}
