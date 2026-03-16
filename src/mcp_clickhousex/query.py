"""Query execution and analysis logic."""

from __future__ import annotations

from typing import Any

from mcp_clickhousex.config import get_client, get_command_timeout, get_max_rows
from mcp_clickhousex.validation import validate_read_only

_ALLOWED_EXPLAIN_TYPES = frozenset({"plan", "pipeline", "syntax"})
_DEFAULT_EXPLAIN_TYPES: list[str] = ["plan", "pipeline"]

_EXPLAIN_PREFIX: dict[str, str] = {
    "plan": "EXPLAIN PLAN indexes=1",
    "pipeline": "EXPLAIN PIPELINE",
    "syntax": "EXPLAIN SYNTAX",
}


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


def analyze_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
    types: list[str] | None = None,
) -> dict[str, str]:
    """Run EXPLAIN variants on a read-only SELECT and return text results."""
    validate_read_only(sql)

    if types is None or len(types) == 0:
        types = _DEFAULT_EXPLAIN_TYPES

    unknown = set(types) - _ALLOWED_EXPLAIN_TYPES
    if unknown:
        raise ValueError(
            f"Unknown EXPLAIN types: {sorted(unknown)}. "
            f"Allowed: {sorted(_ALLOWED_EXPLAIN_TYPES)}"
        )

    client = get_client(profile)
    if parameters is None:
        parameters = {}

    timeout = get_command_timeout(profile)
    settings: dict[str, Any] = {"max_execution_time": timeout}
    if database:
        settings["database"] = database

    out: dict[str, str] = {}
    for t in types:
        explain_sql = f"{_EXPLAIN_PREFIX[t]} {sql}"
        result = client.query(explain_sql, parameters=parameters, settings=settings)
        out[t] = "\n".join(str(row[0]) for row in result.result_rows)

    return out
