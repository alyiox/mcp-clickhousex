"""Query execution and analysis logic."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mcp_clickhousex import snapshots
from mcp_clickhousex.config import (
    get_client,
    get_command_timeout,
    get_max_rows,
    get_snapshot_max_rows,
    get_snapshot_timeout,
)
from mcp_clickhousex.models import ExplainResult, QueryResult
from mcp_clickhousex.snapshots import to_csv
from mcp_clickhousex.validation import validate_explain_target, validate_read_only

_ALLOWED_EXPLAIN_TYPES = frozenset({"plan", "pipeline", "syntax"})
_DEFAULT_EXPLAIN_TYPES: list[str] = ["plan", "pipeline"]

_EXPLAIN_PREFIX: dict[str, str] = {
    "plan": "EXPLAIN PLAN indexes=1",
    "pipeline": "EXPLAIN PIPELINE",
    "syntax": "EXPLAIN SYNTAX",
}


def _settings(timeout: int, database: str | None) -> dict[str, Any]:
    settings: dict[str, Any] = {"max_execution_time": timeout}
    if database:
        settings["database"] = database
    return settings


def _fetch_rows(
    sql: str,
    parameters: dict[str, Any] | None,
    database: str | None,
    profile: str | None,
    max_rows: int,
    timeout: int,
) -> tuple[list[str], list[list[Any]], bool]:
    """Run *sql* under a row cap; return ``(columns, rows, truncated)``."""
    settings = _settings(timeout, database)
    # +1 so we can detect truncation: max_rows+1 returned means more rows existed.
    settings["max_result_rows"] = max_rows + 1
    settings["result_overflow_mode"] = "break"

    result = get_client(profile).query(
        sql, parameters=parameters or {}, settings=settings
    )

    columns = list(result.column_names)
    rows = [list(row) for row in result.result_rows]

    truncated = len(rows) > max_rows
    return columns, rows[:max_rows] if truncated else rows, truncated


def run_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
    snapshot: bool = False,
) -> QueryResult:
    """Execute a read-only SELECT or SHOW statement and return the result.

    When *snapshot* is ``False`` (default), returns ``{data, row_count}``
    where ``data`` is an RFC 4180 CSV string (header + rows).  When it is
    ``True``, persists the full result as a CSV file and returns
    ``{snapshot_uri, row_count}`` instead, under the larger snapshot caps.
    """
    validate_read_only(sql)

    if snapshot:
        max_rows = get_snapshot_max_rows(profile)
        timeout = get_snapshot_timeout(profile)
    else:
        max_rows = get_max_rows(profile)
        timeout = get_command_timeout(profile)

    columns, rows, truncated = _fetch_rows(
        sql, parameters, database, profile, max_rows, timeout
    )
    overflow: dict[str, Any] = {
        "truncated": True if truncated else None,
        "row_limit": max_rows if truncated else None,
    }

    if snapshot:
        snapshot_id = snapshots.save(columns, rows)
        return QueryResult(
            snapshot_uri=f"chx://snapshots/{snapshot_id}",
            row_count=len(rows),
            **overflow,
        )

    return QueryResult(data=to_csv(columns, rows), row_count=len(rows), **overflow)


def analyze_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
    types: Sequence[str] | None = None,
) -> ExplainResult:
    """Run EXPLAIN variants on a read-only SELECT and return text results."""
    validate_explain_target(sql)

    if not types:
        types = _DEFAULT_EXPLAIN_TYPES

    unknown = set(types) - _ALLOWED_EXPLAIN_TYPES
    if unknown:
        raise ValueError(
            f"Unknown EXPLAIN types: {sorted(unknown)}. "
            f"Allowed: {sorted(_ALLOWED_EXPLAIN_TYPES)}"
        )

    client = get_client(profile)
    settings = _settings(get_command_timeout(profile), database)

    out: dict[str, str] = {}
    for t in types:
        result = client.query(
            f"{_EXPLAIN_PREFIX[t]} {sql}",
            parameters=parameters or {},
            settings=settings,
        )
        out[t] = "\n".join(str(row[0]) for row in result.result_rows)

    return ExplainResult(**out)
