"""Query execution and analysis logic."""

from __future__ import annotations

import csv
import io
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
from mcp_clickhousex.models import (
    ExplainResult,
    QueryResult,
    ShowResult,
    SnapshotResult,
)
from mcp_clickhousex.validation import validate_read_only, validate_show_statement

_ALLOWED_EXPLAIN_TYPES = frozenset({"plan", "pipeline", "syntax"})
_DEFAULT_EXPLAIN_TYPES: list[str] = ["plan", "pipeline"]

_EXPLAIN_PREFIX: dict[str, str] = {
    "plan": "EXPLAIN PLAN indexes=1",
    "pipeline": "EXPLAIN PIPELINE",
    "syntax": "EXPLAIN SYNTAX",
}


def _to_csv(columns: list[str], rows: list[list[Any]]) -> str:
    """Serialize *columns* and *rows* to an RFC 4180 CSV string."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)
    return buf.getvalue()


def run_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
    snapshot: bool = False,
) -> QueryResult | SnapshotResult:
    """Execute a read-only SELECT and return the result.

    When *snapshot* is ``False`` (default), returns ``{data, row_count}``
    where ``data`` is an RFC 4180 CSV string (header + rows).  Interactive
    row limits apply (default 500, hard ceiling 1 000).

    When *snapshot* is ``True``, persists the full result as a CSV file and
    returns ``{snapshot_uri, row_count}``.  Snapshot limits apply (default
    10 000, hard ceiling 50 000).  Entries expire after 7 days.
    """
    validate_read_only(sql)

    client = get_client(profile)
    if parameters is None:
        parameters = {}

    if snapshot:
        max_rows = get_snapshot_max_rows(profile)
        timeout = get_snapshot_timeout(profile)
    else:
        max_rows = get_max_rows(profile)
        timeout = get_command_timeout(profile)

    settings: dict[str, Any] = {
        # +1 so we can detect truncation: max_rows+1 returned means more rows existed.
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

    if snapshot:
        snapshot_id = snapshots.save(columns, rows)
        return SnapshotResult(
            snapshot_uri=f"chx://snapshots/{snapshot_id}",
            row_count=len(rows),
            truncated=True if truncated else None,
            row_limit=max_rows if truncated else None,
        )

    return QueryResult(
        data=_to_csv(columns, rows),
        row_count=len(rows),
        truncated=True if truncated else None,
        row_limit=max_rows if truncated else None,
    )


def run_show(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
) -> ShowResult:
    """Execute a SHOW statement and return ``{columns, rows}``.

    Applies the profile's interactive max_rows limit via ClickHouse
    max_result_rows; if the result exceeds the limit, rows are truncated
    and ``truncated`` is set to true.
    """
    validate_show_statement(sql)

    client = get_client(profile)
    if parameters is None:
        parameters = {}

    max_rows = get_max_rows(profile)
    timeout = get_command_timeout(profile)
    settings: dict[str, Any] = {
        # +1 so we can detect truncation: max_rows+1 returned means more rows existed.
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

    return ShowResult(
        columns=columns,
        rows=rows,
        truncated=True if truncated else None,
        row_limit=max_rows if truncated else None,
    )


def analyze_query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
    profile: str | None = None,
    types: Sequence[str] | None = None,
) -> ExplainResult:
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

    return ExplainResult(**out)
