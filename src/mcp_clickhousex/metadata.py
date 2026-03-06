"""Metadata discovery using the ClickHouse system database.

SQL queries are loaded from the ``sql/`` directory at runtime so they
are not embedded in Python source.
"""

from __future__ import annotations

from importlib import resources
from typing import Any

from mcp_clickhousex.config import get_client


def _load_sql(name: str) -> str:
    """Read a ``.sql`` file from the ``sql`` package directory."""
    ref = resources.files("mcp_clickhousex").joinpath("sql", name)
    return ref.read_text(encoding="utf-8")


def _query(
    sql: str,
    parameters: dict[str, Any] | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """Execute a read-only metadata query and return ``{columns, rows}``."""
    client = get_client(profile)
    result = client.query(sql, parameters=parameters)
    columns = list(result.column_names)
    rows = [list(row) for row in result.result_rows]
    return {"columns": columns, "rows": rows}


def list_databases(profile: str | None = None) -> dict[str, Any]:
    """Return all databases from ``system.databases``."""
    sql = _load_sql("databases.sql")
    return _query(sql, profile=profile)


def list_tables(
    database: str | None = None, profile: str | None = None
) -> dict[str, Any]:
    """Return tables/views in *database* from ``system.tables``."""
    if not database:
        client = get_client(profile)
        database = client.database or "default"
    sql = _load_sql("tables.sql")
    return _query(sql, parameters={"database": database}, profile=profile)


def list_columns(
    table: str, database: str | None = None, profile: str | None = None
) -> dict[str, Any]:
    """Return columns for *table* from ``system.columns``.

    *table* may be qualified as ``database.table``; if so the
    *database* argument is ignored.
    """
    if "." in table:
        database, table = table.split(".", 1)
    elif not database:
        client = get_client(profile)
        database = client.database or "default"
    sql = _load_sql("columns.sql")
    return _query(
        sql, parameters={"database": database, "table": table}, profile=profile
    )
