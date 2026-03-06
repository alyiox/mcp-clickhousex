"""Read-only SQL validation for the MCP query tool."""

import re

_COMMAND_RE = re.compile(
    r"(^\s*)(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|"
    r"GRANT|REVOKE|ATTACH|DETACH|RENAME|OPTIMIZE|SET|KILL|SYSTEM)\s",
    re.IGNORECASE,
)

_SELECT_OR_CTE_RE = re.compile(
    r"^\s*(SELECT|WITH)\s",
    re.IGNORECASE,
)


def validate_read_only(sql: str) -> None:
    """Ensure *sql* is a single, read-only SELECT (or WITH … SELECT).

    Raises ``ValueError`` when the query is empty, contains multiple
    statements, or is not a read-only SELECT.
    """
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty.")

    stripped = sql.strip().rstrip(";").strip()

    if ";" in stripped:
        raise ValueError("Multiple SQL statements are not allowed.")

    if not _SELECT_OR_CTE_RE.search(stripped):
        raise ValueError("Only read-only SELECT queries are allowed.")

    if _COMMAND_RE.search(stripped):
        raise ValueError("The query contains forbidden SQL operations.")
