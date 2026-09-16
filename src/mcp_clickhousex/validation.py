"""Read-only SQL validation for MCP query and SHOW tools."""

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

_SHOW_RE = re.compile(r"^\s*SHOW\s", re.IGNORECASE)

_INTO_OUTFILE_RE = re.compile(r"\bINTO\s+OUTFILE\b", re.IGNORECASE)

# String literals and quoted identifiers, so a ';' inside one is not mistaken
# for a statement separator.
_QUOTED_RE = re.compile(
    r"'(?:\\.|''|[^'])*'"
    r'|"(?:\\.|""|[^"])*"'
    r"|`(?:``|[^`])*`",
    re.DOTALL,
)


def _blank_quoted(sql: str) -> str:
    """Replace quoted spans with spaces, preserving offsets."""
    return _QUOTED_RE.sub(lambda m: " " * len(m.group(0)), sql)


def validate_read_only(sql: str) -> None:
    """Ensure *sql* is a single, read-only SELECT (or WITH … SELECT).

    Raises ``ValueError`` when the query is empty, contains multiple
    statements, or is not a read-only SELECT.
    """
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty.")

    stripped = sql.strip().rstrip(";").strip()

    if ";" in _blank_quoted(stripped):
        raise ValueError("Multiple SQL statements are not allowed.")

    if not _SELECT_OR_CTE_RE.search(stripped):
        raise ValueError("Only read-only SELECT queries are allowed.")

    if _COMMAND_RE.search(stripped):
        raise ValueError("The query contains forbidden SQL operations.")


def validate_show_statement(sql: str) -> None:
    """Ensure *sql* is a single SHOW statement without INTO OUTFILE.

    Raises ``ValueError`` when the query is empty, contains multiple
    statements, is not a SHOW statement, or requests server-side export.
    """
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty.")

    stripped = sql.strip().rstrip(";").strip()

    unquoted = _blank_quoted(stripped)

    if ";" in unquoted:
        raise ValueError("Multiple SQL statements are not allowed.")

    if _INTO_OUTFILE_RE.search(unquoted):
        raise ValueError("INTO OUTFILE is not allowed.")

    if not _SHOW_RE.search(stripped):
        raise ValueError("Only SHOW statements are allowed.")
